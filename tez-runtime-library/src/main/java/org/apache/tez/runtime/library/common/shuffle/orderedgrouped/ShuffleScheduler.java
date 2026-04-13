/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.ShuffleClient;

public class ShuffleScheduler extends ShuffleClient<MapOutput> {

  private final TezCounter shuffleNumSkippedOrderedInputCounter;

  private final long startTime;

  private final AtomicInteger numFetchedSpills = new AtomicInteger(0);
  private final AtomicLong totalBytesShuffledTillNow = new AtomicLong(0);

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  private final AtomicInteger remainingMaps;

  private final FetchedInputAllocatorOrderedGrouped allocator;
  private final ExceptionReporter exceptionReporter;
  private final MergeManager mergeManager;

  private volatile Thread shuffleSchedulerThread = null;

  public ShuffleScheduler(
      InputContext inputContext,
      Configuration conf,
      int numInputs,
      ExceptionReporter exceptionReporter,
      MergeManager mergeManager,
      FetchedInputAllocatorOrderedGrouped allocator,
      long startTime,
      String srcNameTrimmed) throws IOException {
    super(inputContext, conf, numInputs, srcNameTrimmed);

    this.allocator = allocator;
    this.exceptionReporter = exceptionReporter;
    this.mergeManager = mergeManager;

    remainingMaps = new AtomicInteger(numInputs);

    this.shuffleNumSkippedOrderedInputCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_NUM_SKIPPED_ORDERED_INPUTS);

    this.startTime = startTime;

    LOG.info("ShuffleScheduler for {}/{}: shuffleClientId={}, numInputs={}",
      inputContext.getUniqueIdentifier(), srcNameTrimmed, shuffleClientId, numInputs);
  }

  public void start() throws Exception {
    shuffleSchedulerThread = Thread.currentThread();
    mergeManager.setupParentThread(shuffleSchedulerThread);

    synchronized (this) {
      while (!isShutdown.get() && remainingMaps.get() != 0) {
        wait();
      }
    }
  }

  public void close() {
    if (!isShutdown.getAndSet(true)) {
      try {
        logProgress();
      } catch (Exception e) {
        LOG.warn("Failed log progress while closing, ignoring and continuing shutdown. Message={}",
            e.getMessage());
      }

      // Notify and interrupt the waiting scheduler thread
      synchronized (this) {
        notifyAll();
      }

      // TODO: should we remove this?
      // Interrupt the ShuffleScheduler thread only if the close is invoked by another thread.
      // If this is invoked on the same thread, then the shuffleRunner has already complete, and there's
      // no point interrupting it.
      // The interrupt is needed to unblock any merges or waits which may be happening, so that the thread can
      // exit.
      if (shuffleSchedulerThread != null && !Thread.currentThread().equals(shuffleSchedulerThread)) {
        shuffleSchedulerThread.interrupt();
      }

      shuffleServer.unregister(shuffleClientId);
    }
  }

  public void wakeupLoop() {
    shuffleServer.wakeupLoop();
  }

  public void addKnownMapOutput(
      String hostName, String containerId, int port, int partitionId, CompositeInputAttemptIdentifier srcAttempt) {
    // Note: this check is optional.
    // use srcAttempt.getInput() for quick checking
    if (!validateInputAttemptForPipelinedShuffle(srcAttempt.getInput())) {
      return;
    }

    shuffleServer.addKnownInput(this, hostName, containerId, port, srcAttempt, partitionId);
  }

  public void fetchSucceeded(
      InputAttemptIdentifier srcAttemptIdentifier,
      MapOutput output,
      long bytesCompressed,
      long bytesDecompressed,
      long copyDuration) throws IOException {
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();

    boolean updateStats = false;
    boolean allInputsFetched = false;
    synchronized (lockForInput(inputIdentifier)) {
      if (!isInputFinished(inputIdentifier)) {
        // The result of checkCommitRegister() is valid while lockForInput(inputIdentifier) is held.
        CommitRegister cr = checkCommitRegister(srcAttemptIdentifier);
        boolean isPipelined = cr.isPipelined;
        boolean commitAndRegister = cr.commitAndRegister;
        boolean killInPipelined = cr.killInPipelined;   // killBecauseDifferentSpillAttemptInPipelined
        assert !(!isPipelined) || commitAndRegister;
        assert !(isPipelined && commitAndRegister) || !killInPipelined;
        assert !(isPipelined && killInPipelined) || !commitAndRegister;

        // 1. call output.commit() or output.abort() if necessary
        // consider commitAndRegister only
        if (output != null) {
          if (commitAndRegister) {
            output.commit();
            updateStats = true;
          } else {
            LOG.warn("Duplicate fetch of ordered input for {} ({} remaining): {}",
              inputContext.getUniqueIdentifier(), remainingMaps.get(), srcAttemptIdentifier);
            shuffleNumDuplicateInputsCounter.increment(1);
            // free the resource - especially memory
            output.abort();
          }
        } else {
          // cannot call output.commit()/abort()
          // Output null implies that a physical input completion is being registered without needing to fetch data
          shuffleNumSkippedOrderedInputCounter.increment(1);
        }

        // 2. register completed input if necessary
        // consider isPipelined, commitAndRegister, killInPipelined
        if (!isPipelined) {
          // commitAndRegister == true
          allInputsFetched = registerCompletedInput(srcAttemptIdentifier);
        } else {
          if (commitAndRegister) {
            // killInPipelined == false
            allInputsFetched = registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier);
          } else {
            if (!killInPipelined) {
              LOG.info("Ordered spill already processed for {} (remaining={}): {}",
                  inputContext.getUniqueIdentifier(), remainingMaps.get(), srcAttemptIdentifier);
            } else {
              String message = "Killing self as previous attempt ordered data could have been consumed in pipelined shuffling";
              IOException exception = new IOException(
                  message + ": " + inputContext.getUniqueIdentifier() + ", " + srcAttemptIdentifier);
              killSelf(exception, message);
            }
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Source done for {} ({} remaining): {}",
              inputContext.getUniqueIdentifier(), remainingMaps.get(), srcAttemptIdentifier);
        }
      } else {
        // input is already finished. duplicate fetch.
        LOG.warn("Fetch of ordered input after completion for {} ({} remaining): {}",
            inputContext.getUniqueIdentifier(), remainingMaps.get(), srcAttemptIdentifier);
        // free the resource - especially memory
        // If the source does not generate data, output will be null.
        if (output != null) {
          shuffleNumDuplicateInputsCounter.increment(1);
          output.abort();
        }
      }
    }

    if (allInputsFetched) {
      synchronized (this) {
        notifyAll();
      }
      LOG.info("All inputs fetched for ShuffleScheduler {}", shuffleClientId);
    }

    if (updateStats) {
      Type type = output.getType();
      updateCounters(srcAttemptIdentifier, bytesCompressed, bytesDecompressed, copyDuration, type);
      totalBytesShuffledTillNow.addAndGet(bytesCompressed);
      logProgress();
    }
  }

  // Called only while holding lockForInput(inputIdentifier).
  private boolean registerCompletedInput(InputAttemptIdentifier srcAttemptIdentifier) {
    int remaining = remainingMaps.decrementAndGet();
    setInputFinished(srcAttemptIdentifier.getInputIdentifier());
    numFetchedSpills.incrementAndGet();
    return remaining == 0;
  }

  // Called only while holding lockForInput(inputIdentifier).
  private boolean registerCompletedInputForPipelinedShuffle(
      InputAttemptIdentifier srcAttemptIdentifier) {
    // The input has been successfully fetched for inputIdentifier + spillId, so srcAttemptIdentifier can be obsolete.
    // srcAttemptIdentifier is already validated.

    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    boolean eventInfoIsDone;
    ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);
    if (eventInfo == null) {
      eventInfo = new ShuffleEventInfo(srcAttemptIdentifier);
      shuffleInfoEventsMap.put(inputIdentifier, eventInfo);
    }

    // spill not processed yet, so register
    assert !eventInfo.getEventsProcessed().get(srcAttemptIdentifier.getSpillEventId());
    eventInfo.spillProcessed(srcAttemptIdentifier.getSpillEventId());
    numFetchedSpills.incrementAndGet();
    if (srcAttemptIdentifier.getFetchTypeInfo() == InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE) {
      eventInfo.setFinalEventId(srcAttemptIdentifier.getSpillEventId());
    }

    eventInfoIsDone = eventInfo.isDone();
    if (eventInfoIsDone) {
      shuffleInfoEventsMap.remove(inputIdentifier);
      int remaining = remainingMaps.decrementAndGet();
      setInputFinished(inputIdentifier);
      return remaining == 0;
    }
    return false;
  }

  public void fetchFailed(CompositeInputAttemptIdentifier srcAttemptIdentifier,
                          boolean readFailed, boolean connectFailed) {
    shuffleNumFailedInputsCounter.increment(1);

    // It suffices to call isObsoleteInputAttemptIdentifier() with srcAttemptIdentifier only once
    // because the presence of any obsolete input in srcAttemptIdentifier's InputAttemptIdentifiers implies that
    // the source task will re-generate the entire output.
    if (isObsoleteInputAttemptIdentifier(srcAttemptIdentifier)) {
      LOG.info("Do not report obsolete ordered input: {}", srcAttemptIdentifier);
      return;
    }

    boolean shouldInformAM = false;
    for (int i = 0; i < srcAttemptIdentifier.getInputIdentifierCount(); i++) {
      InputAttemptIdentifier inputAttemptIdentifier = srcAttemptIdentifier.expand(i);
      int inputIdentifier = inputAttemptIdentifier.getInputIdentifier();

      synchronized (lockForInput(inputIdentifier)) {
        if (isInputFinished(inputIdentifier)) {   // e.g., if empty partition
          LOG.warn("Ordered fetch failed for {}, but input already completed: InputIdentifier={}",
            shuffleClientId, inputAttemptIdentifier);
          continue;
        }

        shouldInformAM = readFailed || connectFailed;
        assert shouldInformAM && (readFailed ^ connectFailed);

        // Unlike in the original implementation, we do not check the number of fetch failures for srcAttemptIdentifier
        // and fail the current TaskAttempt immediately.
        if (inputAttemptIdentifier.canRetrieveInputInChunks()) {
          ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);
          if (eventInfo != null && inputAttemptIdentifier.getAttemptNumber() == eventInfo.attemptNum) {
            // Some spills with the same attempt number have been downloaded, so this TaskAttempt cannot succeed.
            // ShuffleServer.fetchFailed already verified !existsConcurrentNotFailedFetcher, so we should kill here.
            exceptionReporter.reportException(new TezUncheckedException("Failed to fetch input " + inputAttemptIdentifier));
          } else {
            LOG.warn("Ordered fetch failed, but do not kill yet because no spill has been downloaded yet: {}", inputAttemptIdentifier);
          }
        } else {
          LOG.warn("Ordered fetch failed, but do not kill (non-pipelined): {}", inputAttemptIdentifier);
        }
      }
    }

    // It suffices to call informAM() with srcAttemptIdentifier only once
    // because the source task will re-generate the entire output.
    if (shouldInformAM) {
      informAM("Ordered", srcAttemptIdentifier);   // send InputReadErrorEvent only, without killing TaskAttempt
    }
  }

  public void waitForMergeManager() throws InterruptedException {
    mergeManager.waitForInMemoryMerge();
    mergeManager.waitForShuffleToMergeMemory();
  }

  public FetchedInputAllocatorOrderedGrouped getAllocator() {
    return allocator;
  }

  public ExceptionReporter getExceptionReporter() {
    return exceptionReporter;
  }

  // can run in ShuffleServer.call() thread, ShuffleInputEventHandler thread
  protected boolean validateInputAttemptForPipelinedShuffle(InputAttemptIdentifier input) {
    if (input.canRetrieveInputInChunks()) {   // for pipelined shuffle only
      synchronized (lockForInput(input.getInputIdentifier())) {
        return validateInputAttemptForPipelinedShuffleCommon(input);
      }
    } else {
      return true;
    }
  }

  @Override
  protected void killSelf(Exception exception, String message) {
    LOG.error(message, exception);
    exceptionReporter.killSelf(exception, message);
  }


  private void logProgress() {
    int inputsDone = numInputs - remainingMaps.get();
    if (inputsDone == numInputs || isShutdown.get()) {
      long kbs = totalBytesShuffledTillNow.get() / 1024;
      long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;
      long transferRate = kbs / secsSinceStart;

      StringBuilder s = new StringBuilder();
      s.append("ShuffleScheduler " + shuffleClientId);
      s.append(", copy=" + inputsDone);
      s.append(", numFetchedSpills=" + numFetchedSpills.get());
      s.append(", numInputs=" + numInputs);
      s.append(", transfer rate (KB/s) = " + transferRate);
      LOG.info(s.toString());
    }
  }
}
