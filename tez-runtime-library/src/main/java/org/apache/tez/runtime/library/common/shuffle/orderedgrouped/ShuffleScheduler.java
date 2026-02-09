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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.shuffle.ShuffleClient;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MapOutput.Type;

public class ShuffleScheduler extends ShuffleClient<MapOutput> {

  private final TezCounter shuffleNumSkippedOrderedInputCounter;

  private final long startTime;

  private int numFetchedSpills = 0;
  private long totalBytesShuffledTillNow = 0;

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

  public synchronized void fetchSucceeded(
      InputAttemptIdentifier srcAttemptIdentifier,
      MapOutput output,
      long bytesCompressed,
      long bytesDecompressed,
      long copyDuration) throws IOException {
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();

    boolean updateStats = false;
    if (!isInputFinished(inputIdentifier)) {
      // guard shuffleInfoEventsMap[], already covered by this.synchronized
      // The result of checkCommitRegister() is valid in this.synchronized, so inside fetchSucceeded()
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
        registerCompletedInput(srcAttemptIdentifier);
      } else {
        if (commitAndRegister) {
          // killInPipelined == false
          registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier);
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

      if (remainingMaps.get() == 0) {
        notifyAll();
        LOG.info("All inputs fetched for ShuffleScheduler {}", shuffleClientId);
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

    if (updateStats) {
      updateCounters(srcAttemptIdentifier, bytesCompressed, bytesDecompressed, copyDuration,
          output.getType().toString(),
          output.getType() == Type.DISK,
          output.getType() == Type.DISK_DIRECT);
      totalBytesShuffledTillNow += bytesCompressed;
      logProgress();
    }
  }

  // inside synchronized (this)
  private void registerCompletedInput(InputAttemptIdentifier srcAttemptIdentifier) {
    remainingMaps.decrementAndGet();
    setInputFinished(srcAttemptIdentifier.getInputIdentifier());
    numFetchedSpills++;
  }

  // inside synchronized (this), covering synchronize (shuffleInfoEventsMap)
  private void registerCompletedInputForPipelinedShuffle(
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
    numFetchedSpills++;
    if (srcAttemptIdentifier.getFetchTypeInfo() == InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE) {
      eventInfo.setFinalEventId(srcAttemptIdentifier.getSpillEventId());
    }

    eventInfoIsDone = eventInfo.isDone();
    if (eventInfoIsDone) {
      shuffleInfoEventsMap.remove(inputIdentifier);
      remainingMaps.decrementAndGet();
      setInputFinished(inputIdentifier);
    }
  }

  public void fetchFailed(CompositeInputAttemptIdentifier srcAttemptIdentifier,
                          boolean readFailed, boolean connectFailed) {
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    shuffleNumFailedInputsCounter.increment(1);

    if (isInputFinished(inputIdentifier)) {
      LOG.warn("Ordered fetch failed for {}, but input already completed: InputIdentifier={}",
        shuffleClientId, srcAttemptIdentifier);
      return;
    }

    if (isObsoleteInputAttemptIdentifier(srcAttemptIdentifier)) {
      LOG.info("Do not report obsolete ordered input: {}", srcAttemptIdentifier);
      return;
    }

    boolean shouldInformAM = readFailed || connectFailed;
    assert shouldInformAM && (readFailed ^ connectFailed);
    if (shouldInformAM) {
      informAM(srcAttemptIdentifier);   // send InputReadErrorEvent only, without killing TaskAttempt
    }

    // Unlike in the original implementation, we do not check the number of fetch failures for srcAttemptIdentifier
    // and fail the current TaskAttempt immediately.
    if (srcAttemptIdentifier.canRetrieveInputInChunks()) {
      synchronized (this) {
        ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);
        if (eventInfo != null && srcAttemptIdentifier.getAttemptNumber() == eventInfo.attemptNum) {
          // Some spills with the same attempt number have been downloaded, so this TaskAttempt cannot succeed.
          // ShuffleServer.fetchFailed already verified !existsConcurrentNotFailedFetcher, so we should kill here.
          exceptionReporter.reportException(new TezUncheckedException("Failed to fetch input " + srcAttemptIdentifier));
        } else {
          LOG.warn("Ordered fetch failed, but do not kill yet because no spill has been downloaded yet: {}", srcAttemptIdentifier);
        }
      }
    } else {
      LOG.warn("Ordered fetch failed, but do not kill (non-pipelined): {}", srcAttemptIdentifier);
    }
  }

  // Notify AM
  public void informAM(CompositeInputAttemptIdentifier srcAttempt) {
    LOG.warn("ShuffleScheduler {}: Reporting fetch failure for InputIdentifier: {}, taskAttemptIdentifier: {}",
        shuffleClientId, srcAttempt,
        TezRuntimeUtils.getTaskAttemptIdentifier(
            inputContext.getSourceVertexName(), srcAttempt.getInputIdentifier(), srcAttempt.getAttemptNumber()));
    InputReadErrorEvent readError = InputReadErrorEvent.create(
        "Ordered: Fetch failure while fetching from " + inputContext.getUniqueIdentifier(),
        srcAttempt.getInputIdentifier(),
        srcAttempt.getAttemptNumber());
    List<Event> failedEvents = Lists.newArrayListWithCapacity(1);
    failedEvents.add(readError);
    inputContext.sendEvents(failedEvents);
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
      // synchronized (this) covers synchronized (shuffleInfoEventsMap)
      synchronized (this) {
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
      long kbs = totalBytesShuffledTillNow / 1024;
      long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;
      long transferRate = kbs / secsSinceStart;

      StringBuilder s = new StringBuilder();
      s.append("ShuffleScheduler " + shuffleClientId);
      s.append(", copy=" + inputsDone);
      s.append(", numFetchedSpills=" + numFetchedSpills);
      s.append(", numInputs=" + numInputs);
      s.append(", transfer rate (KB/s) = " + transferRate);
      LOG.info(s.toString());
    }
  }
}
