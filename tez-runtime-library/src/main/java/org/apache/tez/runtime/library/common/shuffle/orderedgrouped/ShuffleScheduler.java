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
import java.util.Iterator;
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

  public static class ShuffleErrorCounterGroup {
    public final TezCounter ioErrs;
    public final TezCounter wrongLengthErrs;
    public final TezCounter badIdErrs;
    public final TezCounter wrongMapErrs;
    public final TezCounter connectionErrs;
    public final TezCounter wrongReduceErrs;

    public ShuffleErrorCounterGroup(
        TezCounter ioErrs,
        TezCounter wrongLengthErrs,
        TezCounter badIdErrs,
        TezCounter wrongMapErrs,
        TezCounter connectionErrs,
        TezCounter wrongReduceErrs) {
      this.ioErrs = ioErrs;
      this.wrongLengthErrs = wrongLengthErrs;
      this.badIdErrs = badIdErrs;
      this.wrongMapErrs = wrongMapErrs;
      this.connectionErrs = connectionErrs;
      this.wrongReduceErrs = wrongReduceErrs;
    }
  }

  enum ShuffleErrors {
    IO_ERROR,
    WRONG_LENGTH,
    BAD_ID,
    WRONG_MAP,
    CONNECTION,
    WRONG_REDUCE
  }
  private final static String SHUFFLE_ERR_GRP_NAME = "Shuffle Errors";

  private final TezCounter shuffledInputsCounter;
  private final TezCounter skippedInputCounter;
  private final TezCounter reduceShuffleBytes;
  private final TezCounter reduceBytesDecompressed;
  private final TezCounter failedShuffleCounter;
  private final TezCounter bytesShuffledToDisk;
  private final TezCounter bytesShuffledToDiskDirect;
  private final TezCounter bytesShuffledToMem;

  private final ShuffleErrorCounterGroup shuffleErrorCounterGroup;

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

    // Counters used by the ShuffleScheduler
    this.shuffledInputsCounter = inputContext.getCounters().findCounter(
        TaskCounter.NUM_SHUFFLED_INPUTS);
    this.reduceShuffleBytes = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES);
    this.reduceBytesDecompressed = inputContext.getCounters().findCounter(
        TaskCounter.SHUFFLE_BYTES_DECOMPRESSED);
    this.failedShuffleCounter = inputContext.getCounters().findCounter(
        TaskCounter.NUM_FAILED_SHUFFLE_INPUTS);
    this.bytesShuffledToDisk = inputContext.getCounters().findCounter(
        TaskCounter.SHUFFLE_BYTES_TO_DISK);
    this.bytesShuffledToDiskDirect =  inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_DISK_DIRECT);
    this.bytesShuffledToMem = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_TO_MEM);

    // Counters used by Fetchers
    TezCounter ioErrsCounter = inputContext.getCounters().findCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.IO_ERROR.toString());
    TezCounter wrongLengthErrsCounter = inputContext.getCounters().findCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.WRONG_LENGTH.toString());
    TezCounter badIdErrsCounter = inputContext.getCounters().findCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.BAD_ID.toString());
    TezCounter wrongMapErrsCounter = inputContext.getCounters().findCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.WRONG_MAP.toString());
    TezCounter connectionErrsCounter = inputContext.getCounters().findCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.CONNECTION.toString());
    TezCounter wrongReduceErrsCounter = inputContext.getCounters().findCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.WRONG_REDUCE.toString());
    this.shuffleErrorCounterGroup = new ShuffleErrorCounterGroup(ioErrsCounter, wrongLengthErrsCounter,
        badIdErrsCounter, wrongMapErrsCounter, connectionErrsCounter, wrongReduceErrsCounter);

    this.startTime = startTime;

    this.skippedInputCounter = inputContext.getCounters().findCounter(TaskCounter.NUM_SKIPPED_INPUTS);

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
    if (!validateInputAttemptForPipelinedShuffle(srcAttempt, false)) {
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

    if (!isInputFinished(srcAttemptIdentifier.getInputIdentifier())) {
      if (output != null) {
        output.commit();
        fetchStatsLogger.logIndividualFetchComplete(copyDuration, bytesCompressed, bytesDecompressed,
            output.getType().toString(), srcAttemptIdentifier);
        if (output.getType() == Type.DISK) {
          bytesShuffledToDisk.increment(bytesCompressed);
        } else if (output.getType() == Type.DISK_DIRECT) {
          bytesShuffledToDiskDirect.increment(bytesCompressed);
        } else {
          bytesShuffledToMem.increment(bytesCompressed);
        }
        shuffledInputsCounter.increment(1);
      } else {
        // Output null implies that a physical input completion is being
        // registered without needing to fetch data
        skippedInputCounter.increment(1);
      }

      /**
       * In case of pipelined shuffle, it is quite possible that fetchers pulled the FINAL_UPDATE
       * spill in advance due to smaller output size.  In such scenarios, we need to wait until
       * we retrieve all spill details to claim success.
       */
      if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
        registerCompletedInput(srcAttemptIdentifier);
      } else {
        registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier);
      }

      if (remainingMaps.get() == 0) {
        notifyAll();
        LOG.info("All inputs fetched for ShuffleScheduler {}", shuffleClientId);
      }

      // update the status
      totalBytesShuffledTillNow += bytesCompressed;
      logProgress();
      reduceShuffleBytes.increment(bytesCompressed);
      reduceBytesDecompressed.increment(bytesDecompressed);
      if (LOG.isDebugEnabled()) {
        LOG.debug("src task: "
            + TezRuntimeUtils.getTaskAttemptIdentifier(
            inputContext.getSourceVertexName(), srcAttemptIdentifier.getInputIdentifier(),
            srcAttemptIdentifier.getAttemptNumber()) + " done");
      }
    } else {
      // input is already finished. duplicate fetch.
      LOG.warn("Duplicate fetch of ordered input: {}", srcAttemptIdentifier);
      // free the resource - especially memory

      // If the src does not generate data, output will be null.
      if (output != null) {
        output.abort();
      }
    }
    // TODO NEWTEZ Should this be releasing the output, if not committed ? Possible memory leak in case of speculation.
  }

  // Invariant: inside synchronized(this)
  private void registerCompletedInput(InputAttemptIdentifier srcAttemptIdentifier) {
    remainingMaps.decrementAndGet();
    setInputFinished(srcAttemptIdentifier.getInputIdentifier());
    numFetchedSpills++;
  }

  // Invariant: inside synchronized(this)
  private void registerCompletedInputForPipelinedShuffle(
      InputAttemptIdentifier srcAttemptIdentifier) {
    // corresponds to ShuffleManager.registerCompletedInputForPipelinedShuffle()

    if (isObsoleteInputAttemptIdentifier(srcAttemptIdentifier)) {
      LOG.info("Do not register obsolete input: {}", srcAttemptIdentifier);
      return;
    }

    // Allow only one task attempt to proceed.
    if (!validateInputAttemptForPipelinedShuffle(srcAttemptIdentifier, true)) {
      return;
    }

    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    boolean eventInfoIsDone;
    // synchronized (shuffleInfoEventsMap) {  // covered by this.synchronized
    ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);
    assert eventInfo != null;

    eventInfo.spillProcessed(srcAttemptIdentifier.getSpillEventId());
    numFetchedSpills++;
    if (srcAttemptIdentifier.getFetchTypeInfo() == InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE) {
      eventInfo.setFinalEventId(srcAttemptIdentifier.getSpillEventId());
    }

    eventInfoIsDone = eventInfo.isDone();
    if (eventInfoIsDone) {
      shuffleInfoEventsMap.remove(inputIdentifier);
    }
    // }

    // check if we downloaded all spills pertaining to this InputAttemptIdentifier
    if (eventInfoIsDone) {
      remainingMaps.decrementAndGet();
      setInputFinished(inputIdentifier);
    }
  }

  public void fetchFailed(InputAttemptIdentifier srcAttemptIdentifier,
                          boolean readFailed, boolean connectFailed) {
    failedShuffleCounter.increment(1);

    if (isInputFinished(srcAttemptIdentifier.getInputIdentifier())) {
      LOG.warn("Ordered fetch failed for {}, but input already completed: InputIdentifier={}",
        shuffleClientId, srcAttemptIdentifier);
      return;
    }

    if (isObsoleteInputAttemptIdentifier(srcAttemptIdentifier)) {
      LOG.info("Do not report obsolete input: {}", srcAttemptIdentifier);
      return;
    }

    boolean shouldInformAM = readFailed || connectFailed;
    assert readFailed ^ connectFailed;
    assert shouldInformAM;
    if (shouldInformAM) {
      informAM(srcAttemptIdentifier);
    }

    // unlike in the original implementation, we do not check the number of fetch failures for srcAttemptIdentifier
    // and fail the current TaskAttempt immediately
    if (srcAttemptIdentifier.canRetrieveInputInChunks()) {
      synchronized (this) {
        int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
        ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);

        if (eventInfo != null && srcAttemptIdentifier.getAttemptNumber() == eventInfo.attemptNum) {
          // some spills with the same attempt number have been downloaded, so this TaskAttempt cannot succeed
          exceptionReporter.reportException(new TezUncheckedException("Failed to fetch input " + srcAttemptIdentifier));
        } else {
          LOG.warn("Ordered fetch failed, but do not kill yet because no spill has been downloaded yet: {}", srcAttemptIdentifier);
        }
      }
    } else {
      LOG.warn("Ordered fetch failed, but do not kill (not pipelined): {}", srcAttemptIdentifier);
    }
  }

  // Notify AM
  public void informAM(InputAttemptIdentifier srcAttempt) {
    LOG.info("ShuffleScheduler {}: Reporting fetch failure for InputIdentifier: {}, taskAttemptIdentifier: {}",
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

  public ShuffleErrorCounterGroup getShuffleErrorCounterGroup() {
    return shuffleErrorCounterGroup;
  }

  protected synchronized boolean validateInputAttemptForPipelinedShuffle(
      InputAttemptIdentifier input, boolean registerShuffleInfoEvent) {
    // For pipelined shuffle
    // TODO: TEZ-2132 for error handling. As of now, fail fast if there is a different attempt
    if (input.canRetrieveInputInChunks()) {
      // synchronized (shuffleInfoEventsMap) {  // covered by this.synchronized
        int inputIdentifier = input.getInputIdentifier();
        ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);

        if (eventInfo != null && input.getAttemptNumber() != eventInfo.attemptNum) {
          IOException exception = new IOException("Previous event already got scheduled for " +
              input + ". Previous attempt's data could have been already merged "
              + "to memory/disk outputs.  Killing (self) this task early."
              + " currentAttemptNum=" + eventInfo.attemptNum
              + ", eventsProcessed=" + eventInfo.getEventsProcessed()
              + ", newAttemptNum=" + input.getAttemptNumber());
          String message = "Killing self as previous attempt data could have been consumed";
          killSelf(exception, message);
          return false;
        }

        if (eventInfo == null && registerShuffleInfoEvent) {
          shuffleInfoEventsMap.put(inputIdentifier, new ShuffleEventInfo(input));
        }
      // }
    }

    return true;
  }

  private void killSelf(Exception exception, String message) {
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

  private void setInputFinished(int inputIndex) {
    synchronized(completedInputSet) {
      completedInputSet.set(inputIndex, true);
    }
  }

  private boolean isInputFinished(int inputIndex) {
    synchronized (completedInputSet) {
      return completedInputSet.get(inputIndex);
    }
  }

  @Override
  public void obsoleteKnownInput(InputAttemptIdentifier srcAttempt) {
    super.obsoleteKnownInput(srcAttempt);
    LOG.info("ShuffleScheduler: shuffleClientId={}, numInputs={}, remaining={}",
        shuffleClientId, numInputs, remainingMaps.get());
  }
}
