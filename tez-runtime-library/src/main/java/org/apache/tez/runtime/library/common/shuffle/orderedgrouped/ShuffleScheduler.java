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
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.Preconditions;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.InputHost;
import org.apache.tez.runtime.library.common.shuffle.ShuffleClient;
import org.apache.tez.runtime.library.common.shuffle.ShuffleServer;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils.FetchStatsLogger;
import org.apache.tez.runtime.library.common.shuffle.impl.ShuffleManager;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MapOutput.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleScheduler implements ShuffleClient<MapOutput> {

  /**
   * Placeholder for tracking shuffle events in case we get multiple spills info for the same
   * attempt.
   */
  static class ShuffleEventInfo {
    final int attemptNum;
    final String id;

    BitSet eventsProcessed;
    int finalEventId = -1;        // 0 indexed
    boolean scheduledForDownload; // whether chunks got scheduled for download (getMapHost)

    ShuffleEventInfo(InputAttemptIdentifier input) {
      this.id = input.getInputIdentifier() + "_" + input.getAttemptNumber();
      this.attemptNum = input.getAttemptNumber();
      this.eventsProcessed = new BitSet();
    }

    void spillProcessed(int spillId) {
      if (finalEventId != -1) {
        Preconditions.checkState(eventsProcessed.cardinality() <= (finalEventId + 1),
            "Wrong state. eventsProcessed cardinality={} finalEventId={}, spillId={}, {}",
            eventsProcessed.cardinality(), finalEventId, spillId, toString());
      }
      eventsProcessed.set(spillId);
    }

    void setFinalEventId(int spillId) {
      finalEventId = spillId;
    }

    boolean isDone() {
      return ((finalEventId != -1) && (finalEventId + 1) == eventsProcessed.cardinality());
    }

    public String toString() {
      return "[eventsProcessed=" + eventsProcessed + ", finalEventId=" + finalEventId
          +  ", id=" + id + ", attemptNum=" + attemptNum + "]";
    }
  }

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

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleScheduler.class);
  private static final Logger LOG_FETCH = LoggerFactory.getLogger(LOG.getName() + ".fetch");
  private static final FetchStatsLogger fetchStatsLogger = new FetchStatsLogger(LOG_FETCH, LOG);

  private final BitSet finishedMaps;
  private final int numInputs;
  private int numFetchedSpills;

  // to track shuffleInfo events when finalMerge is disabled in source or pipelined shuffle is enabled in source
  // Invariant: guard with this.synchronized
  private final Map<Integer, ShuffleEventInfo> shuffleInfoEventsMap;

  private final Set<InputAttemptIdentifier> obsoleteInputs = new HashSet<InputAttemptIdentifier>();
  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  private final InputContext inputContext;

  private final TezCounter shuffledInputsCounter;
  private final TezCounter skippedInputCounter;
  private final TezCounter reduceShuffleBytes;
  private final TezCounter reduceBytesDecompressed;
  private final TezCounter failedShuffleCounter;
  private final TezCounter bytesShuffledToDisk;
  private final TezCounter bytesShuffledToDiskDirect;
  private final TezCounter bytesShuffledToMem;

  private final String srcNameTrimmed;
  private final AtomicInteger remainingMaps;
  private final long startTime;

  private final FetchedInputAllocatorOrderedGrouped allocator;
  private final ExceptionReporter exceptionReporter;
  private final MergeManager mergeManager;

  private final int maxNumFetchers;

  private volatile Thread shuffleSchedulerThread = null;

  private long totalBytesShuffledTillNow = 0;

  private final ShuffleServer shuffleSchedulerServer;
  private final long shuffleSchedulerId;
  private final ShuffleErrorCounterGroup shuffleErrorCounterGroup;

  private int numFetchers = 0;
  private int numPartitionRanges = 0;
  private final Object lock = new Object();

  public ShuffleScheduler(
      InputContext inputContext,
      Configuration conf,
      int numberOfInputs,
      ExceptionReporter exceptionReporter,
      MergeManager mergeManager,
      FetchedInputAllocatorOrderedGrouped allocator,
      long startTime,
      String srcNameTrimmed) throws IOException {
    this.inputContext = inputContext;
    this.exceptionReporter = exceptionReporter;
    this.allocator = allocator;
    this.mergeManager = mergeManager;
    this.numInputs = numberOfInputs;
    remainingMaps = new AtomicInteger(numberOfInputs);
    finishedMaps = new BitSet(numberOfInputs);
    this.srcNameTrimmed = srcNameTrimmed;

    this.shuffleSchedulerServer = ShuffleServer.getInstance();
    this.shuffleSchedulerId = shuffleSchedulerServer.register(this);

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

    this.maxNumFetchers = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES_DEFAULT);

    this.skippedInputCounter = inputContext.getCounters().findCounter(TaskCounter.NUM_SKIPPED_INPUTS);

    shuffleInfoEventsMap = new HashMap<Integer, ShuffleEventInfo>();
    LOG.info("ShuffleScheduler running for sourceVertex: {}", inputContext.getSourceVertexName());
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

      shuffleSchedulerServer.unregister(shuffleSchedulerId);
    }
  }

  public void addKnownMapOutput(
      String inputHostName, int port, int partitionId, CompositeInputAttemptIdentifier srcAttempt) {
    if (!validateInputAttemptForPipelinedShuffle(srcAttempt, false)) {
      return;
    }

    shuffleSchedulerServer.addKnownInput(this, inputHostName, port, srcAttempt, partitionId);
  }

  public void obsoleteInput(InputAttemptIdentifier srcAttempt) {
    // The incoming srcAttempt does not contain a path component.
    LOG.info("{}: Adding obsolete input: {}", srcNameTrimmed, srcAttempt);

    // Even if we remove ShuffleEventInfo from shuffleInfoEventsMap[] (see below),
    // new Fetchers may be created from obsolete input again.
    // Hence, add srcAttempt to obsoleteInputs[].
    synchronized (obsoleteInputs) {
      obsoleteInputs.add(srcAttempt);
    }
  }

  public boolean cleanInputHostForConstructFetcher(InputHost.PartitionToInputs pendingInputs) {
    // safe to update pendingInputs because we are running in FetcherServer.call() thread
    assert pendingInputs.getShuffleClientId() == shuffleSchedulerId;
    assert pendingInputs.getInputs().size() <= shuffleSchedulerServer.getMaxTaskOutputAtOnce();

    boolean removedAnyInput = false;

    // avoid adding attempts which have already been completed
    synchronized (finishedMaps) {
      for (Iterator<InputAttemptIdentifier> inputIter = pendingInputs.getInputs().iterator();
           inputIter.hasNext();) {
        InputAttemptIdentifier input = inputIter.next();

        boolean alreadyCompleted;
        if (input instanceof CompositeInputAttemptIdentifier) {
          CompositeInputAttemptIdentifier compositeInput = (CompositeInputAttemptIdentifier) input;
          int nextClearBit = finishedMaps.nextClearBit(compositeInput.getInputIdentifier());
          int maxClearBit = compositeInput.getInputIdentifier() + compositeInput.getInputIdentifierCount();
          alreadyCompleted = nextClearBit > maxClearBit;
        } else {
          alreadyCompleted = finishedMaps.get(input.getInputIdentifier());
        }

        if (alreadyCompleted) {
          LOG.info("Skipping completed input: " + input);
          inputIter.remove();
          removedAnyInput = true;
        }
      }
    }

    for (Iterator<InputAttemptIdentifier> inputIter = pendingInputs.getInputs().iterator();
         inputIter.hasNext();) {
      InputAttemptIdentifier input = inputIter.next();

      // avoid adding attempts which have been marked as OBSOLETE
      if (isObsoleteInputAttemptIdentifier(input)) {
        LOG.info("Skipping obsolete input: " + input);
        inputIter.remove();
        removedAnyInput = true;
        continue;
      }

      if (!validateInputAttemptForPipelinedShuffle(input, false)) {
        inputIter.remove();   // no need to fetch for input, so remove
        removedAnyInput = true;
      }
    }

    return removedAnyInput;
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
        LOG.info("All inputs fetched for input vertex: " + inputContext.getSourceVertexName());
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
      LOG.warn("Duplicate fetch of input no longer needs to be fetched: " + srcAttemptIdentifier);
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
      LOG.info("Do not register obsolete input: " + srcAttemptIdentifier);
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

    if (isObsoleteInputAttemptIdentifier(srcAttemptIdentifier)) {
      LOG.info("Do not report obsolete input: " + srcAttemptIdentifier);
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
    LOG.info("{}: Reporting fetch failure for InputIdentifier: {} taskAttemptIdentifier: {}",
        srcNameTrimmed, srcAttempt,
        TezRuntimeUtils.getTaskAttemptIdentifier(
            inputContext.getSourceVertexName(), srcAttempt.getInputIdentifier(), srcAttempt.getAttemptNumber()));
    InputReadErrorEvent readError = InputReadErrorEvent.create(
        "Ordered: Fetch failure while fetching from "
          + TezRuntimeUtils.getTaskAttemptIdentifier(
          inputContext.getSourceVertexName(),
          srcAttempt.getInputIdentifier(),
          srcAttempt.getAttemptNumber()),
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


  public void fetcherStarted() {
    synchronized (lock) {
      numFetchers += 1;
    }
  }

  public void fetcherFinished() {
    synchronized (lock) {
      numFetchers -= 1;
      assert numFetchers >= 0;
    }
  }

  // partitionRangeAdded/Removed() are called only from the inside of synchronized(InputHost),
  // so numPartitionRanges is up-to-date and accurate.

  public void partitionRangeAdded() {
    synchronized (lock) {
      numPartitionRanges += 1;
    }
  }

  public void partitionRangeRemoved() {
    synchronized (lock) {
      numPartitionRanges -= 1;
      assert numPartitionRanges >= 0;
    }
  }

  // if true, we should scan pending InputHosts in ShuffleServer
  // if false, no need to consider this ShuffleManager for now
  public boolean shouldScanPendingInputs() {
    synchronized (lock) {
      return numPartitionRanges > 0 && numFetchers < maxNumFetchers;
    }
  }

  public long getShuffleClientId() {
    return shuffleSchedulerId;
  }

  public int getDagIdentifier() {
    return inputContext.getDagIdentifier();
  }

  public int[] getLocalShufflePorts() {
    return shuffleSchedulerServer.getLocalShufflePorts();
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

  private synchronized boolean validateInputAttemptForPipelinedShuffle(
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
              + ", eventsProcessed=" + eventInfo.eventsProcessed
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

  private boolean isObsoleteInputAttemptIdentifier(InputAttemptIdentifier input) {
    synchronized (obsoleteInputs) {
      if (obsoleteInputs.isEmpty()) {
        return false;
      }
      Iterator<InputAttemptIdentifier> obsoleteInputsIter = obsoleteInputs.iterator();
      while (obsoleteInputsIter.hasNext()) {
        InputAttemptIdentifier obsoleteInput = obsoleteInputsIter.next();
        if (input.include(obsoleteInput.getInputIdentifier(), obsoleteInput.getAttemptNumber())) {
          return true;
        }
      }
    }
    return false;
  }

  private void killSelf(Exception exception, String message) {
    LOG.error(message, exception);
    exceptionReporter.killSelf(exception, message);
  }

  private void logProgress() {
    int inputsDone = numInputs - remainingMaps.get();
    if (inputsDone == numInputs || isShutdown.get()) {
      double mbs = (double) totalBytesShuffledTillNow / (1024 * 1024);
      long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;

      double transferRate = mbs / secsSinceStart;
      StringBuilder s = new StringBuilder();
      s.append("copy=" + inputsDone);
      s.append(", numFetchedSpills=" + numFetchedSpills);
      s.append(", numInputs=" + numInputs);
      s.append(", transfer rate (MB/s) = " + transferRate);  // CumulativeDataFetched/TimeSinceInputStarted
      LOG.info(s.toString());
    }
  }

  private void setInputFinished(int inputIndex) {
    synchronized(finishedMaps) {
      finishedMaps.set(inputIndex, true);
    }
  }

  private boolean isInputFinished(int inputIndex) {
    synchronized (finishedMaps) {
      return finishedMaps.get(inputIndex);
    }
  }
}
