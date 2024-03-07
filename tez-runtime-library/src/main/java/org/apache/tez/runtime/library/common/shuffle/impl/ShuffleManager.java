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

package org.apache.tez.runtime.library.common.shuffle.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput.Type;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.common.shuffle.FetcherCallback;
import org.apache.tez.runtime.library.common.shuffle.InputHost;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils.FetchStatsLogger;

import org.apache.tez.common.Preconditions;
import com.google.common.collect.Lists;

// This only knows how to deal with a single srcIndex for a given targetIndex.
// In case the src task generates multiple outputs for the same target Index
// (multiple src-indices), modifications will be required.
public class ShuffleManager implements FetcherCallback {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleManager.class);
  private static final Logger LOG_FETCH = LoggerFactory.getLogger(LOG.getName() + ".fetch");
  private static final FetchStatsLogger fetchStatsLogger = new FetchStatsLogger(LOG_FETCH, LOG);

  private final ShuffleManagerServer shuffleManagerServer;
  private final InputContext inputContext;
  private final int numInputs;
  private final FetchedInputAllocator inputManager;
  private final long shuffleManagerId;
  private final String srcNameTrimmed;

  private final TezCounter approximateInputRecords;
  private final TezCounter shuffledInputsCounter;
  private final TezCounter failedShufflesCounter;
  private final TezCounter bytesShuffledCounter;
  private final TezCounter decompressedDataSizeCounter;
  private final TezCounter bytesShuffledToDiskCounter;
  private final TezCounter bytesShuffledToMemCounter;
  private final TezCounter bytesShuffledDirectDiskCounter;
  private final TezCounter shufflePhaseTime;
  private final TezCounter firstEventReceived;
  private final TezCounter lastEventReceived;

  // TODO: keep in ShuffleManagerServer???
  private final int maxNumFetchers;

  private final long startTime;

  // accessed only from ShuffleInputEventHandler thread, so thread-safe
  private long inputRecordsFromEvents = 0L;
  private long eventsReceived = 0L;

  private final AtomicInteger numFetchedSpills = new AtomicInteger(0);
  private final AtomicLong totalBytesShuffledTillNow = new AtomicLong(0L);

  private final AtomicBoolean inputReadyNotificationSent = new AtomicBoolean(false);
  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  // actual number of completed inputs that have been added to completedInputs[]
  // numCompletedInputs == numInputs --> all inputs have been received
  private final AtomicInteger numCompletedInputs = new AtomicInteger(0);

  // thread-safe for reading and updating
  // FetchedInput is not thread-safe, but automatically guarded:
  //   - create FetchedInput before calling completedInputs.add() in ShuffleInputEventHandler/Fetcher threads
  //   - consume FetcherInput after calling completedInputs.take() in UnorderedKVReader.next() thread
  //   - after calling completedInputs.add(), ShuffleInputEventHandler/Fetcher threads never update FetchedInput
  // endOfInputMarker is added at the end as End of Input message
  private final BlockingQueue<FetchedInput> completedInputs;
  private static final FetchedInput endOfInputMarker = new NullFetchedInput(null);

  // thread-safe for reading and updating
  // InputAttemptIdentifier is immutable
  private final Set<InputAttemptIdentifier> obsoletedInputs;

  // to track shuffleInfo events when finalMerge is disabled OR pipelined shuffle is enabled in source
  // thread-safe for reading and updating
  // however, ShuffleEventInfo is not thread-safe and should be guarded
  // guard ShuffleEventInfo with: synchronized (shuffleInfoEventsMap)
  private final Map<Integer, ShuffleEventInfo> shuffleInfoEventsMap;

  // not thread-safe - accessed from:
  //   1. ShuffleManagerServer.call() thread
  //   2. ShuffleInputEventHandler thread
  //   3. Fetcher thread via fetchSucceeded()
  // optimistic-guard:
  //   1. completedInputSet.set(index) == true --> actually true and up-to-date
  //   2. completedInputSet.set(index) == false --> synchronized(completedInputSet) and read again
  // guard with: synchronized (completedInputSet)
  // Cf. isCompletedInput()
  private final BitSet completedInputSet;

  private int numFetchers = 0;
  private int numPartitionRanges = 0;
  private final Object lock = new Object();

  public ShuffleManager(InputContext inputContext, Configuration conf, int numInputs,
      FetchedInputAllocator inputAllocator) throws IOException {
    this.shuffleManagerServer = ShuffleManagerServer.getInstance();
    this.inputContext = inputContext;
    this.numInputs = numInputs;
    this.inputManager = inputAllocator;
    this.shuffleManagerId = ShuffleManagerServer.getInstance().register(this);
    this.srcNameTrimmed = TezUtilsInternal.cleanVertexName(inputContext.getSourceVertexName());

    this.approximateInputRecords = inputContext.getCounters().findCounter(TaskCounter.APPROXIMATE_INPUT_RECORDS);
    this.shuffledInputsCounter = inputContext.getCounters().findCounter(TaskCounter.NUM_SHUFFLED_INPUTS);
    this.failedShufflesCounter = inputContext.getCounters().findCounter(TaskCounter.NUM_FAILED_SHUFFLE_INPUTS);
    this.bytesShuffledCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES);
    this.decompressedDataSizeCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_DECOMPRESSED);
    this.bytesShuffledToDiskCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_TO_DISK);
    this.bytesShuffledToMemCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_TO_MEM);
    this.bytesShuffledDirectDiskCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_DISK_DIRECT);
    this.shufflePhaseTime = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_PHASE_TIME);
    this.firstEventReceived = inputContext.getCounters().findCounter(TaskCounter.FIRST_EVENT_RECEIVED);
    this.lastEventReceived = inputContext.getCounters().findCounter(TaskCounter.LAST_EVENT_RECEIVED);

    this.maxNumFetchers = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES_DEFAULT);

    this.startTime = System.currentTimeMillis();

    // In case of pipelined shuffle, it is possible to get multiple FetchedInput per attempt.
    // We do not know upfront the number of spills from source.
    completedInputs = new LinkedBlockingDeque<FetchedInput>();
    obsoletedInputs = Collections.newSetFromMap(new ConcurrentHashMap<InputAttemptIdentifier, Boolean>());

    shuffleInfoEventsMap = new ConcurrentHashMap<Integer, ShuffleEventInfo>();
    completedInputSet = new BitSet(numInputs);

    LOG.info("ShuffleManage for {}: numInputs={}", srcNameTrimmed, numInputs);
  }

  public FetchedInputAllocator getInputManager() {
    return inputManager;
  }

  public long getShuffleManagerId() {
    return shuffleManagerId;
  }

  public int getDagIdentifier() {
    return inputContext.getDagIdentifier();
  }

  // called only when inputContext.useShuffleHandlerProcessOnK8s()
  public int[] getLocalShufflePorts() {
    return shuffleManagerServer.getLocalShufflePorts();
  }

  // called from ShuffleInputEventHandler thread
  public void updateApproximateInputRecords(int delta) {
    if (delta <= 0) {
      return;
    }
    inputRecordsFromEvents += delta;
    eventsReceived++;
    approximateInputRecords.setValue((inputRecordsFromEvents / eventsReceived) * numInputs);
  }

  public void run() throws IOException {
    // ShuffleManager does not run any thread
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

  // if true, we should scan pending InputHosts in ShuffleManagerServer
  // if false, no need to consider this ShuffleManager for now
  public boolean shouldScanPendingInputs() {
    synchronized (lock) {
      return numPartitionRanges > 0 && numFetchers < maxNumFetchers;
    }
  }

  // inside ShuffleManagerServer.call() thread
  public void cleanInputHostForConstructFetcher(
      InputHost.PartitionToInputs pendingInputs, InputHost inputHost) {
    // safe to update pendingInputs because we are running in ShuffleManagerServer.call() thread
    assert pendingInputs.getShuffleManagerId() == shuffleManagerId;

    int includedMaps = 0;
    for (Iterator<InputAttemptIdentifier> inputIter = pendingInputs.getInputs().iterator();
         inputIter.hasNext();) {
      InputAttemptIdentifier input = inputIter.next();

      // validateInputAttemptForPipelinedShuffle() for pipelined shuffle
      if (!validateInputAttemptForPipelinedShuffle(input)) {
        continue;
      }

      // avoid adding attempts which have already been completed
      synchronized (completedInputSet) {
        boolean alreadyCompleted;
        if (input instanceof CompositeInputAttemptIdentifier) {
          CompositeInputAttemptIdentifier compositeInput = (CompositeInputAttemptIdentifier) input;
          int nextClearBit = completedInputSet.nextClearBit(compositeInput.getInputIdentifier());
          int maxClearBit = compositeInput.getInputIdentifier() + compositeInput.getInputIdentifierCount();
          alreadyCompleted = nextClearBit > maxClearBit;
        } else {
          alreadyCompleted = completedInputSet.get(input.getInputIdentifier());
        }
        if (alreadyCompleted) {
          inputIter.remove();
          continue;
        }
      }

      // avoid adding attempts which have been marked as OBSOLETE
      if (isObsoleteInputAttemptIdentifier(input)) {
        LOG.info("Skipping obsolete input: " + input);
        inputIter.remove();
        continue;
      }

      // check if max threshold is met, and add back to input if too many
      if (includedMaps >= shuffleManagerServer.getMaxTaskOutputAtOnce()) {
        inputIter.remove();
        inputHost.addKnownInput(
            this,
            pendingInputs.getPartition(),
            pendingInputs.getPartitionCount(), input, shuffleManagerServer.getPendingHosts());
      } else {
        includedMaps++;
      }
    }

    synchronized (shuffleInfoEventsMap) {
      for (InputAttemptIdentifier input : pendingInputs.getInputs()) {
        ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(input.getInputIdentifier());
        if (eventInfo != null) {
          eventInfo.scheduledForDownload = true;
        }
      }
    }
  }

  // can run in ShuffleManagerServer.call() thread, ShuffleInputEventHandler thread, Fetcher thread
  private boolean validateInputAttemptForPipelinedShuffle(InputAttemptIdentifier input) {
    // for pipelined shuffle
    // TODO: TEZ-2132 for error handling. As of now, fail fast if there is a different attempt
    if (input.canRetrieveInputInChunks()) {
      ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(input.getInputIdentifier());
      if (eventInfo != null && input.getAttemptNumber() != eventInfo.attemptNum) {
        // guard eventInfo because we access eventInfo's mutable fields
        synchronized (shuffleInfoEventsMap) {
          if (eventInfo.scheduledForDownload || !eventInfo.eventsProcessed.isEmpty()) {
            IOException exception = new IOException("Previous event already got scheduled for " +
                    input + ". Previous attempt's data could have been already merged "
                    + "to memory/disk outputs.  Killing (self) this task early."
                    + " currentAttemptNum=" + eventInfo.attemptNum
                    + ", eventsProcessed=" + eventInfo.eventsProcessed
                    + ", scheduledForDownload=" + eventInfo.scheduledForDownload
                    + ", newAttemptNum=" + input.getAttemptNumber());
            String message = "Killing self as previous attempt data could have been consumed";
            killSelf(exception, message);
            return false;
          }
        }
      }
    }
    return true;
  }

  private void killSelf(Exception exception, String message) {
    LOG.error(message, exception);
    inputContext.killSelf(exception, message);
  }

  private boolean isCompletedInput(int inputIdentifier) {
    if (!completedInputSet.get(inputIdentifier)) {
      synchronized (completedInputSet) {
        if (!completedInputSet.get(inputIdentifier)) {
          return false;
        } else
          return true;
      }
    } else {
      return true;
    }
  }

  /////////////////// Methods for ShuffleInputEventHandler

  public void addKnownInput(String hostName, int port,
                            CompositeInputAttemptIdentifier srcAttemptIdentifier, int partitionId) {
    if (!validateInputAttemptForPipelinedShuffle(srcAttemptIdentifier)) {
      return;
    }

    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    for (int i = 0; i < srcAttemptIdentifier.getInputIdentifierCount(); i++) {
      if (shuffleInfoEventsMap.get(inputIdentifier + i) == null) {
        shuffleInfoEventsMap.put(inputIdentifier + i, new ShuffleEventInfo(srcAttemptIdentifier.expand(i)));
      }
    }

    shuffleManagerServer.addKnownInput(this, hostName, port, srcAttemptIdentifier, partitionId);
  }

  public void addCompletedInputWithNoData(
      InputAttemptIdentifier srcAttemptIdentifier) {
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    if (LOG.isDebugEnabled()) {
      LOG.debug("No input data exists for SrcTask: " + inputIdentifier + ". Marking as complete.");
    }

    boolean isCompleted = isCompletedInput(inputIdentifier);
    if (!isCompleted) {
      NullFetchedInput fetchedInput = new NullFetchedInput(srcAttemptIdentifier);
      if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
        registerCompletedInput(fetchedInput);
      } else {
        registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier, fetchedInput);
      }
    }
  }

  public void addCompletedInputWithData(
      InputAttemptIdentifier srcAttemptIdentifier, FetchedInput fetchedInput)
      throws IOException {
    //InputIdentifier inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received Data via Event: " + srcAttemptIdentifier + " to " + fetchedInput.getType());
    }

    boolean isCompleted = isCompletedInput(inputIdentifier);
    if (!isCompleted) {
      fetchedInput.commit();
      if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
        registerCompletedInput(fetchedInput);
      } else {
        registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier, fetchedInput);
      }
    } else {
      fetchedInput.abort();   // if this fails, the fetcher may attempt another
    }
  }

  public void updateEventReceivedTime() {
    long relativeTime = System.currentTimeMillis() - startTime;
    if (firstEventReceived.getValue() == 0) {
      firstEventReceived.setValue(relativeTime);
      lastEventReceived.setValue(relativeTime);
      return;
    }
    lastEventReceived.setValue(relativeTime);
  }

  public void obsoleteKnownInput(InputAttemptIdentifier srcAttemptIdentifier) {
    obsoletedInputs.add(srcAttemptIdentifier);
  }

  /////////////////// End of Methods for ShuffleInputEventHandler
  /////////////////// fetchSucceeded/fetchFailed() from Fetcher

  /**
   * Placeholder for tracking shuffle events in case we get multiple spills info for the same
   * attempt.
   */
  static class ShuffleEventInfo {
    final int attemptNum;
    final String id;

    // not thread-safe, so guard with: synchronized (shuffleInfoEventsMap)
    BitSet eventsProcessed;
    int finalEventId = -1;        // 0 indexed
    boolean scheduledForDownload; // whether chunks got scheduled for download

    ShuffleEventInfo(InputAttemptIdentifier input) {
      this.id = input.getInputIdentifier() + "_" + input.getAttemptNumber();
      this.attemptNum = input.getAttemptNumber();
      this.eventsProcessed = new BitSet();
    }

    // invariant: inside synchronized (shuffleInfoEventsMap)
    void spillProcessed(int spillId) {
      if (finalEventId != -1) {
        Preconditions.checkState(eventsProcessed.cardinality() <= (finalEventId + 1),
            "Wrong state. eventsProcessed cardinality={} finalEventId={}, spillId={}, {}",
        eventsProcessed.cardinality(), finalEventId, spillId, toString());
      }
      eventsProcessed.set(spillId);
    }

    // invariant: inside synchronized (shuffleInfoEventsMap)
    boolean isDone() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("finalEventId=" + finalEventId + ", eventsProcessed cardinality=" +
            eventsProcessed.cardinality());
      }
      return ((finalEventId != -1) && (finalEventId + 1) == eventsProcessed.cardinality());
    }

    // called from registerCompletedInputForPipelinedShuffle() and no need guard there
    public String toString() {
      return "[eventsProcessed=" + eventsProcessed + ", finalEventId=" + finalEventId
          +  ", id=" + id + ", attemptNum=" + attemptNum
          + ", scheduledForDownload=" + scheduledForDownload + "]";
    }
  }

  // called from (multiple) Fetcher threads, via ShuffleManagerServer
  public void fetchSucceeded(String host, InputAttemptIdentifier srcAttemptIdentifier,
      FetchedInput fetchedInput, long fetchedBytes, long decompressedLength, long copyDuration)
      throws IOException {
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();

    // Count irrespective of whether this is a copy of an already fetched input
    boolean isCompleted = isCompletedInput(inputIdentifier);
    if (!isCompleted) {
      fetchedInput.commit();
      fetchStatsLogger.logIndividualFetchComplete(copyDuration,
          fetchedBytes, decompressedLength, fetchedInput.getType().toString(), srcAttemptIdentifier);

      // Processing counters for completed and commit fetches only. Need
      // additional counters for excessive fetches - which primarily comes
      // in after speculation or retries.
      shuffledInputsCounter.increment(1);
      bytesShuffledCounter.increment(fetchedBytes);
      if (fetchedInput.getType() == Type.MEMORY) {
        bytesShuffledToMemCounter.increment(fetchedBytes);
      } else if (fetchedInput.getType() == Type.DISK) {
        bytesShuffledToDiskCounter.increment(fetchedBytes);
      } else if (fetchedInput.getType() == Type.DISK_DIRECT) {
        bytesShuffledDirectDiskCounter.increment(fetchedBytes);
      }
      decompressedDataSizeCounter.increment(decompressedLength);

      if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
        registerCompletedInput(fetchedInput);
      } else {
        registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier, fetchedInput);
      }

      long totalBytes = totalBytesShuffledTillNow.addAndGet(fetchedBytes);
      logProgress(totalBytes);
    } else {
      fetchedInput.abort(); // If this fails, the fetcher may attempt another abort.
    }
  }

  // called from ShuffleInputEventHandler thread, Fetcher thread
  private void registerCompletedInput(FetchedInput fetchedInput) {
    maybeInformInputReady(fetchedInput);
    // call adjustCompletedInputs() because this is not pipelined shuffle
    adjustCompletedInputs(fetchedInput);

    numFetchedSpills.getAndIncrement();
  }

  // called from ShuffleInputEventHandler thread, Fetcher thread
  private void registerCompletedInputForPipelinedShuffle(
      InputAttemptIdentifier srcAttemptIdentifier, FetchedInput fetchedInput) {
    /**
     * For pipelinedshuffle it is possible to get multiple spills. Claim success only when
     * all spills pertaining to an attempt are done.
     */
    if (!validateInputAttemptForPipelinedShuffle(srcAttemptIdentifier)) {
      return;
    }

    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);

    // for empty partition case
    if (eventInfo == null && fetchedInput instanceof NullFetchedInput) {
      eventInfo = new ShuffleEventInfo(srcAttemptIdentifier);
      shuffleInfoEventsMap.put(inputIdentifier, eventInfo);
    }

    boolean eventInfoIsDone;
    synchronized (shuffleInfoEventsMap) {   // guard because we update eventInfo
      eventInfo.spillProcessed(srcAttemptIdentifier.getSpillEventId());
      numFetchedSpills.getAndIncrement();
      if (srcAttemptIdentifier.getFetchTypeInfo() == InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE) {
        eventInfo.finalEventId = srcAttemptIdentifier.getSpillEventId();
      }
      eventInfoIsDone = eventInfo.isDone();
    }

    /**
     * When fetch is complete for a spill, add it to completedInputs to ensure that it is
     * available for downstream processing. Final success will be claimed only when all
     * spills are downloaded from the source.
     */
    maybeInformInputReady(fetchedInput);
    // call adjustCompletedInputs() only if we have downloaded all spills pertaining to this InputAttemptIdentifier
    if (eventInfoIsDone) {
      adjustCompletedInputs(fetchedInput);
      shuffleInfoEventsMap.remove(srcAttemptIdentifier.getInputIdentifier());
    }

    if (LOG.isTraceEnabled()) {
      // no need to guard eventInfo because we want to print its thread-local state
      LOG.trace("eventInfo " + eventInfo.toString());
    }
  }

  private void maybeInformInputReady(FetchedInput fetchedInput) {
    if (!(fetchedInput instanceof NullFetchedInput)) {
      completedInputs.add(fetchedInput);
    }
    if (!inputReadyNotificationSent.getAndSet(true)) {
      // TODO Should eventually be controlled by Inputs which are processing the data.
      inputContext.inputIsReady();
    }
  }

  private void adjustCompletedInputs(FetchedInput fetchedInput) {
    synchronized (completedInputSet) {
      completedInputSet.set(fetchedInput.getInputAttemptIdentifier().getInputIdentifier());
    }

    int numComplete = numCompletedInputs.incrementAndGet();
    if (numComplete == numInputs) {
      // Poison pill End of Input message to awake blocking take call
      completedInputs.add(endOfInputMarker);
      LOG.info("All inputs fetched for input vertex : " + inputContext.getSourceVertexName());
    }
  }

  // called from Fetcher threads, via ShuffleManagerServer
  public void fetchFailed(String host,
      InputAttemptIdentifier srcAttemptIdentifier, boolean connectFailed) {
    LOG.info("{}: Fetch failed, InputIdentifier={}, connectFailed={}",
        srcNameTrimmed, srcAttemptIdentifier, connectFailed);
    failedShufflesCounter.increment(1);
    if (srcAttemptIdentifier == null) {
      reportFatalError(null, "Received fetchFailure for an unknown src (null)");
    }

    // we send InputReadError regardless of connectFailed (Cf. gla2019.6.10.pptx, page 21)
    // TODO NEWTEZ. Implement logic to report fetch failures after a threshold.
    // For now, reporting immediately.
    if (isObsoleteInputAttemptIdentifier(srcAttemptIdentifier)) {
      LOG.info("Do not report obsolete input: " + srcAttemptIdentifier);
      return;
    }
    InputReadErrorEvent readError = InputReadErrorEvent.create(
        "Unordered: Fetch failure while fetching from "
            + TezRuntimeUtils.getTaskAttemptIdentifier(
            inputContext.getSourceVertexName(),
            srcAttemptIdentifier.getInputIdentifier(),
            srcAttemptIdentifier.getAttemptNumber()),
        srcAttemptIdentifier.getInputIdentifier(),
        srcAttemptIdentifier.getAttemptNumber());

    List<Event> failedEvents = Lists.newArrayListWithCapacity(1);
    failedEvents.add(readError);
    inputContext.sendEvents(failedEvents);
  }

  private void reportFatalError(Throwable exception, String message) {
    LOG.error(message);
    inputContext.reportFailure(TaskFailureType.NON_FATAL, exception, message);
  }

  // thread-safe because InputAttemptIdentifier is immutable
  private boolean isObsoleteInputAttemptIdentifier(InputAttemptIdentifier input) {
    if (input == null || obsoletedInputs.isEmpty()) {
      return false;
    }
    Iterator<InputAttemptIdentifier> obsoleteInputsIter = obsoletedInputs.iterator();
    while (obsoleteInputsIter.hasNext()) {
      InputAttemptIdentifier obsoleteInput = obsoleteInputsIter.next();
      if (input.include(obsoleteInput.getInputIdentifier(), obsoleteInput.getAttemptNumber())) {
        return true;
      }
    }
    return false;
  }

  /////////////////// End of fetchSucceeded/fetchFailed() from Fetcher

  public void shutdown() {
    shufflePhaseTime.setValue(System.currentTimeMillis() - startTime);

    // TODO: need to cleanup all FetchedInput (DiskFetchedInput, LocalDiskFetchedInput), lockFile
    // As of now relying on job cleanup (when all directories would be cleared)

    if (!isShutdown.getAndSet(true)) {
      LOG.info("Shutting down pending fetchers on source {}", srcNameTrimmed);
      shuffleManagerServer.unregister(shuffleManagerId);
    }
  }

  /**
   * @return the next available input, or null if there are no available inputs.
   *         This method will block if there are currently no available inputs,
   *         but more may become available.
   */
  public FetchedInput getNextInput() throws InterruptedException {
    // block until next input or End of Input message
    // the only place where completedInputs.take() is called
    FetchedInput fetchedInput = completedInputs.take();
    if (fetchedInput == endOfInputMarker) {   // reference equality
      fetchedInput = null;
    }
    return fetchedInput;
  }

  public int getNumInputs() {
    return numInputs;
  }

  public float getNumCompletedInputsFloat() {
    return numCompletedInputs.floatValue();
  }

  /////////////////// End of methods for walking the available inputs


  /**
   * Fake input that is added to the completed input list in case an input does not have any data.
   *
   */
  @VisibleForTesting
  static class NullFetchedInput extends FetchedInput {

    public NullFetchedInput(InputAttemptIdentifier inputAttemptIdentifier) {
      super(inputAttemptIdentifier, null);
    }

    @Override
    public Type getType() {
      return Type.MEMORY;
    }

    @Override
    public long getSize() {
      return -1;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }

    @Override
    public InputStream getInputStream() throws IOException {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }

    @Override
    public void commit() throws IOException {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }

    @Override
    public void abort() throws IOException {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }

    @Override
    public void free() {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }
  }

  private void logProgress(long totalBytesShuffledTillNow) {
    int inputsDone = numCompletedInputs.get();
    if (inputsDone == numInputs) {
      double mbs = (double) totalBytesShuffledTillNow / (1024 * 1024);
      long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;

      double transferRate = mbs / secsSinceStart;
      StringBuilder s = new StringBuilder();
      s.append("copy=" + inputsDone);
      s.append(", numFetchedSpills=" + numFetchedSpills);
      s.append(", numInputs=" + numInputs);
      s.append(", transfer rate (MB/s) = " + ShuffleUtils.MBPS_FORMAT.get().format(transferRate));  // CumulativeDataFetched/TimeSinceInputStarted
      LOG.info(s.toString());
    }
  }
}
