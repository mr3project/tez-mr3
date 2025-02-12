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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.tez.runtime.library.common.shuffle.ShuffleClient;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils.FetchStatsLogger;

import org.apache.tez.common.Preconditions;
import com.google.common.collect.Lists;

// This only knows how to deal with a single srcIndex for a given targetIndex.
// In case the src task generates multiple outputs for the same target Index
// (multiple src-indices), modifications will be required.
public class ShuffleManager extends ShuffleClient<FetchedInput> {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleManager.class);
  private static final Logger LOG_FETCH = LoggerFactory.getLogger(LOG.getName() + ".fetch");
  private static final FetchStatsLogger fetchStatsLogger = new FetchStatsLogger(LOG_FETCH, LOG);

  private final FetchedInputAllocator inputManager;

  private final TezCounter approximateInputRecords;
  private final TezCounter shuffledInputsCounter;
  private final TezCounter failedShufflesCounter;
  private final TezCounter bytesShuffledCounter;
  private final TezCounter decompressedDataSizeCounter;
  private final TezCounter bytesShuffledToDiskCounter;
  private final TezCounter bytesShuffledToMemCounter;
  private final TezCounter bytesShuffledDirectDiskCounter;
  private final TezCounter shufflePhaseTime;

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

  public ShuffleManager(InputContext inputContext, Configuration conf, int numInputs,
      FetchedInputAllocator inputAllocator, String srcNameTrimmed) throws IOException {
    super(inputContext, conf, numInputs, srcNameTrimmed);

    this.inputManager = inputAllocator;

    this.approximateInputRecords = inputContext.getCounters().findCounter(TaskCounter.APPROXIMATE_INPUT_RECORDS);
    this.shuffledInputsCounter = inputContext.getCounters().findCounter(TaskCounter.NUM_SHUFFLED_INPUTS);
    this.failedShufflesCounter = inputContext.getCounters().findCounter(TaskCounter.NUM_FAILED_SHUFFLE_INPUTS);
    this.bytesShuffledCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES);
    this.decompressedDataSizeCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_DECOMPRESSED);
    this.bytesShuffledToDiskCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_TO_DISK);
    this.bytesShuffledToMemCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_TO_MEM);
    this.bytesShuffledDirectDiskCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_DISK_DIRECT);
    this.shufflePhaseTime = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_PHASE_TIME);

    this.startTime = System.currentTimeMillis();

    // In case of pipelined shuffle, it is possible to get multiple FetchedInput per attempt.
    // We do not know upfront the number of spills from source.
    completedInputs = new LinkedBlockingDeque<FetchedInput>();

    LOG.info("ShuffleManager for {}: numInputs={}", srcNameTrimmed, numInputs);
  }

  public FetchedInputAllocator getInputManager() {
    return inputManager;
  }

  // called only when inputContext.useShuffleHandlerProcessOnK8s()
  public int[] getLocalShufflePorts() {
    return shuffleServer.getLocalShufflePorts();
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

  // can run in ShuffleServer.call() thread, ShuffleInputEventHandler thread, Fetcher thread
  protected boolean validateInputAttemptForPipelinedShuffle(
      InputAttemptIdentifier input, boolean registerShuffleInfoEvent) {
    // for pipelined shuffle
    // TODO: TEZ-2132 for error handling. As of now, fail fast if there is a different attempt
    if (input.canRetrieveInputInChunks()) {
      synchronized (shuffleInfoEventsMap) {
        int inputIdentifier = input.getInputIdentifier();
        ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);

        if (eventInfo != null && input.getAttemptNumber() != eventInfo.attemptNum) {
          // there is a slight chance that the following assert{} is invalid (Cf. registerCompletedInputForPipelinedShuffle())
          // assert !eventInfo.eventsProcessed.isEmpty();
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
      }
    }

    return true;
  }

  private void killSelf(Exception exception, String message) {
    LOG.error(message, exception);
    inputContext.killSelf(exception, message);
  }

  /////////////////// Methods for ShuffleInputEventHandler

  // called sequentially from ShuffleInputEventHandler thread
  public void addKnownInput(String hostName, int port,
                            CompositeInputAttemptIdentifier srcAttemptIdentifier, int partitionId) {
    // Note: this check is optional.
    // if we skip this check, we call killSelf() after fetches with different attemptNumbers succeed
    if (!validateInputAttemptForPipelinedShuffle(srcAttemptIdentifier, false)) {
      return;
    }

    shuffleServer.addKnownInput(this, hostName, port, srcAttemptIdentifier, partitionId);
  }

  public void addCompletedInputWithNoData(
      InputAttemptIdentifier srcAttemptIdentifier) {
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    if (LOG.isDebugEnabled()) {
      LOG.debug("No input data exists for SrcTask: " + inputIdentifier + ". Marking as complete.");
    }

    synchronized (completedInputSet) {
      boolean isCompleted = completedInputSet.get(inputIdentifier);
      if (!isCompleted) {
        NullFetchedInput fetchedInput = new NullFetchedInput(srcAttemptIdentifier);
        if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
          registerCompletedInput(fetchedInput);
        } else {
          registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier, fetchedInput);
        }
      }
    }
  }

  public void addCompletedInputWithData(
      InputAttemptIdentifier srcAttemptIdentifier, FetchedInput fetchedInput) throws IOException {
    //InputIdentifier inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received Data via Event: " + srcAttemptIdentifier + " to " + fetchedInput.getType());
    }

    synchronized (completedInputSet) {
      boolean isCompleted = completedInputSet.get(inputIdentifier);
      if (!isCompleted) {
        fetchedInput.commit();
        // 1. 'pipelined == false && merged == true'  --> FINAL_MERGE_ENABLED == true
        //    --> !srcAttemptIdentifier.canRetrieveInputInChunks()
        // 2. 'pipelined == true  && merged == false' --> canSendDataOverDME() is never true
        //    --> addCompletedInputWithData() is never called
        // 3. 'pipelined == false && merged == false' --> should never be used (not supported)
        // 4. 'pipelined == true  && merged == true'  --> wrong combination
        Preconditions.checkState(!srcAttemptIdentifier.canRetrieveInputInChunks(),
          "Received data via event, but spills are used: {}", srcAttemptIdentifier);
        registerCompletedInput(fetchedInput);
      } else {
        fetchedInput.abort();
      }
    }
  }

  /////////////////// End of Methods for ShuffleInputEventHandler
  /////////////////// fetchSucceeded/fetchFailed() from Fetcher

  // called from (multiple) Fetcher threads, via ShuffleServer
  public void fetchSucceeded(
      InputAttemptIdentifier srcAttemptIdentifier,
      FetchedInput fetchedInput,
      long fetchedBytes, long decompressedLength, long copyDuration) throws IOException {
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();

    boolean updateStats = false;
    synchronized (completedInputSet) {
      boolean isCompleted = completedInputSet.get(inputIdentifier);
      if (!isCompleted) {
        fetchedInput.commit();
        if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
          registerCompletedInput(fetchedInput);
        } else {
          registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier, fetchedInput);
        }
        updateStats = true;
      } else {
        LOG.warn("Duplicate fetch of unordered input: {}", srcAttemptIdentifier);
        fetchedInput.abort();
      }
    }

    if (updateStats) {
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

      long totalBytes = totalBytesShuffledTillNow.addAndGet(fetchedBytes);
      logProgress(totalBytes);
    }
  }

  // called from ShuffleInputEventHandler thread, Fetcher thread
  // inside synchronized (completedInputSet)
  private void registerCompletedInput(FetchedInput fetchedInput) {
    maybeInformInputReady(fetchedInput);
    // call adjustCompletedInputs() because this is not pipelined shuffle
    adjustCompletedInputs(fetchedInput);
    numFetchedSpills.getAndIncrement();
  }

  // called from ShuffleInputEventHandler thread, Fetcher thread
  // inside synchronized (completedInputSet)
  private void registerCompletedInputForPipelinedShuffle(
      InputAttemptIdentifier srcAttemptIdentifier, FetchedInput fetchedInput) {
    if (isObsoleteInputAttemptIdentifier(srcAttemptIdentifier)) {
      LOG.info("Do not register obsolete input: " + srcAttemptIdentifier);
      return;
    }

    /**
     * For pipelined shuffle, it is possible to get multiple spills. Claim success only when
     * all spills pertaining to an attempt are done.
     */
    if (!validateInputAttemptForPipelinedShuffle(srcAttemptIdentifier, true)) {
      return;
    }

    boolean eventInfoIsDone;
    synchronized (shuffleInfoEventsMap) {   // guard because we update eventInfo
      int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
      ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);
      assert eventInfo != null;

      eventInfo.spillProcessed(srcAttemptIdentifier.getSpillEventId());
      numFetchedSpills.getAndIncrement();
      if (srcAttemptIdentifier.getFetchTypeInfo() == InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE) {
        eventInfo.setFinalEventId(srcAttemptIdentifier.getSpillEventId());
      }

      eventInfoIsDone = eventInfo.isDone();
      if (eventInfoIsDone) {
        shuffleInfoEventsMap.remove(inputIdentifier);
      }
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

  // inside synchronized (completedInputSet)
  private void adjustCompletedInputs(FetchedInput fetchedInput) {
    completedInputSet.set(fetchedInput.getInputAttemptIdentifier().getInputIdentifier());

    int numComplete = numCompletedInputs.incrementAndGet();
    if (numComplete == numInputs) {
      // Poison pill End of Input message to awake blocking take call
      completedInputs.add(endOfInputMarker);
      LOG.info("All inputs fetched for input vertex : " + inputContext.getSourceVertexName());
    }
  }

  // called from Fetcher threads, via ShuffleServer (except calls from FetchFutureCallback.onSuccess())
  // readFailed is not used in ShuffleManager
  public void fetchFailed(
      InputAttemptIdentifier srcAttemptIdentifier, boolean readFailed, boolean connectFailed) {
    assert !readFailed;   // ignore in ShuffleManager
    final int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    failedShufflesCounter.increment(1);

    synchronized (completedInputSet) {
      boolean isCompleted = completedInputSet.get(inputIdentifier);
      if (isCompleted) {
        LOG.warn("{}, Unordered fetch failed, but input already completed: InputIdentifier={}",
          srcNameTrimmed, srcAttemptIdentifier);
        return;
      }
    }

    LOG.info("{}: Fetch failed, InputIdentifier={}, connectFailed={}",
        srcNameTrimmed, srcAttemptIdentifier, connectFailed);

    if (srcAttemptIdentifier == null) {
      reportNonFatalError(null, "Received fetchFailure for an unknown source (null)");
    }

    if (isObsoleteInputAttemptIdentifier(srcAttemptIdentifier)) {
      LOG.info("Do not report obsolete input: " + srcAttemptIdentifier);
      return;
    }

    // we send InputReadError regardless of connectFailed (Cf. gla2019.6.10.pptx, page 21)
    InputReadErrorEvent readError = InputReadErrorEvent.create(
        "Unordered: Fetch failure while fetching from "
            + TezRuntimeUtils.getTaskAttemptIdentifier(
            inputContext.getSourceVertexName(),
            inputIdentifier,
            srcAttemptIdentifier.getAttemptNumber()),
        srcAttemptIdentifier.getInputIdentifier(),
        srcAttemptIdentifier.getAttemptNumber());
    List<Event> failedEvents = Lists.newArrayListWithCapacity(1);
    failedEvents.add(readError);
    inputContext.sendEvents(failedEvents);

    if (srcAttemptIdentifier.canRetrieveInputInChunks()) {
      synchronized (shuffleInfoEventsMap) {
        ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);

        if (eventInfo != null && srcAttemptIdentifier.getAttemptNumber() == eventInfo.attemptNum) {
          // some spills with the same attempt number have been downloaded, so this TaskAttempt cannot succeed
          reportNonFatalError(null, "Failed to fetch input " + srcAttemptIdentifier);
        } else {
          LOG.warn("Unordered fetch failed, but do not kill yet because no spill has been downloaded yet: {}", srcAttemptIdentifier);
        }
      }
    } else {
      LOG.warn("Unordered fetch failed, but do not kill (not pipelined): {}", srcAttemptIdentifier);
    }
  }

  private void reportNonFatalError(Throwable exception, String message) {
    LOG.error(message);
    inputContext.reportFailure(TaskFailureType.NON_FATAL, exception, message);
  }

  /////////////////// End of fetchSucceeded/fetchFailed() from Fetcher

  public void shutdown() {
    shufflePhaseTime.setValue(System.currentTimeMillis() - startTime);

    // TODO: need to cleanup all FetchedInput (DiskFetchedInput, LocalDiskFetchedInput), lockFile
    // As of now relying on job cleanup (when all directories would be cleared)

    if (!isShutdown.getAndSet(true)) {
      LOG.info("Shutting down pending fetchers: shuffleClientId={}, source={}", shuffleClientId, srcNameTrimmed);
      shuffleServer.unregister(shuffleClientId);
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
