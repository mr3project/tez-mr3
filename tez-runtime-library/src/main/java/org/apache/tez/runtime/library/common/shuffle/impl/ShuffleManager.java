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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput.Type;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.common.shuffle.MemoryFetchedInput;
import org.apache.tez.runtime.library.common.shuffle.ShuffleClient;

import org.apache.tez.common.Preconditions;
import com.google.common.collect.Lists;

// This only knows how to deal with a single srcIndex for a given targetIndex.
// In case the src task generates multiple outputs for the same target Index
// (multiple src-indices), modifications will be required.
public class ShuffleManager extends ShuffleClient<FetchedInput> {

  private final FetchedInputAllocator inputManager;

  private final TezCounter approximateInputRecords;
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
  // Except for endOfInputMarker, all FetchedInputs are in State.COMMITTED.
  private final BlockingQueue<FetchedInput> completedInputs;
  private static final FetchedInput endOfInputMarker = new NullFetchedInput(null);

  // sum of the sizes of all MemoryFetchedInput in completedInputs[]
  private final AtomicLong totalSizeOfMemoryCompletedInputs = new AtomicLong(0L);
  private final AtomicInteger numCallsGetNextInput = new AtomicInteger(0);

  // Use striped locks to serialize completion for a specific inputIdentifier while allowing
  // unrelated inputIdentifiers to proceed in parallel.
  //
  // Variables/invariants guarded by lockForInput(inputIdentifier):
  //  - per-input completion transaction in addCompletedInputWithNoData(), addCompletedInputWithData(),
  //    and fetchSucceeded() (check-complete -> commit/abort -> queue/register)
  //  - per-input queueing/notification side effects in maybeInformInputReady()
  //  - per-input finalization in adjustCompletedInputs()
  //
  // Note: completedInputSet itself is ALWAYS guarded with synchronized(completedInputSet).
  // lockForInput(inputIdentifier) protects cross-variable atomicity for one inputIdentifier,
  // not raw access to completedInputSet.
  // This per-input locking model also replaces the old global map monitor
  // (synchronized(shuffleInfoEventsMap)) for pipelined completion paths.

  public ShuffleManager(InputContext inputContext, Configuration conf, int numInputs,
      FetchedInputAllocator inputAllocator, String srcNameTrimmed) throws IOException {
    super(inputContext, conf, numInputs, srcNameTrimmed);

    this.inputManager = inputAllocator;

    this.approximateInputRecords = inputContext.getCounters().findCounter(TaskCounter.APPROXIMATE_INPUT_RECORDS);
    this.shufflePhaseTime = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_PHASE_TIME);

    this.startTime = System.currentTimeMillis();

    // In case of pipelined shuffle, it is possible to get multiple FetchedInput per attempt.
    // We do not know upfront the number of spills from source.
    completedInputs = new LinkedBlockingDeque<FetchedInput>();

    LOG.info("ShuffleManager for {}/{}: shuffleClientId={}, numInputs={}",
        inputContext.getUniqueIdentifier(), srcNameTrimmed, shuffleClientId, numInputs);
  }

  public FetchedInputAllocator getInputManager() {
    return inputManager;
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
    inputContext.killSelf(exception, message);
  }

  /////////////////// Methods for ShuffleInputEventHandler

  // called sequentially from ShuffleInputEventHandler thread
  public void addKnownInput(String hostName, String containerId, int port,
                            CompositeInputAttemptIdentifier srcAttemptIdentifier, int partitionId) {
    // Note: this check is optional.
    // if we skip this check, we call killSelf() after fetches with different attemptNumbers succeed
    // use input.getInput() for quick checking
    if (!validateInputAttemptForPipelinedShuffle(srcAttemptIdentifier.getInput())) {
      return;
    }

    shuffleServer.addKnownInput(this, hostName, containerId, port, srcAttemptIdentifier, partitionId);
  }

  public void wakeupLoop() {
    shuffleServer.wakeupLoop();
  }

  public void addCompletedInputWithNoData(
      InputAttemptIdentifier srcAttemptIdentifier) {
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    if (LOG.isDebugEnabled()) {
      LOG.debug("No input data exists for SrcTask: " + inputIdentifier + ". Marking as complete.");
    }

    synchronized (lockForInput(inputIdentifier)) {
      boolean isCompleted = isInputFinished(inputIdentifier);
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
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received Data via Event: " + srcAttemptIdentifier + " to " + fetchedInput.getType());
    }

    synchronized (lockForInput(inputIdentifier)) {
      boolean isCompleted = isInputFinished(inputIdentifier);
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
      long bytesCompressed,
      long bytesDecompressed,
      long copyDuration) throws IOException {
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();

    boolean updateStats = false;
    synchronized (lockForInput(inputIdentifier)) {
      boolean isCompleted = isInputFinished(inputIdentifier);
      if (!isCompleted) {
        if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
          fetchedInput.commit();  // may fail with IOException
          updateStats = true;
          registerCompletedInput(fetchedInput);
        } else {
          CommitRegister cr = checkCommitRegister(srcAttemptIdentifier);
          boolean commitAndRegister = cr.commitAndRegister;
          boolean killInPipelined = cr.killInPipelined;   // killBecauseDifferentSpillAttemptInPipelined
          assert cr.isPipelined;
          assert !(commitAndRegister && killInPipelined);

          // 1. call fetchedInput.commit() or fetchedInput.abort() if necessary
          // consider commitAndRegister only
          if (commitAndRegister) {
            fetchedInput.commit();  // may fail with IOException
            updateStats = true;
            // 2. register completed input for pipelined shuffle
            registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier, fetchedInput);
          } else {
            LOG.warn("Duplicate fetch of unordered input for {} ({}/{} completed): {}",
              inputContext.getUniqueIdentifier(), numCompletedInputs.get(), numInputs, srcAttemptIdentifier);
            shuffleNumDuplicateInputsCounter.increment(1);
            fetchedInput.abort();

            if (!killInPipelined) {
              LOG.info("Unordered spill already processed for {} ({}/{} completed): {}",
                inputContext.getUniqueIdentifier(), numCompletedInputs.get(), numInputs, srcAttemptIdentifier);
            } else {
              String message = "Killing self as previous attempt unordered data could have been consumed in pipelined shuffling";
              IOException exception = new IOException(
                  message + ": " + inputContext.getUniqueIdentifier() + ", " + srcAttemptIdentifier);
              killSelf(exception, message);
            }
          }
        }
      } else {
        // input is already finished. duplicate fetch.
        LOG.warn("Fetch of unordered input after completion for {} ({}/{} completed): {}",
            inputContext.getUniqueIdentifier(), numCompletedInputs.get(), numInputs, srcAttemptIdentifier);
        // free the resource - especially memory
        shuffleNumDuplicateInputsCounter.increment(1);
        fetchedInput.abort();
      }
    }

    if (updateStats) {
      Type type = fetchedInput.getType();
      updateCounters(srcAttemptIdentifier, bytesCompressed, bytesDecompressed, copyDuration,
          type.toString(),
          type == Type.DISK,
          type == Type.DISK_DIRECT,
          type == Type.LOCAL_BYTE_CACHE);
      long totalBytes = totalBytesShuffledTillNow.addAndGet(bytesCompressed);
      logProgress(totalBytes);
    }
  }

  // called from ShuffleInputEventHandler thread, Fetcher thread
  // Called only while holding lockForInput(inputIdentifier).
  private void registerCompletedInput(FetchedInput fetchedInput) {
    maybeInformInputReady(fetchedInput);
    // call adjustCompletedInputs() because this is not pipelined shuffle
    adjustCompletedInputs(fetchedInput);
    numFetchedSpills.getAndIncrement();
  }

  // called from ShuffleInputEventHandler thread, Fetcher thread
  // Called only while holding lockForInput(inputIdentifier).
  private void registerCompletedInputForPipelinedShuffle(
      InputAttemptIdentifier srcAttemptIdentifier, FetchedInput fetchedInput) {
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
    numFetchedSpills.getAndIncrement();
    if (srcAttemptIdentifier.getFetchTypeInfo() == InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE) {
      eventInfo.setFinalEventId(srcAttemptIdentifier.getSpillEventId());
    }

    eventInfoIsDone = eventInfo.isDone();
    if (eventInfoIsDone) {
      shuffleInfoEventsMap.remove(inputIdentifier);
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

  // Called only while holding lockForInput(inputIdentifier).
  private void maybeInformInputReady(FetchedInput fetchedInput) {
    if (!(fetchedInput instanceof NullFetchedInput)) {
      completedInputs.add(fetchedInput);
      if (fetchedInput instanceof MemoryFetchedInput) {
        totalSizeOfMemoryCompletedInputs.addAndGet(fetchedInput.getSize());
      }
    }
    if (!inputReadyNotificationSent.getAndSet(true)) {
      // TODO Should eventually be controlled by Inputs which are processing the data.
      inputContext.inputIsReady();
    }
  }

  // Called only while holding lockForInput(inputIdentifier).
  // completedInputSet access remains guarded by setInputFinished().
  private void adjustCompletedInputs(FetchedInput fetchedInput) {
    setInputFinished(fetchedInput.getInputAttemptIdentifier().getInputIdentifier());

    int numComplete = numCompletedInputs.incrementAndGet();
    if (numComplete == numInputs) {
      // Poison pill End of Input message to awake blocking take call
      completedInputs.add(endOfInputMarker);
      LOG.info("All inputs fetched for ShuffleManager {}", shuffleClientId);
    }
  }

  // called from Fetcher threads, via ShuffleServer (except calls from FetchFutureCallback.onSuccess())
  // readFailed is not used in ShuffleManager
  public void fetchFailed(
      CompositeInputAttemptIdentifier srcAttemptIdentifier, boolean readFailed, boolean connectFailed) {
    assert !readFailed;   // ignore in ShuffleManager
    shuffleNumFailedInputsCounter.increment(1);

    if (isObsoleteInputAttemptIdentifier(srcAttemptIdentifier)) {
      LOG.info("Do not report obsolete unordered input: {}", srcAttemptIdentifier);
      return;
    }

    boolean shouldInformAM = false;
    for (int i = 0; i < srcAttemptIdentifier.getInputIdentifierCount(); i++) {
      InputAttemptIdentifier inputAttemptIdentifier = srcAttemptIdentifier.expand(i);
      int inputIdentifier = inputAttemptIdentifier.getInputIdentifier();

      synchronized (lockForInput(inputIdentifier)) {
        if (isInputFinished(inputIdentifier)) {
          LOG.warn("Unordered fetch failed for {}, but input already completed: InputIdentifier={}",
              shuffleClientId, inputAttemptIdentifier);
          continue;
        }

        shouldInformAM = true;

        if (inputAttemptIdentifier.canRetrieveInputInChunks()) {
          ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);
          if (eventInfo != null && inputAttemptIdentifier.getAttemptNumber() == eventInfo.attemptNum) {
            // some spills with the same attempt number have been downloaded, so this TaskAttempt cannot succeed
            // ShuffleServer.fetchFailed already verified !existsConcurrentNotFailedFetcher, so we should kill here.
            reportNonFatalError("Failed to fetch input " + inputAttemptIdentifier);
          } else {
            LOG.warn("Unordered fetch failed, but do not kill yet because no spill has been downloaded yet: {}", inputAttemptIdentifier);
          }
        } else {
          LOG.warn("Unordered fetch failed, but do not kill (not pipelined): {}", inputAttemptIdentifier);
        }
      }
    }

    if (shouldInformAM) {
      informAM("Unordered", srcAttemptIdentifier);   // send InputReadErrorEvent only, without killing TaskAttempt
    }
  }

  private void reportNonFatalError(String message) {
    LOG.error(message);
    inputContext.reportFailure(TaskFailureType.NON_FATAL, null, message);
  }

  /////////////////// End of fetchSucceeded/fetchFailed() from Fetcher

  public void shutdown() {
    shufflePhaseTime.setValue(System.currentTimeMillis() - startTime);

    // TODO: need to cleanup all FetchedInput (DiskFetchedInput, LocalDiskFetchedInput), lockFile
    // As of now relying on job cleanup (when all directories would be cleared)

    if (!isShutdown.getAndSet(true)) {
      LOG.info("Shutting down pending fetchers: ShuffleManager {}", shuffleClientId);
      shuffleServer.unregister(shuffleClientId);
    }
  }

  /**
   * @return the next available input, or null if there are no available inputs.
   *         This method will block if there are currently no available inputs,
   *         but more may become available.
   */
  public FetchedInput getNextInput() throws InterruptedException {
    numCallsGetNextInput.incrementAndGet();

    // block until next input or End of Input message
    // the only place where completedInputs.take() is called
    FetchedInput fetchedInput = completedInputs.take();

    if (fetchedInput instanceof MemoryFetchedInput) {
      totalSizeOfMemoryCompletedInputs.addAndGet(-fetchedInput.getSize());
    }

    if (fetchedInput == endOfInputMarker) {   // reference equality
      fetchedInput = null;
    }
    return fetchedInput;
  }

  public int getNumCallsGetNextInput() {
    return numCallsGetNextInput.get();
  }

  public int getNumInputs() {
    return numInputs;
  }

  public long getTotalSizeOfMemoryCompletedInputs() {
    return totalSizeOfMemoryCompletedInputs.get();
  }


  /////////////////// End of methods for walking the available inputs

  /**
   * Fake input that is added to the completed input list in case an input does not have any data.
   *
   */
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
      long kbs = totalBytesShuffledTillNow / 1024;
      long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;
      long transferRate = kbs / secsSinceStart;

      StringBuilder s = new StringBuilder();
      s.append("ShuffleManager " + shuffleClientId);
      s.append(", copy=" + inputsDone);
      s.append(", numFetchedSpills=" + numFetchedSpills);
      s.append(", numInputs=" + numInputs);
      s.append(", transfer rate (KB/s) = " + transferRate);
      LOG.info(s.toString());
    }
  }
}
