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

package org.apache.tez.runtime.library.common.shuffle;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.Preconditions;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.FetcherConfig;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ShuffleClient<T extends ShuffleInput> {

  protected static final Logger LOG = LoggerFactory.getLogger(ShuffleClient.class);
  protected static final Logger LOG_FETCH = LoggerFactory.getLogger(LOG.getName() + ".fetch");
  protected static final ShuffleUtils.FetchStatsLogger fetchStatsLogger = new ShuffleUtils.FetchStatsLogger(LOG_FETCH, LOG);

  enum ShuffleErrors {
    IO_ERROR,
    WRONG_LENGTH,
    BAD_ID,
    WRONG_MAP,
    CONNECTION,
    WRONG_REDUCE
  }
  private final static String SHUFFLE_ERR_GRP_NAME = "Shuffle Errors";

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

  /**
   * Placeholder for tracking shuffle events in case we get multiple spills info for the same attempt.
   */
  static public class ShuffleEventInfo {
    public final int attemptNum;
    public final String id;
    private BitSet eventsProcessed;

    private int finalEventId = -1;        // 0 indexed

    public ShuffleEventInfo(InputAttemptIdentifier input) {
      this.id = input.getInputIdentifier() + "_" + input.getAttemptNumber();
      this.attemptNum = input.getAttemptNumber();
      this.eventsProcessed = new BitSet();
    }

    public BitSet getEventsProcessed() {
      return eventsProcessed;
    }

    public void spillProcessed(int spillId) {
      if (finalEventId != -1) {
        Preconditions.checkState(eventsProcessed.cardinality() <= (finalEventId + 1),
          "Wrong state. eventsProcessed cardinality={} finalEventId={}, spillId={}, {}",
          eventsProcessed.cardinality(), finalEventId, spillId, toString());
      }
      eventsProcessed.set(spillId);
    }

    public void setFinalEventId(int spillId) {
      finalEventId = spillId;
    }

    public boolean isDone() {
      return ((finalEventId != -1) && (finalEventId + 1) == eventsProcessed.cardinality());
    }

    public String toString() {
      return "[eventsProcessed=" + eventsProcessed + ", finalEventId=" + finalEventId
        +  ", id=" + id + ", attemptNum=" + attemptNum + "]";
    }
  }

  // not thread-safe - accessed from:
  //   1. ShuffleServer.call() thread
  //   2. ShuffleInputEventHandler thread
  //   3. Fetcher thread via fetchSucceeded()
  protected final BitSet completedInputSet;
  protected final int numInputs;

  protected final InputContext inputContext;

  // passed only to Fetcher and not used elsewhere, so public is okay
  public final Configuration conf;

  protected final String srcNameTrimmed;
  private final String logIdentifier;

  protected final ShuffleServer shuffleServer;
  // use Long instead of long because we need Long when accessing ShuffleServer.shuffleClients[]
  // In particular, the conversion from long to Long can be expensive in ShuffleServer.fetchSucceeded().
  protected final Long shuffleClientId;

  // thread-safe for reading and updating
  // InputAttemptIdentifier is immutable
  private final Set<InputAttemptIdentifier> obsoletedInputs;  // not CompositeInputAttemptIdentifier

  protected final int maxNumFetchers;

  // to track shuffleInfo events when finalMerge is disabled in source or pipelined shuffle is enabled in source.
  // NOTE: ConcurrentHashMap removes the need for a single global map monitor
  // (synchronized(shuffleInfoEventsMap)), but NOT the need for this map itself.
  // We still need per-input ShuffleEventInfo state to validate attempts/spills and detect completion.
  // Invariant: guard multi-step decisions with caller lock (this.synchronized in ShuffleScheduler,
  //            lockForInput(inputIdentifier) in ShuffleManager).
  protected final Map<Integer, ShuffleEventInfo> shuffleInfoEventsMap;

  private int numFetchers = 0;
  private int numPartitionRanges = 0;
  private final Object lock = new Object();

  private final TezCounter shuffleNumFetchersCounter;

  private final TezCounter shuffleNumInputsCounter;
  protected final TezCounter shuffleNumDuplicateInputsCounter;
  protected final TezCounter shuffleNumFailedInputsCounter;

  private final TezCounter shuffleBytesCounter;
  private final TezCounter shuffleBytesDecompressedCounter;

  private final TezCounter shuffleBytesDiskCounter;
  private final TezCounter shuffleBytesDiskDirectCounter;
  private final TezCounter shuffleBytesMemoryCounter;

  private final ShuffleErrorCounterGroup shuffleErrorCounterGroup;

  public ShuffleClient(
      InputContext inputContext,
      Configuration conf,
      int numInputs,
      String srcNameTrimmed) throws IOException {
    this.shuffleServer = (ShuffleServer)inputContext.getShuffleServer();
    this.inputContext = inputContext;
    this.conf = conf;
    this.srcNameTrimmed = srcNameTrimmed;
    this.logIdentifier = inputContext.getUniqueIdentifier() + "-" + srcNameTrimmed;

    this.numInputs = numInputs;
    this.completedInputSet = new BitSet(numInputs);

    this.obsoletedInputs = Collections.newSetFromMap(new ConcurrentHashMap<InputAttemptIdentifier, Boolean>());

    this.maxNumFetchers = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES_DEFAULT);

    this.shuffleInfoEventsMap = new ConcurrentHashMap<Integer, ShuffleEventInfo>();

    this.shuffleClientId = shuffleServer.register(this);

    TezCounters counters = inputContext.getCounters();
    this.shuffleNumFetchersCounter = counters.findCounter(TaskCounter.SHUFFLE_NUM_FETCHERS);
    this.shuffleNumInputsCounter = counters.findCounter(TaskCounter.SHUFFLE_NUM_INPUTS);
    this.shuffleNumDuplicateInputsCounter = counters.findCounter(TaskCounter.SHUFFLE_NUM_DUPLICATE_INPUTS);
    this.shuffleNumFailedInputsCounter = counters.findCounter(TaskCounter.SHUFFLE_NUM_FAILED_INPUTS);
    this.shuffleBytesCounter = counters.findCounter(TaskCounter.SHUFFLE_BYTES);
    this.shuffleBytesDecompressedCounter = counters.findCounter(TaskCounter.SHUFFLE_BYTES_DECOMPRESSED);
    this.shuffleBytesDiskCounter = counters.findCounter(TaskCounter.SHUFFLE_BYTES_DISK);
    this.shuffleBytesDiskDirectCounter = counters.findCounter(TaskCounter.SHUFFLE_BYTES_DISK_DIRECT);
    this.shuffleBytesMemoryCounter = counters.findCounter(TaskCounter.SHUFFLE_BYTES_MEMORY);

    TezCounter ioErrsCounter = counters.findCounter(SHUFFLE_ERR_GRP_NAME, ShuffleErrors.IO_ERROR.toString());
    TezCounter wrongLengthErrsCounter = counters.findCounter(SHUFFLE_ERR_GRP_NAME, ShuffleErrors.WRONG_LENGTH.toString());
    TezCounter badIdErrsCounter = counters.findCounter(SHUFFLE_ERR_GRP_NAME, ShuffleErrors.BAD_ID.toString());
    TezCounter wrongMapErrsCounter = counters.findCounter(SHUFFLE_ERR_GRP_NAME, ShuffleErrors.WRONG_MAP.toString());
    TezCounter connectionErrsCounter = counters.findCounter(SHUFFLE_ERR_GRP_NAME, ShuffleErrors.CONNECTION.toString());
    TezCounter wrongReduceErrsCounter = counters.findCounter(SHUFFLE_ERR_GRP_NAME, ShuffleErrors.WRONG_REDUCE.toString());
    this.shuffleErrorCounterGroup = new ShuffleErrorCounterGroup(
        ioErrsCounter, wrongLengthErrsCounter,
        badIdErrsCounter, wrongMapErrsCounter,
        connectionErrsCounter, wrongReduceErrsCounter);
  }

  public int getNumInputs() {
    return numInputs;
  }

  public String getLogIdentifier() {
    return logIdentifier;
  }

  public ShuffleErrorCounterGroup getShuffleErrorCounterGroup() {
    return shuffleErrorCounterGroup;
  }

  protected void setInputFinished(int inputIndex) {
    synchronized (completedInputSet) {
      completedInputSet.set(inputIndex, true);
    }
  }

  protected boolean isInputFinished(int inputIndex) {
    synchronized (completedInputSet) {
      return completedInputSet.get(inputIndex);
    }
  }

  // inside ShuffleServer.call() thread
  protected boolean cleanInputHostForConstructFetcher(InputHost.PartitionToInputs pendingInputs) {
    // safe to update pendingInputs because we are running in ShuffleServer.call() thread
    // use '==' instead of 'equals' because we want to avoid conversion from long to Long
    assert pendingInputs.getShuffleClientId() == shuffleClientId;
    assert pendingInputs.getInputs().size() <= shuffleServer.getMaxTaskOutputAtOnce();

    boolean removedAnyInput = false;

    // avoid adding attempts which have already been completed
    // guard with synchronized because completedInputSet should not be updated while traversing
    synchronized (completedInputSet) {
      for (Iterator<CompositeInputAttemptIdentifier> inputIter = pendingInputs.getInputs().iterator();
           inputIter.hasNext();) {
        CompositeInputAttemptIdentifier compositeInput = inputIter.next();

        int nextClearBit = completedInputSet.nextClearBit(compositeInput.getInputIdentifier());
        int maxClearBit = compositeInput.getInputIdentifier() + compositeInput.getInputIdentifierCount();
        boolean alreadyCompleted = nextClearBit > maxClearBit;

        if (alreadyCompleted) {
          LOG.info("Skipping completed input: {}", compositeInput);
          inputIter.remove();
          removedAnyInput = true;
        }
      }
    }

    for (Iterator<CompositeInputAttemptIdentifier> inputIter = pendingInputs.getInputs().iterator();
         inputIter.hasNext();) {
      CompositeInputAttemptIdentifier input = inputIter.next();

      // avoid adding attempts which have been marked as OBSOLETE
      if (isObsoleteInputAttemptIdentifier(input)) {
        LOG.info("Skipping obsolete input: {}", input);
        inputIter.remove();
        removedAnyInput = true;
        continue;
      }

      // use input.getInput() for quick checking
      if (!validateInputAttemptForPipelinedShuffle(input.getInput())) {
        inputIter.remove();   // no need to fetch for input, so remove
        removedAnyInput = true;
      }
    }

    return removedAnyInput;
  }

  public void obsoleteKnownInput(InputAttemptIdentifier srcAttempt) {
    // The incoming srcAttempt does not contain a path component.
    LOG.info("{}/{}: Adding obsolete input: {}", inputContext.getUniqueIdentifier(), srcNameTrimmed, srcAttempt);

    // Even if we remove ShuffleEventInfo from shuffleInfoEventsMap[] (see below),
    // new Fetchers may be created from obsolete input again.
    // Hence, add srcAttempt to obsoleteInputs[].
    obsoletedInputs.add(srcAttempt);
  }

  public void informAM(String header, CompositeInputAttemptIdentifier srcAttemptIdentifier) {
    LOG.warn("{} {}: Reporting fetch failure for InputIdentifier: {}, {}", header,
        shuffleClientId, srcAttemptIdentifier,
        TezRuntimeUtils.getTaskAttemptIdentifier(inputContext.getSourceVertexName(),
            srcAttemptIdentifier.getInputIdentifier(), srcAttemptIdentifier.getAttemptNumber()));

    // we send InputReadError regardless of connectFailed (Cf. gla2019.6.10.pptx, page 21)
    InputReadErrorEvent readError = InputReadErrorEvent.create(
        "Fetch failure while fetching from " + header + ": "
          + inputContext.getUniqueIdentifier() + "/" + inputContext.getSourceVertexName(),
        srcAttemptIdentifier.getInputIdentifier(), srcAttemptIdentifier.getAttemptNumber());

    List<Event> failedEvents = Lists.newArrayListWithCapacity(1);
    failedEvents.add(readError);
    inputContext.sendEvents(failedEvents);
  }

  // thread-safe because InputAttemptIdentifier is immutable
  protected boolean isObsoleteInputAttemptIdentifier(CompositeInputAttemptIdentifier srcAttemptIdentifier) {
    if (srcAttemptIdentifier == null || obsoletedInputs.isEmpty()) {
      return false;
    }
    Iterator<InputAttemptIdentifier> obsoleteInputsIter = obsoletedInputs.iterator();
    while (obsoleteInputsIter.hasNext()) {
      InputAttemptIdentifier obsoleteInput = obsoleteInputsIter.next();
      if (srcAttemptIdentifier.include(obsoleteInput.getInputIdentifier(), obsoleteInput.getAttemptNumber())) {
        return true;
      }
    }
    return false;
  }

  public Long getShuffleClientId() {
    return shuffleClientId;
  }

  public int getDagIdentifier() {
    return inputContext.getDagIdentifier();
  }

  public void fetcherStarted() {
    synchronized (lock) {
      numFetchers += 1;
    }
  }

  public void fetcherFinished() {
    shuffleNumFetchersCounter.increment(1);
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
  // if false, no need to consider this ShuffleClient for now
  // called only from ShuffleServer.call() thread
  public boolean shouldScanPendingInputs() {
    synchronized (lock) {
      return numPartitionRanges > 0 && numFetchers < maxNumFetchers;
    }
  }

  // return value of checkCommitRegister()
  static public class CommitRegister {
    // Invariants:
    //   !(!isPipelined) || commitAndRegister
    //   !(isPipelined && commitAndRegister) || !killInPipelined
    //   !(isPipelined && killInPipelined) || !commitAndRegister
    public final boolean isPipelined;
    public final boolean commitAndRegister;
    public final boolean killInPipelined;   // == killBecauseDifferentSpillAttemptInPipelined
    public CommitRegister(
        boolean isPipelined,
        boolean commitAndRegister,
        boolean killInPipelined) {
      this.isPipelined = isPipelined;
      this.commitAndRegister = commitAndRegister;
      this.killInPipelined = killInPipelined;
    }
  }

  // Invariant: this method is part of a multi-step transaction and must run under caller-side lock.
  // The result of checkCommitRegister() is valid only while that caller lock is held.
  protected CommitRegister checkCommitRegister(InputAttemptIdentifier srcAttemptIdentifier) {
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    // assert !isInputFinished(inputIdentifier);

    // non-pipelined: MapOutput output is the entire data, so commit
    // pipelined: check if the spill is new and should be committed
    boolean isPipelined = srcAttemptIdentifier.canRetrieveInputInChunks();
    boolean commitAndRegister;
    boolean killBecauseDifferentSpillAttemptInPipelined = false;
    if (!isPipelined) {
      commitAndRegister = true;
    } else {
      ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);
      if (eventInfo == null) {  // this is the first spill fetched successfully
        commitAndRegister = true;
      } else {
        int attemptNum = srcAttemptIdentifier.getAttemptNumber();
        if (attemptNum == eventInfo.attemptNum) {
          boolean isAlreadyProcessed = eventInfo.getEventsProcessed().get(srcAttemptIdentifier.getSpillEventId());
          commitAndRegister = !isAlreadyProcessed;
        } else {
          commitAndRegister = false;
          killBecauseDifferentSpillAttemptInPipelined = true;
        }
      }
    }

    return new CommitRegister(isPipelined, commitAndRegister, killBecauseDifferentSpillAttemptInPipelined);
  }

  // Invariant: called while caller-side lock for the input is held.
  // Invariant: input.canRetrieveInputInChunks() == true
  protected boolean validateInputAttemptForPipelinedShuffleCommon(
      InputAttemptIdentifier input) {
    int inputIdentifier = input.getInputIdentifier();
    ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);

    if (eventInfo != null && input.getAttemptNumber() != eventInfo.attemptNum) {
      IOException exception = new IOException(
        "Unordered: Previous attempt's data could have been already merged to memory/disk outputs: " + input
          + ", currentAttemptNum=" + eventInfo.attemptNum
          + ", eventsProcessed=" + eventInfo.getEventsProcessed()
          + ", newAttemptNum=" + input.getAttemptNumber());
      String message = "Killing self as previous attempt data could have been consumed";
      killSelf(exception, message);
      return false;
    }

    return true;
  }

  // process counters for completed and commit fetches only
  protected void updateCounters(
      InputAttemptIdentifier srcAttemptIdentifier,
      long bytesCompressed,
      long bytesDecompressed,
      long copyDuration,
      String outputType, boolean isOutputDisk, boolean isOutputDiskDirect) {
    fetchStatsLogger.logIndividualFetchComplete(copyDuration, bytesCompressed, bytesDecompressed,
      outputType, srcAttemptIdentifier);

    shuffleNumInputsCounter.increment(1);
    shuffleBytesCounter.increment(bytesCompressed);
    shuffleBytesDecompressedCounter.increment(bytesDecompressed);
    if (isOutputDisk) {
      shuffleBytesDiskCounter.increment(bytesCompressed);
    } else if (isOutputDiskDirect) {
      shuffleBytesDiskDirectCounter.increment(bytesCompressed);
    } else {
      shuffleBytesMemoryCounter.increment(bytesCompressed);
    }
  }

  protected abstract void killSelf(Exception exception, String message);

  public abstract void fetchSucceeded(
      InputAttemptIdentifier srcAttemptIdentifier,
      T fetchedInput,
      long fetchedBytes, long decompressedLength, long copyDuration) throws IOException;

  public abstract void fetchFailed(
      CompositeInputAttemptIdentifier srcAttemptIdentifier,
      boolean readFailed, boolean connectFailed);

  protected abstract boolean validateInputAttemptForPipelinedShuffle(InputAttemptIdentifier input);

  public FetcherConfig getFetcherConfig() {
    return inputContext.getFetcherConfig(this.conf);
  }
}
