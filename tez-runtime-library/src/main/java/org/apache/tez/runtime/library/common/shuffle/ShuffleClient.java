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

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.Preconditions;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ShuffleClient<T extends ShuffleInput> {

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

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleClient.class);

  // not thread-safe - accessed from:
  //   1. ShuffleServer.call() thread
  //   2. ShuffleInputEventHandler thread
  //   3. Fetcher thread via fetchSucceeded()
  protected final BitSet completedInputSet;
  protected final int numInputs;

  protected final InputContext inputContext;
  protected final String srcNameTrimmed;
  private final String logIdentifier;

  protected final ShuffleServer shuffleServer;
  protected final long shuffleClientId;

  // thread-safe for reading and updating
  // InputAttemptIdentifier is immutable
  private final Set<InputAttemptIdentifier> obsoletedInputs;

  protected final int maxNumFetchers;

  // to track shuffleInfo events when finalMerge is disabled in source or pipelined shuffle is enabled in source
  // Invariant: guard with this.synchronized in ShuffleScheduler
  //            guard with: synchronized (shuffleInfoEventsMap) in ShuffleManager
  protected final Map<Integer, ShuffleEventInfo> shuffleInfoEventsMap;

  private int numFetchers = 0;
  private int numPartitionRanges = 0;
  private final Object lock = new Object();

  public ShuffleClient(
      InputContext inputContext,
      Configuration conf,
      int numInputs,
      String srcNameTrimmed) throws IOException {
    this.shuffleServer = (ShuffleServer)inputContext.getShuffleServer();
    this.shuffleClientId = shuffleServer.register(this);
    this.inputContext = inputContext;
    this.srcNameTrimmed = srcNameTrimmed;
    this.logIdentifier = inputContext.getUniqueIdentifier() + "-" + srcNameTrimmed;

    this.numInputs = numInputs;
    this.completedInputSet = new BitSet(numInputs);

    this.obsoletedInputs = Collections.newSetFromMap(new ConcurrentHashMap<InputAttemptIdentifier, Boolean>());

    this.maxNumFetchers = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES_DEFAULT);

    this.shuffleInfoEventsMap = new HashMap<Integer, ShuffleEventInfo>();
  }

  public String getLogIdentifier() {
    return logIdentifier;
  }

  // inside ShuffleServer.call() thread
  protected boolean cleanInputHostForConstructFetcher(InputHost.PartitionToInputs pendingInputs) {
    // safe to update pendingInputs because we are running in ShuffleServer.call() thread
    assert pendingInputs.getShuffleClientId() == shuffleClientId;
    assert pendingInputs.getInputs().size() <= shuffleServer.getMaxTaskOutputAtOnce();

    boolean removedAnyInput = false;

    // avoid adding attempts which have already been completed
    // guard with synchronized because completedInputSet should not be updated while traversing
    synchronized (completedInputSet) {
      for (Iterator<InputAttemptIdentifier> inputIter = pendingInputs.getInputs().iterator();
           inputIter.hasNext();) {
        InputAttemptIdentifier input = inputIter.next();

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

  public void obsoleteKnownInput(InputAttemptIdentifier srcAttempt) {
    // The incoming srcAttempt does not contain a path component.
    LOG.info("{}: Adding obsolete input: {}", srcNameTrimmed, srcAttempt);

    // Even if we remove ShuffleEventInfo from shuffleInfoEventsMap[] (see below),
    // new Fetchers may be created from obsolete input again.
    // Hence, add srcAttempt to obsoleteInputs[].
    obsoletedInputs.add(srcAttempt);
  }

  // thread-safe because InputAttemptIdentifier is immutable
  protected boolean isObsoleteInputAttemptIdentifier(InputAttemptIdentifier input) {
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

  public long getShuffleClientId() {
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

  public abstract void fetchSucceeded(
      InputAttemptIdentifier srcAttemptIdentifier,
      T fetchedInput,
      long fetchedBytes, long decompressedLength, long copyDuration) throws IOException;

  public abstract void fetchFailed(
      InputAttemptIdentifier srcAttemptIdentifier,
      boolean readFailed, boolean connectFailed);

  protected abstract boolean validateInputAttemptForPipelinedShuffle(
      InputAttemptIdentifier input, boolean registerShuffleInfoEvent);
}
