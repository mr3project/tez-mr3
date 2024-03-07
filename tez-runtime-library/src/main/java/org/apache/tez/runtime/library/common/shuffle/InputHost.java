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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.impl.ShuffleManager;

/**
 * Represents a Host with respect to the MapReduce ShuffleHandler.
 * 
 */
public class InputHost extends HostPort {

  private static class PartitionRange {

    private final int partitionId;
    private final int partitionCount;

    PartitionRange(int partitionId, int partitionCount) {
      this.partitionId = partitionId;
      this.partitionCount = partitionCount;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PartitionRange that = (PartitionRange) o;

      if (partitionId != that.partitionId) return false;
      return partitionCount == that.partitionCount;
    }

    @Override
    public int hashCode() {
      int result = partitionId;
      result = 31 * result + partitionCount;
      return result;
    }

    public int getPartition() {
      return partitionId;
    }

    public int getPartitionCount() {
      return partitionCount;
    }
  }

  // Long = shuffleManagerId
  // Invariant:
  //   1. partitionToInputs[shuffleManagerId] is never empty
  //   2. partitionToInputs[shuffleMangerId][partitionRange] is never empty
  // no need to use concurrent Map/Queue because we guard all access with synchronzied{}
  private final Map<Long, Map<PartitionRange, List<InputAttemptIdentifier>>>
      partitionToInputs = new HashMap<>();

  private boolean hasPendingInput;

  public InputHost(HostPort hostPort) {
    super(hostPort.getHost(), hostPort.getPort());
    this.hasPendingInput = false;
  }

  public synchronized InputHost takeFromPendingHosts(
      BlockingQueue<InputHost> pendingHosts) throws InterruptedException {
    assert hasPendingInput;
    InputHost inputHost = pendingHosts.take();
    assert inputHost == this;
    hasPendingInput = false;
    return inputHost;
  }

  public synchronized void addToPendingHostsIfNecessary(BlockingQueue<InputHost> pendingHosts) {
    // 'assert !hasPendingInput' is invalid because addKnownInput() may have been called
    if (!hasPendingInput && !partitionToInputs.isEmpty()) {
      pendingHosts.add(this);
      hasPendingInput = true;
    }
  }

  public synchronized void addKnownInput(
      ShuffleManager shuffleManager, int partitionId, int partitionCount, InputAttemptIdentifier srcAttempt,
      BlockingQueue<InputHost> pendingHosts) {
    long shuffleManagerId = shuffleManager.getShuffleManagerId();
    Map<PartitionRange, List<InputAttemptIdentifier>> partitionMap = partitionToInputs.get(shuffleManagerId);
    if (partitionMap == null) {
      partitionMap = new HashMap<PartitionRange, List<InputAttemptIdentifier>>();
      partitionToInputs.put(shuffleManagerId, partitionMap);
    }

    PartitionRange partitionRange = new PartitionRange(partitionId, partitionCount);
    List<InputAttemptIdentifier> inputs = partitionMap.get(partitionRange);
    if (inputs == null) {
      inputs = new ArrayList<InputAttemptIdentifier>();
      partitionMap.put(partitionRange, inputs);
      shuffleManager.partitionRangeAdded();
    }

    inputs.add(srcAttempt);

    if (!hasPendingInput) {
      boolean added = pendingHosts.offer(this);
      if (!added) {
        String errorMessage = "Unable to add host " + super.toString() + " to pending queue";
        throw new TezUncheckedException(errorMessage);
      }
      hasPendingInput = true;
    }
  }

  public synchronized PartitionToInputs clearAndGetOnePartitionRange(
      ConcurrentMap<Long, ShuffleManager> shuffleManagers) {
    if (partitionToInputs.isEmpty()) {
      // this can happen if:
      //   1. ShuffleManagerServer takes InputHost from pendingHosts[]
      //   2. pendingHosts[] does not contain InputHost
      //   3. addKnownInput() is called before constructFetcherForHost() is called,
      //      and add InputHost to pendingHosts[]
      //   4. InputHost.partitionToInputs[] is all consumed and becomes empty
      return null;
    }

    Iterator<Map.Entry<Long, Map<PartitionRange, List<InputAttemptIdentifier>>>> iterator =
        partitionToInputs.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, Map<PartitionRange, List<InputAttemptIdentifier>>> entry = iterator.next();
      long shuffleManagerId = entry.getKey();
      Map<PartitionRange, List<InputAttemptIdentifier>> partitionMap = entry.getValue();
      assert !partitionMap.isEmpty();   // invariant on partitionToInputs[]

      ShuffleManager shuffleManager = shuffleManagers.get(shuffleManagerId);
      if (shuffleManager == null) {
        iterator.remove();
        continue;
      }
      if (!shuffleManager.shouldScanPendingInputs()) {
        continue;
      }

      Map.Entry<PartitionRange, List<InputAttemptIdentifier>> maxEntry = null;
      int maxIdentifierCount = Integer.MIN_VALUE;
      for (Map.Entry<PartitionRange, List<InputAttemptIdentifier>> e : partitionMap.entrySet()) {
        int currentSize = e.getValue().size();
        assert currentSize > 0;
        if (currentSize > maxIdentifierCount) {
          maxIdentifierCount = currentSize;
          maxEntry = e;
        }
      }
      assert maxEntry != null;

      PartitionRange range = maxEntry.getKey();
      List<InputAttemptIdentifier> queue = maxEntry.getValue();
      PartitionToInputs ret = new PartitionToInputs(shuffleManagerId, range.getPartition(), range.getPartitionCount(), queue);
      partitionMap.remove(range);
      shuffleManager.partitionRangeRemoved();

      if (partitionMap.isEmpty()) {
        iterator.remove();
      }

      return ret;
    }

    return null;
  }

  // return true if some entries have been removed
  public synchronized boolean clearShuffleManagerId(long shuffleManagerId) {
    Map<PartitionRange, List<InputAttemptIdentifier>> current = partitionToInputs.remove(shuffleManagerId);
    return current != null;
  }

  public synchronized String toDetailedString() {
    return "HostPort=" + super.toString() + ", partitionToInputs=" +
        partitionToInputs.keySet().stream().map(x -> x.toString())
            .collect(Collectors.joining(", "));
  }
  
  @Override
  public String toString() {
    return "InputHost " + super.toString();
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object to) {
    return super.equals(to);
  }

  public static class PartitionToInputs {
    private final long shuffleManagerId;
    private final int partition;
    private final int partitionCount;
    private List<InputAttemptIdentifier> inputs;  // can be removed from after initializing

    public PartitionToInputs(long shuffleManagerId, int partition, int partitionCount, List<InputAttemptIdentifier> input) {
      this.shuffleManagerId = shuffleManagerId;
      this.partition = partition;
      this.partitionCount = partitionCount;
      this.inputs = input;
    }

    public long getShuffleManagerId() {
      return shuffleManagerId;
    }

    public int getPartition() {
      return partition;
    }

    public int getPartitionCount() {
      return partitionCount;
    }

    public List<InputAttemptIdentifier> getInputs() {
      return inputs;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("shuffleManagerId=");
      sb.append(shuffleManagerId);
      sb.append(", partition=");
      sb.append(partition);
      sb.append(", partitionCount=");
      sb.append(partitionCount);
      sb.append(", input=");
      sb.append(inputs);
      return sb.toString();
    }
  }
}
