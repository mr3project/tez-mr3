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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.tez.runtime.library.common.shuffle.ShuffleServer.RangesScheme;

/**
 * Represents a Host with respect to the MapReduce ShuffleHandler.
 * 
 */
public class InputHost {

  private static final Logger LOG = LoggerFactory.getLogger(InputHost.class);

  public static class PartitionRange {

    private final int partitionId;
    private final int partitionCount;

    public PartitionRange(int partitionId, int partitionCount) {
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

    @Override
    public String toString() {
      if (partitionCount == 1) {
        return String.valueOf(partitionId);
      } else {
        return partitionId + "-" + (partitionId + partitionCount - 1);
      }
    }
  }

  // Long = shuffleClientId
  // Invariant:
  //   1. partitionToInputs[shuffleClientId] is never empty
  //   2. partitionToInputs[shuffleClientId][partitionRange] is never empty
  // no need to use concurrent Map/Queue because we guard all access with synchronized{}
  private final Map<Long, Map<PartitionRange, List<CompositeInputAttemptIdentifier>>> partitionToInputs = new HashMap<>();

  private boolean hasPendingInput;

  // use synchronized (blockingFetchers)
  private final Set<Fetcher<?>> blockingFetchers;
  private long blockStartMillis;

  // shared by all InputHosts
  private final static AtomicInteger numHostBlocked = new AtomicInteger(0);

  private final HostPort hostPort;

  public InputHost(HostPort hostPort) {
    this.hasPendingInput = false;
    this.blockingFetchers = new HashSet<Fetcher<?>>();
    this.hostPort = hostPort;
  }

  public HostPort getHostPort() {
    return hostPort;
  }

  public void addHostBlocked(Fetcher<?> fetcher) {
    synchronized (blockingFetchers) {
      boolean wasEmpty = blockingFetchers.isEmpty();
      boolean result = blockingFetchers.add(fetcher);
      assert result;  // can be called only once per Fetcher

      if (wasEmpty) {
        int newNum = numHostBlocked.incrementAndGet();
        blockStartMillis = System.currentTimeMillis();
        LOG.warn("Host blocked: {}, numHostBlocked={}", this, newNum);
      }
    }
  }

  public void removeHostBlocked(Fetcher<?> fetcher) {
    // fetcher might be removed more than once because removeHostBlocked() can be called from different threads
    synchronized (blockingFetchers) {
      boolean wasEmpty = blockingFetchers.isEmpty();
      if (!wasEmpty) {
        blockingFetchers.remove(fetcher);  // fetcher may or may not be found in hostBlocked[]
        boolean isEmpty = blockingFetchers.isEmpty();
        if (isEmpty) {
          int newNum = numHostBlocked.decrementAndGet();
          LOG.info("Host unblocked: {}, numHostBlocked={}, duration={}",
              this, newNum, System.currentTimeMillis() - blockStartMillis);
        }
      }
    }
  }

  public boolean isHostNormal() {
    synchronized (blockingFetchers) {
      return blockingFetchers.isEmpty();
    }
  }

  // should be consistent with clearAndGetOnePartitionRange()
  public synchronized boolean hasFetcherToLaunch(ConcurrentMap<Long, ShuffleClient<?>> shuffleClients) {
    assert hasPendingInput;   // because we remove from pendingHosts[] only later in ShuffleServer.call()
    return
      !partitionToInputs.isEmpty() &&
      partitionToInputs.keySet().stream().anyMatch(id -> {
          ShuffleClient<?> shuffleClient = shuffleClients.get(id);
          return shuffleClient != null && shuffleClient.shouldScanPendingInputs();
      });
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

  // partitionId == output partition in DME (DataMovementEvent.sourceIndex)
  // partitionId != srcAttempt.inputIdentifier
  public synchronized void addKnownInput(
      ShuffleClient<?> shuffleClient,
      int partitionId, int partitionCount, CompositeInputAttemptIdentifier srcAttempt,
      BlockingQueue<InputHost> pendingHosts, boolean checkForDuplicate) {
    Long shuffleClientId = shuffleClient.getShuffleClientId();
    Map<PartitionRange, List<CompositeInputAttemptIdentifier>> partitionMap = partitionToInputs.get(shuffleClientId);
    if (partitionMap == null) {
      partitionMap = new HashMap<PartitionRange, List<CompositeInputAttemptIdentifier>>();
      partitionToInputs.put(shuffleClientId, partitionMap);
    }

    PartitionRange partitionRange = new PartitionRange(partitionId, partitionCount);
    List<CompositeInputAttemptIdentifier> inputs = partitionMap.get(partitionRange);
    if (inputs == null) {
      inputs = new ArrayList<CompositeInputAttemptIdentifier>();
      partitionMap.put(partitionRange, inputs);
      shuffleClient.partitionRangeAdded();
    }

    if (checkForDuplicate) {
      if (inputs.contains(srcAttempt)) {
        // This can happen if multiple Fetchers are created from the same IAI and then some of them
        // return IAI in pendingInputs[] (e.g., mapOutput.getType() == Type.WAIT in FetcherOrderedGrouped)
        LOG.warn("ShuffleClient {} / PartitionMap {} already contains {}, so skip adding",
            shuffleClientId, partitionRange, srcAttempt);
      } else {
        LOG.info("ShuffleClient {} / PartitionMap {} adds as pending input: {}",
            shuffleClientId, partitionRange, srcAttempt);
        inputs.add(srcAttempt);
      }
    } else {
      inputs.add(srcAttempt);
    }

    if (!hasPendingInput) {
      boolean added = pendingHosts.offer(this);
      if (!added) {
        String errorMessage = "Unable to add host " + hostPort.toString() + " to pending queue";
        throw new TezUncheckedException(errorMessage);
      }
      hasPendingInput = true;
    }
  }

  public synchronized PartitionToInputs clearAndGetOnePartitionRange(
      ConcurrentMap<Long, ShuffleClient<?>> shuffleClients,
      int maxTaskOutputAtOnce,
      ShuffleServer.RangesScheme rangesScheme) {
    if (partitionToInputs.isEmpty()) {
      // this can happen if:
      //   1. ShuffleServer takes InputHost from pendingHosts[]
      //   2. pendingHosts[] does not contain InputHost
      //   3. addKnownInput() is called before constructFetcherForHost() is called,
      //      and add InputHost to pendingHosts[]
      //   4. InputHost.partitionToInputs[] is all consumed and becomes empty
      return null;
    }

    ShuffleClient<?> shuffleClient = null;
    if (rangesScheme == RangesScheme.SCHEME_FIRST) {
      shuffleClient = getFirstShuffleClient(shuffleClients);
    } else {
      shuffleClient = getMaxSizeShuffleClient(shuffleClients);
    }
    if (shuffleClient == null) {
      return null;
    }
    Long shuffleClientId = shuffleClient.getShuffleClientId();

    Map<PartitionRange, List<CompositeInputAttemptIdentifier>> partitionMap = partitionToInputs.get(shuffleClientId);
    assert !partitionMap.isEmpty();   // invariant on partitionToInputs[]

    Map.Entry<PartitionRange, List<CompositeInputAttemptIdentifier>> maxSizeEntry = getMaxSizeEntry(partitionMap);
    assert maxSizeEntry != null;

    // extract PartitionToInputs from maxSizeEntry, updating partitionMap[] and notifying shuffleClient
    PartitionRange range = maxSizeEntry.getKey();
    List<CompositeInputAttemptIdentifier> queue = maxSizeEntry.getValue();
    PartitionToInputs ret;
    if (queue.size() <= maxTaskOutputAtOnce) {
      ret = new PartitionToInputs(shuffleClientId, range, queue);
      partitionMap.remove(range);
      shuffleClient.partitionRangeRemoved();
    } else {
      List<CompositeInputAttemptIdentifier> inputToConsume = new ArrayList<>(queue.subList(0, maxTaskOutputAtOnce));
      queue.subList(0, maxTaskOutputAtOnce).clear();
      ret = new PartitionToInputs(shuffleClientId, range, inputToConsume);
    }

    if (partitionMap.isEmpty()) {
      partitionToInputs.remove(shuffleClientId);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("InputHost returns for the next fetcher: {}", ret);
    }
    return ret;
  }

  private ShuffleClient getFirstShuffleClient(
      ConcurrentMap<Long, ShuffleClient<?>> shuffleClients) {
    Iterator<Map.Entry<Long, Map<PartitionRange, List<CompositeInputAttemptIdentifier>>>> iterator =
        partitionToInputs.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, Map<PartitionRange, List<CompositeInputAttemptIdentifier>>> entry = iterator.next();
      Long shuffleClientId = entry.getKey();
      Map<PartitionRange, List<CompositeInputAttemptIdentifier>> partitionMap = entry.getValue();
      assert !partitionMap.isEmpty();   // invariant on partitionToInputs[]

      ShuffleClient<?> shuffleClient = shuffleClients.get(shuffleClientId);
      if (shuffleClient == null) {
        iterator.remove();
        continue;
      }
      if (!shuffleClient.shouldScanPendingInputs()) {
        continue;
      }

      return shuffleClient;
    }

    return null;
  }

  private ShuffleClient getMaxSizeShuffleClient(
      ConcurrentMap<Long, ShuffleClient<?>> shuffleClients) {
    int maxCount = Integer.MIN_VALUE;
    ShuffleClient maxShuffleClient = null;

    Iterator<Map.Entry<Long, Map<PartitionRange, List<CompositeInputAttemptIdentifier>>>> iterator =
        partitionToInputs.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, Map<PartitionRange, List<CompositeInputAttemptIdentifier>>> entry = iterator.next();
      Long shuffleClientId = entry.getKey();
      Map<PartitionRange, List<CompositeInputAttemptIdentifier>> partitionMap = entry.getValue();
      assert !partitionMap.isEmpty();   // invariant on partitionToInputs[]

      ShuffleClient<?> shuffleClient = shuffleClients.get(shuffleClientId);
      if (shuffleClient == null) {
        iterator.remove();
        continue;
      }
      if (!shuffleClient.shouldScanPendingInputs()) {
        continue;
      }

      int currentSize = partitionMap.values().stream()
          .mapToInt(x -> x.size())
          .sum();
      if (currentSize > maxCount) {
        maxCount = currentSize;
        maxShuffleClient = shuffleClient;
      }
    }

    return maxShuffleClient;
  }

  private Map.Entry<PartitionRange, List<CompositeInputAttemptIdentifier>> getMaxSizeEntry(
      Map<PartitionRange, List<CompositeInputAttemptIdentifier>> partitionMap) {
    Map.Entry<PartitionRange, List<CompositeInputAttemptIdentifier>> maxEntry = null;

    int maxIdentifierCount = Integer.MIN_VALUE;
    for (Map.Entry<PartitionRange, List<CompositeInputAttemptIdentifier>> e : partitionMap.entrySet()) {
      int currentSize = e.getValue().size();
      assert currentSize > 0;
      if (currentSize > maxIdentifierCount) {
        maxIdentifierCount = currentSize;
        maxEntry = e;
      }
    }

    return maxEntry;
  }

  public synchronized void clearShuffleClientId(Long shuffleClientId) {
    Map<PartitionRange, List<CompositeInputAttemptIdentifier>> partitionMap = partitionToInputs.remove(shuffleClientId);
    if (LOG.isDebugEnabled()) {
      if (partitionMap != null) {
        String logString = partitionMap.entrySet().stream()
          .map(entry -> entry.getKey().toString() + " -> " + entry.getValue().size())
          .collect(Collectors.joining(", ", "PartitionMap: {", "}"));
        LOG.warn("{} still contains input for ShuffleClient {}: {}", this, shuffleClientId, logString);
      }
    }
  }

  public synchronized boolean containsInput(
      Long shuffleClientId, PartitionRange partitionRange, CompositeInputAttemptIdentifier srcAttemptIdentifier) {
    Map<PartitionRange, List<CompositeInputAttemptIdentifier>> partitionMap = partitionToInputs.get(shuffleClientId);
    if (partitionMap != null) {
      List<CompositeInputAttemptIdentifier> inputs = partitionMap.get(partitionRange);
      if (inputs != null) {
        return inputs.contains(srcAttemptIdentifier);
      }
    }
    return false;
  }

  public synchronized String toDetailedString() {
    return "HostPort=" + hostPort.toString() + ", partitionToInputs=" +
        partitionToInputs.keySet().stream().map(x -> x.toString()).collect(Collectors.joining(", "));
  }
  
  @Override
  public String toString() {
    return "InputHost " + hostPort.toString();
  }

  public static class PartitionToInputs {
    private final Long shuffleClientId;
    private final PartitionRange partitionRange;
    private final List<CompositeInputAttemptIdentifier> inputs;  // can be removed from after initializing

    public PartitionToInputs(Long shuffleClientId, PartitionRange partitionRange, List<CompositeInputAttemptIdentifier> inputs) {
      this.shuffleClientId = shuffleClientId;
      this.partitionRange = partitionRange;
      this.inputs = inputs;
    }

    public Long getShuffleClientId() {
      return shuffleClientId;
    }

    public PartitionRange getPartitionRange() {
      return partitionRange;
    }

    public int getPartition() {
      return partitionRange.partitionId;
    }

    public int getPartitionCount() {
      return partitionRange.partitionCount;
    }

    public List<CompositeInputAttemptIdentifier> getInputs() {
      return inputs;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("shuffleClientId=");
      sb.append(shuffleClientId);
      sb.append(", partitionId=");
      sb.append(partitionRange.partitionId);
      sb.append(", partitionCount=");
      sb.append(partitionRange.partitionCount);
      sb.append(", input[]=");
      sb.append(inputs.size());
      return sb.toString();
    }
  }
}
