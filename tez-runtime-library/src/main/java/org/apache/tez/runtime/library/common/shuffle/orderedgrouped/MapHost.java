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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.InputHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
class MapHost {
  private static final Logger LOG = LoggerFactory.getLogger(InputHost.class);

  public static enum State {
    IDLE,               // No map outputs available
    BUSY,               // Map outputs are being fetched
    PENDING,            // Known map outputs which need to be fetched
    PENALIZED           // Host penalized due to shuffle failures
  }

  public static class HostPortPartition {

    final String host;
    final int port;
    final int partition;

    HostPortPartition(String host, int port, int partition) {
      this.host = host;
      this.port = port;
      this.partition = partition;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((host == null) ? 0 : host.hashCode());
      result = prime * result + partition;
      result = prime * result + port;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      HostPortPartition other = (HostPortPartition) obj;
      if (partition != other.partition)
        return false;
      if (host == null) {
        if (other.host != null)
          return false;
      } else if (!host.equals(other.host))
        return false;
      if (port != other.port)
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "HostPortPartition [host=" + host + ", port=" + port + ", partition=" + partition + "]";
    }
  }

  private State state = State.IDLE;
  private final String host;
  private final int port;
  private final int partitionId;
  private final int partitionCount;
  // Tracks attempt IDs
  private List<InputAttemptIdentifier> maps = new ArrayList<InputAttemptIdentifier>();

  private final boolean readPartitionAllOnce;
  private final int srcVertexNumTasks;
  private final List<CompositeInputAttemptIdentifier> tempMaps;

  private final int shuffleId;    // TODO: remove

  public MapHost(String host, int port, int partitionId, int partitionCount,
      boolean readPartitionAllOnce, int srcVertexNumTasks, int shuffleId) {
    this.host = host;
    this.port = port;
    this.partitionId = partitionId;
    this.partitionCount = partitionCount;

    this.readPartitionAllOnce = readPartitionAllOnce;
    this.srcVertexNumTasks = srcVertexNumTasks;
    this.shuffleId = shuffleId;
    if (readPartitionAllOnce) {
      assert partitionCount == 1;
      tempMaps = new ArrayList<CompositeInputAttemptIdentifier>();
    } else {
      tempMaps = null;
    }
  }

  public int getPartitionId() {
    return partitionId;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public State getState() {
    return state;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getHostIdentifier() {
    return host + ":" + port;
  }

  public synchronized void addKnownMap(InputAttemptIdentifier srcAttempt) {
    if (readPartitionAllOnce) {   // for InputSpec, readPartitionAllOnce == true implies useRssShuffle == true
      tempMaps.removeIf(input -> {
        if (input.getInputIdentifier() == srcAttempt.getInputIdentifier()) {
          LOG.warn("DEBUG: MapHost should remove InputAttemptIdentifier with the same partitionId and a different attemptNumber: {}, {}, {} != {}",
              partitionId, input.getInputIdentifier(), input.getAttemptNumber(), srcAttempt.getAttemptNumber());
          return false;
        } else {
          return false;
        }
      });
      tempMaps.add((CompositeInputAttemptIdentifier) srcAttempt);
    } else {  // either we do not use RSS or we have readPartitionAllOnce == false
      maps.add(srcAttempt);
    }

    if (readPartitionAllOnce) {
      // TODO: use checkForDuplicateWithDifferentAttemptNumbers when using VertexRerun

      if (tempMaps.size() == srcVertexNumTasks) {
        long partitionTotalSize = 0L;
        for (InputAttemptIdentifier input: tempMaps) {
          CompositeInputAttemptIdentifier cid = (CompositeInputAttemptIdentifier)input;
          partitionTotalSize += cid.getPartitionSize(partitionId);
        }
        // optimize partitionSizes[] because only partitionId is used and other fields are never used
        long[] partitionSizes = new long[1];
        partitionSizes[0] = partitionTotalSize;

        CompositeInputAttemptIdentifier firstId = tempMaps.get(0);
        CompositeInputAttemptIdentifier mergedCid = new CompositeInputAttemptIdentifier(
            firstId.getInputIdentifier(),
            firstId.getAttemptNumber(),
            firstId.getPathComponent(),
            firstId.isShared(),
            firstId.getFetchTypeInfo(),
            firstId.getSpillEventId(),
            1, partitionSizes, -1);
        mergedCid.setInputIdentifiersForReadPartitionAllOnce(tempMaps);

        LOG.info("Ordered MapHost - merged Ordered_shuffleId_taskIndex_attemptNumber={}_{}_{}_{} = {}",
            shuffleId,
            firstId.getTaskIndex(),
            firstId.getAttemptNumber(), partitionId, partitionTotalSize);

        LOG.info("Ordered - Merging {} partition inputs for partitionId={} with total size {}: {} ",
            srcVertexNumTasks, partitionId, partitionTotalSize, mergedCid);

        maps.add(mergedCid);
      }
    }

    if (state == State.IDLE && maps.size() > 0) {
      state = State.PENDING;
    }
  }

  public synchronized List<InputAttemptIdentifier> getAndClearKnownMaps() {
    List<InputAttemptIdentifier> currentKnownMaps = maps;
    maps = new ArrayList<InputAttemptIdentifier>();
    return currentKnownMaps;
  }
  
  public synchronized void markBusy() {
    state = State.BUSY;
  }
  
  public synchronized void markPenalized() {
    state = State.PENALIZED;
  }
  
  public synchronized int getNumKnownMapOutputs() {
    return maps.size();
  }

  /**
   * Called when the node is done with its penalty or done copying.
   * @return the host's new state
   */
  public synchronized State markAvailable() {
    if (maps.isEmpty()) {
      state = State.IDLE;
    } else {
      state = State.PENDING;
    }
    return state;
  }
  
  @Override
  public String toString() {
    return getHostIdentifier();
  }
  
  /**
   * Mark the host as penalized
   */
  public synchronized void penalize() {
    state = State.PENALIZED;
  }
}
