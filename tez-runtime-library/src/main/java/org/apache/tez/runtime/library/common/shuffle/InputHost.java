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
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a Host with respect to the MapReduce ShuffleHandler.
 * 
 */
public class InputHost extends HostPort {
  private static final Logger LOG = LoggerFactory.getLogger(InputHost.class);

  private static class PartitionRange {

    private final int partition;
    private final int partitionCount;

    PartitionRange(int partition, int partitionCount) {
      this.partition = partition;
      this.partitionCount = partitionCount;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PartitionRange that = (PartitionRange) o;

      if (partition != that.partition) return false;
      return partitionCount == that.partitionCount;
    }

    @Override
    public int hashCode() {
      int result = partition;
      result = 31 * result + partitionCount;
      return result;
    }

    public int getPartition() {
      return partition;
    }

    public int getPartitionCount() {
      return partitionCount;
    }
  }

  private String additionalInfo;

  // Each input host can support more than one partition.
  // Each partition has a list of inputs for pipelined shuffle.
  private final Map<PartitionRange, BlockingQueue<InputAttemptIdentifier>>
      partitionToInputs = new ConcurrentHashMap<>();

  // readPartitionAllOnce == true only if RSS is used
  // no need to use ConcurrentHashMap[] for tempPartitionToInputs because:
  //   1. it is used only when readPartitionAllOnce == true and thus RSS is used
  //   2. when RSS is used, addKnownInput() is called only from ShuffleManager.addKnownInput(), thus
  //      always from the same thread
  //   3. addKnownInput() is guarded with synchronized
  private final boolean readPartitionAllOnce;
  private final int srcVertexNumTasks;
  private final Map<PartitionRange, List<CompositeInputAttemptIdentifier>> tempPartitionToInputs;

  public InputHost(HostPort hostPort, boolean readPartitionAllOnce, int srcVertexNumTasks) {
    super(hostPort.getHost(), hostPort.getPort());
    this.readPartitionAllOnce = readPartitionAllOnce;
    this.srcVertexNumTasks = srcVertexNumTasks;
    if (readPartitionAllOnce) {
      tempPartitionToInputs = new HashMap<PartitionRange, List<CompositeInputAttemptIdentifier>>();
    } else {
      tempPartitionToInputs = null;
    }
  }

  public void setAdditionalInfo(String additionalInfo) {
    this.additionalInfo = additionalInfo;
  }

  public String getAdditionalInfo() {
    return (additionalInfo == null) ? "" : additionalInfo;
  }

  public int getNumPendingPartitions() {
    return partitionToInputs.size();
  }

  public synchronized void addKnownInput(int partitionId, int partitionCount,
      InputAttemptIdentifier srcAttempt) {
    if (readPartitionAllOnce) {
      assert partitionCount == 1;
      addKnownInputForReadPartitionAllOnce(partitionId, (CompositeInputAttemptIdentifier) srcAttempt);
    } else {
      PartitionRange partitionRange = new PartitionRange(partitionId, partitionCount);
      addToPartitionToInputs(partitionRange, srcAttempt);
    }
  }

  private void addKnownInputForReadPartitionAllOnce(int partitionId, CompositeInputAttemptIdentifier srcAttempt) {
    PartitionRange partitionRange = new PartitionRange(partitionId, 1);
    List<CompositeInputAttemptIdentifier> inputs = tempPartitionToInputs.get(partitionRange);
    if (inputs == null) {
      inputs = new ArrayList<CompositeInputAttemptIdentifier>();
      tempPartitionToInputs.put(partitionRange, inputs);
    }
    // The following code checks for duplicate InputAttemptIdentifier's with different attemptNumbers.
    // For now, this is unnecessary because we do not use VertexRerun for RSS.
    boolean checkForDuplicateWithDifferentAttemptNumbers = false;
    if (checkForDuplicateWithDifferentAttemptNumbers) {
      inputs.removeIf(input -> {
        if (input.getInputIdentifier() == srcAttempt.getInputIdentifier()) {
          LOG.warn("Removing InputAttemptIdentifier with the same partitionId and a different attemptNumber: {}, {}, {} != {}",
              partitionId, input.getInputIdentifier(), input.getAttemptNumber(), srcAttempt.getAttemptNumber());
          return true;
        } else {
          return false;
        }
      });
    }
    inputs.add(srcAttempt);

    if (inputs.size() == srcVertexNumTasks) {
      long partitionTotalSize = 0L;
      for (CompositeInputAttemptIdentifier cid: inputs) {
        partitionTotalSize += cid.getPartitionSize(partitionId);
      }
      // optimize partitionSizes[] because only partitionId is used and other fields are never used
      long[] partitionSizes = new long[1];
      partitionSizes[0] = partitionTotalSize;

      // mergedCid is consumed by Fetcher
      InputAttemptIdentifier firstId = inputs.get(0);
      CompositeInputAttemptIdentifier mergedCid = new CompositeInputAttemptIdentifier(
          // one representative InputAttemptIdentifier (i.e., destInputIndexes) for:
          //   1. checking 'alreadyCompleted' in ShuffleManager.constructRssFetcher()
          //   2. for committing FetchedInput in RssFetcher
          firstId.getInputIdentifier(),
          firstId.getAttemptNumber(),
          firstId.getPathComponent(),
          firstId.isShared(),
          firstId.getFetchTypeInfo(),
          firstId.getSpillEventId(),
          1, partitionSizes, -1);
      mergedCid.setInputIdentifiersForReadPartitionAllOnce(inputs);

      LOG.info("Merging {} partition inputs for partitionId={} with total size {}: {} ",
          srcVertexNumTasks, partitionId, partitionTotalSize, mergedCid);

      addToPartitionToInputs(partitionRange, mergedCid);
      tempPartitionToInputs.remove(partitionRange);
    }
  }

  private void addToPartitionToInputs(
      PartitionRange partitionRange, InputAttemptIdentifier srcAttempt) {
    BlockingQueue<InputAttemptIdentifier> inputs = partitionToInputs.get(partitionRange);
    if (inputs == null) {
      inputs = new LinkedBlockingQueue<InputAttemptIdentifier>();
      partitionToInputs.put(partitionRange, inputs);
    }
    inputs.add(srcAttempt);
  }

  public synchronized PartitionToInputs clearAndGetOnePartitionRange() {
    for (Map.Entry<PartitionRange, BlockingQueue<InputAttemptIdentifier>> entry :
        partitionToInputs.entrySet()) {
      List<InputAttemptIdentifier> inputs = new ArrayList<InputAttemptIdentifier>(entry.getValue().size());
      entry.getValue().drainTo(inputs);
      PartitionToInputs ret = new PartitionToInputs(entry.getKey().getPartition(), entry.getKey().getPartitionCount(), inputs);
      partitionToInputs.remove(entry.getKey());
      return ret;
    }
    return null;
  }

  public String toDetailedString() {
    return "HostPort=" + super.toString() + ", InputDetails=" +
        partitionToInputs;
  }
  
  @Override
  public String toString() {
    return "HostPort=" + super.toString() + ", PartitionIds=" +
        partitionToInputs.keySet();
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
    private final int partition;
    private final int partitionCount;
    private List<InputAttemptIdentifier> inputs;

    public PartitionToInputs(int partition, int partitionCount, List<InputAttemptIdentifier> input) {
      this.partition = partition;
      this.partitionCount = partitionCount;
      this.inputs = input;
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
      return "partition=" + partition + ", partitionCount=" + partitionCount + ", inputs=" + inputs;
    }
  }
}
