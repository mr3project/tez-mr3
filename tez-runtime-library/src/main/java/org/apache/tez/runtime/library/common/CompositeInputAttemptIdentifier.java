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

package org.apache.tez.runtime.library.common;

import org.apache.hadoop.classification.InterfaceAudience.Private;

import java.util.List;

/**
 * Container for a task number and an attempt number for the task.
 */
@Private
public class CompositeInputAttemptIdentifier extends InputAttemptIdentifier {
  private final int inputIdentifierCount;

  // both fields are set if shufflePayload.getLastEvent() == true
  // partitionSizes.length == 1 if only a single entry is used
  private final long[] partitionSizes;
  private final int taskIndex;  // -1 if not used

  // only for readPartitionAllOnce, size == srcVertexNumTasks
  private List<InputAttemptIdentifier> childInputIdentifiers;

  public CompositeInputAttemptIdentifier(int inputIdentifier, int attemptNumber, String pathComponent, int inputIdentifierCount) {
    this(inputIdentifier, attemptNumber, pathComponent, false, SPILL_INFO.FINAL_MERGE_ENABLED, -1, inputIdentifierCount);
  }

  public CompositeInputAttemptIdentifier(int inputIdentifier, int attemptNumber, String pathComponent, boolean isShared, int inputIdentifierCount) {
    this(inputIdentifier, attemptNumber, pathComponent, isShared, SPILL_INFO.FINAL_MERGE_ENABLED, -1, inputIdentifierCount);
  }

  public CompositeInputAttemptIdentifier(int inputIdentifier, int attemptNumber, String pathComponent,
      boolean shared, SPILL_INFO fetchTypeInfo, int spillEventId, int inputIdentifierCount) {
    this(inputIdentifier, attemptNumber, pathComponent, shared, fetchTypeInfo, spillEventId, inputIdentifierCount, null, -1);
  }

  public CompositeInputAttemptIdentifier(int inputIdentifier, int attemptNumber, String pathComponent,
      boolean shared, SPILL_INFO fetchTypeInfo, int spillEventId,
      int inputIdentifierCount, long[] partitionSizes, int taskIndex) {
    super(inputIdentifier, attemptNumber, pathComponent, shared, fetchTypeInfo, spillEventId);
    this.inputIdentifierCount = inputIdentifierCount;
    this.partitionSizes = partitionSizes;
    this.taskIndex = taskIndex;
    this.childInputIdentifiers = null;
  }
  public int getInputIdentifierCount() {
    return inputIdentifierCount;
  }

  public InputAttemptIdentifier expand(int inputIdentifierOffset) {
    return new InputAttemptIdentifier(getInputIdentifier() + inputIdentifierOffset, getAttemptNumber(),
        getPathComponent(), isShared(), getFetchTypeInfo(), getSpillEventId());
  }

  public boolean include(int thatInputIdentifier, int thatAttemptNumber) {
    return
        super.getInputIdentifier() <= thatInputIdentifier && thatInputIdentifier < (super.getInputIdentifier() + inputIdentifierCount) &&
        super.getAttemptNumber() == thatAttemptNumber;
  }

  public long getPartitionSize(int partitionId) {
    return partitionSizes == null ? -1L :
        (partitionSizes.length == 1 ? partitionSizes[0] : partitionSizes[partitionId]);
  }

  public int getTaskIndex() {
    return taskIndex;
  }

  public void setInputIdentifiersForReadPartitionAllOnce(List<InputAttemptIdentifier> inputIdentifiers) {
    this.childInputIdentifiers = inputIdentifiers;
  }

  public List<InputAttemptIdentifier> getInputIdentifiersForReadPartitionAllOnce() {
    return childInputIdentifiers;
  }

  // PathComponent & shared does not need to be part of the hashCode and equals computation.
  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

  @Override
  public String toString() {
    return super.toString() + ", count=" + inputIdentifierCount;
  }
}
