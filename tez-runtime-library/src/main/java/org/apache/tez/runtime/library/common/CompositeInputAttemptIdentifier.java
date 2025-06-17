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

/**
 * Container for a task number and an attempt number for the task.
 */
public class CompositeInputAttemptIdentifier {

  private final InputAttemptIdentifier input;

  private final int inputIdentifierCount;

  public CompositeInputAttemptIdentifier(int inputIdentifier, int attemptNumber, String pathComponent, int inputIdentifierCount) {
    this(inputIdentifier, attemptNumber, pathComponent, InputAttemptIdentifier.SPILL_INFO.FINAL_MERGE_ENABLED, -1, inputIdentifierCount);
  }

  public CompositeInputAttemptIdentifier(int inputIdentifier, int attemptNumber, String pathComponent,
                                         InputAttemptIdentifier.SPILL_INFO fetchTypeInfo, int spillEventId, int inputIdentifierCount) {
    this.input = new InputAttemptIdentifier(inputIdentifier, attemptNumber, pathComponent, fetchTypeInfo, spillEventId);
    this.inputIdentifierCount = inputIdentifierCount;
  }

  public CompositeInputAttemptIdentifier(InputAttemptIdentifier input) {
    this.input = input;
    this.inputIdentifierCount = 1;
  }

  public InputAttemptIdentifier getInput() {
    return input;
  }

  public int getInputIdentifierCount() {
    return inputIdentifierCount;
  }

  public int getInputIdentifier() {
    return input.getInputIdentifier();
  }

  public int getAttemptNumber() {
    return input.getAttemptNumber();
  }

  public String getPathComponent() {
    return input.getPathComponent();
  }

  public InputAttemptIdentifier.SPILL_INFO getFetchTypeInfo() {
    return input.getFetchTypeInfo();
  }

  public int getSpillEventId() {
    return input.getSpillEventId();
  }

  public boolean canRetrieveInputInChunks() {
    return input.canRetrieveInputInChunks();
  }

  public InputAttemptIdentifier expand(int inputIdentifierOffset) {
    return new InputAttemptIdentifier(getInputIdentifier() + inputIdentifierOffset,
        getAttemptNumber(), getPathComponent(), getFetchTypeInfo(), getSpillEventId());
  }

  public boolean include(int thatInputIdentifier, int thatAttemptNumber) {
    return
        getInputIdentifier() <= thatInputIdentifier && thatInputIdentifier < (getInputIdentifier() + inputIdentifierCount) &&
        getAttemptNumber() == thatAttemptNumber;
  }

  // PathComponent & shared does not need to be part of the hashCode and equals computation.
  @Override
  public int hashCode() {
    return 31 * input.hashCode() + inputIdentifierCount;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    CompositeInputAttemptIdentifier other = (CompositeInputAttemptIdentifier) obj;
    return input.equals(other.input) && inputIdentifierCount == other.inputIdentifierCount;
  }

  @Override
  public String toString() {
    return input.toString() + ", count=" + inputIdentifierCount;
  }
}
