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
import org.apache.tez.dag.api.TezUncheckedException;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Container for a task number and an attempt number for the task.
 */
@Private
public class InputAttemptIdentifier {

  private final int inputIdentifier;
  private final int attemptNumber;
  private final String pathComponent;

  public static final String PATH_PREFIX = "attempt";
  public static final String PATH_PREFIX_MR3 = com.datamonad.mr3.container.ContainerID$.MODULE$.prefixInContainerWorkerEnv();

  public enum SPILL_INFO {
    FINAL_MERGE_ENABLED,  // Final merge is enabled at source
    INCREMENTAL_UPDATE,   // Final merge is disabled and qualifies for incremental spill updates.(i.e spill 0, 1 etc)
    FINAL_UPDATE          // Indicates final piece of data in the pipelined shuffle.
  }

  /**
   * For pipelined shuffles. These fields need not be part of equals() or hashCode() computation.
   * These fields are added for additional information about the source and are not meant to
   * alter the way these sources would be stored in hashmap.
   */
  private final byte fetchTypeInfo;
  private final int spillEventId;

  public InputAttemptIdentifier(int inputIndex, int attemptNumber) {
    this(inputIndex, attemptNumber, null);
  }

  public InputAttemptIdentifier(int inputIdentifier, int attemptNumber, String pathComponent) {
    this(inputIdentifier, attemptNumber, pathComponent, SPILL_INFO.FINAL_MERGE_ENABLED, -1);
  }

  public InputAttemptIdentifier(int inputIdentifier, int attemptNumber, String pathComponent,
      SPILL_INFO fetchTypeInfo, int spillEventId) {
    this.inputIdentifier = inputIdentifier;
    this.attemptNumber = attemptNumber;
    this.pathComponent = pathComponent;
    this.fetchTypeInfo = (byte)fetchTypeInfo.ordinal();
    this.spillEventId = spillEventId;

    if (pathComponent != null && !pathComponent.startsWith(PATH_PREFIX_MR3) && !pathComponent.startsWith(PATH_PREFIX)) {
      throw new TezUncheckedException(
          "Path component must start with: " + PATH_PREFIX_MR3 + "/" + PATH_PREFIX + ", " + this);
    }
  }

  public int getInputIdentifier() {
    return this.inputIdentifier;
  }

  public int getAttemptNumber() {
    return this.attemptNumber;
  }
  
  public String getPathComponent() {
    return this.pathComponent;
  }

  public SPILL_INFO getFetchTypeInfo() {
    if (fetchTypeInfo == SPILL_INFO.INCREMENTAL_UPDATE.ordinal()) {
      return SPILL_INFO.INCREMENTAL_UPDATE;
    } else if (fetchTypeInfo == SPILL_INFO.FINAL_UPDATE.ordinal()) {
      return SPILL_INFO.FINAL_UPDATE;
    }
    return SPILL_INFO.FINAL_MERGE_ENABLED;
  }

  public int getSpillEventId() {
    return spillEventId;
  }

  public boolean canRetrieveInputInChunks() {
    return (fetchTypeInfo == SPILL_INFO.INCREMENTAL_UPDATE.ordinal()) ||
        (fetchTypeInfo == SPILL_INFO.FINAL_UPDATE.ordinal());
  }

  public boolean include(int thatInputIdentifier, int thatAttemptNumber) {
    return this.inputIdentifier == thatInputIdentifier && this.attemptNumber == thatAttemptNumber;
  }

  // PathComponent does not need to be part of the hashCode and equals computation.
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + attemptNumber;
    result = prime * result + inputIdentifier;
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
    InputAttemptIdentifier other = (InputAttemptIdentifier) obj;
    if (attemptNumber != other.attemptNumber)
      return false;
    if (inputIdentifier != other.inputIdentifier)
      return false;
    // do not compare pathComponent as they may not always be present
    if (spillEventId != other.spillEventId)
      return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    s.append("InputAttemptIdentifier [inputIdentifier=");
    s.append(inputIdentifier);
    s.append(", attemptNumber=");
    s.append(attemptNumber);
    s.append(", pathComponent=");
    s.append(pathComponent);
    s.append(", spillType=");
    s.append(fetchTypeInfo);
    s.append(", spillId=");
    s.append(spillEventId);
    s.append("]");
    return s.toString();
  }
}
