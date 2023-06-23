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

package org.apache.tez.runtime.api.events;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Output;

import java.nio.ByteBuffer;

/**
 * Event used by user code to send information between tasks. An output can
 * generate an Event of this type to sending information regarding output data
 * ( such as URI for file-based output data, port info in case of
 * streaming-based data transfers ) to the Input on the destination vertex.
 */
@Public
public final class DataMovementEvent extends Event
  implements com.datamonad.mr3.api.EventToLogicalInput {

  //
  // for MR3
  //

  public int srcOutputIndex() {
    return sourceIndex;
  }

  // should not be called concurrently
  public DataMovementEvent updateSrcOutputDestInputIndex(int newSrcOutputIndex, int newDestInputIndex) {
    /*
    // this is unsafe in local mode because multiple ContainerWorkers may share the same DataMovementEvent
    if (this.sourceIndex < 0 || this.targetIndex < 0) {
      this.sourceIndex = newSrcOutputIndex;
      this.targetIndex = newDestInputIndex;
      return this;
    }
     */
    return new DataMovementEvent(newSrcOutputIndex, newDestInputIndex, version, userPayload);
  }

  public CompositeRoutedDataMovementEvent createCompositeEvent(
      int srcOutputIndex, int destInputIndex, int count) {
    return CompositeRoutedDataMovementEvent.create(srcOutputIndex, destInputIndex, count, version, userPayload);
  }

  @Private
  public static DataMovementEvent createRaw(int version,
                                            ByteBuffer userPayload) {
    return new DataMovementEvent(-1, -1, version, userPayload);
  }

  public void updateSrcOutputIndex(int newSrcOutputIndex) {
    this.sourceIndex = newSrcOutputIndex;
  }

  //
  // from Tez
  //

  // sourceIndex is stored in LogicalOutputMetaData and does not need to be included in EventToLogicalInput
  /**
   * Index(i) of the i-th (physical) Input or Output that generated an Event.
   * For a Processor-generated event, this is ignored.
   */
  private int sourceIndex;

  // targetIndex is set by EdgeManager and does not need to be included in EventToLogicalInput
  /**
   * Index(i) of the i-th (physical) Input or Output that is meant to receive
   * this Event. For a Processor event, this is ignored.
   */
  private int targetIndex;

  /**
   * User Payload for this Event
   */
  private final ByteBuffer userPayload;

  /**
   * Version number to indicate what attempt generated this Event
   */
  private int version;


  @Private
  DataMovementEvent(int sourceIndex,
                    int targetIndex,
                    int version,
                    ByteBuffer userPayload) {
    this.userPayload = userPayload;
    this.sourceIndex = sourceIndex;
    this.version = version;
    this.targetIndex = targetIndex;
  }

  private DataMovementEvent(ByteBuffer userPayload) {
    this(-1, -1, -1, userPayload);
  }

  /**
   * User Event constructor for {@link Output}s
   * @param sourceIndex Index to identify the physical edge of the input/output
   * that generated the event
   * @param userPayload User Payload of the User Event
   */
  public static DataMovementEvent create(int sourceIndex,
                                         ByteBuffer userPayload) {
    return new DataMovementEvent(sourceIndex, -1, -1, userPayload);
  }
  
  @Private
  /**
   * Constructor for Processor-generated User Events
   * @param userPayload
   */
  public static DataMovementEvent create(ByteBuffer userPayload) {
    return new DataMovementEvent(userPayload);
  }

  @Private
  public static DataMovementEvent create(int sourceIndex,
                                         int targetIndex,
                                         int version,
                                         ByteBuffer userPayload) {
    return new DataMovementEvent(sourceIndex, targetIndex, version, userPayload);
  }
  
  /**
   * Make a routable copy of the {@link DataMovementEvent} by adding a target
   * input index
   * 
   * @param targetIndex
   *          The index of the physical input to which this
   *          {@link DataMovementEvent} should be routed
   * @return Copy of this {@link DataMovementEvent} with the target input index
   *         added to it
   */
  @Private
  public DataMovementEvent makeCopy(int targetIndex) {
    return new DataMovementEvent(sourceIndex, targetIndex, version, userPayload);
  }

  public ByteBuffer getUserPayload() {
    return userPayload == null ? null : userPayload.asReadOnlyBuffer();
  }

  public int getSourceIndex() {
    return sourceIndex;
  }

  public int getTargetIndex() {
    return targetIndex;
  }

  @Private
  public void setTargetIndex(int targetIndex) {
    this.targetIndex = targetIndex;
  }

  public int getVersion() {
    return version;
  }

  @Private
  public void setVersion(int version) {
    this.version = version;
  }

  @Override
  public String toString() {
    return "DataMovementEvent [sourceIndex=" + sourceIndex + ", targetIndex="
        + targetIndex + ", version=" + version + "]";
  }
}
