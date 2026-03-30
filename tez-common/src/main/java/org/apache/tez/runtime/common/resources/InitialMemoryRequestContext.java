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

package org.apache.tez.runtime.common.resources;

import java.util.Objects;

public class InitialMemoryRequestContext {

  public enum RequestType {
    PARTITIONED_UNSORTED_OUTPUT,
    UNSORTED_OUTPUT,
    UNSORTED_INPUT,
    SORTED_OUTPUT,
    SORTED_MERGED_INPUT,
    PROCESSOR,
    OTHER
  };

  public enum ComponentType {
    INPUT, OUTPUT, PROCESSOR
  }

  private final long requestedSize;
  private final RequestType requestType;
  private final ComponentType componentType;
  private final String componentVertexName;

  public InitialMemoryRequestContext(long requestedSize, RequestType requestType,
      ComponentType componentType, String componentVertexName) {
    Objects.requireNonNull(requestType, "requestType is null");
    Objects.requireNonNull(componentType, "componentType is null");
    Objects.requireNonNull(componentVertexName, "componentVertexName is null");
    this.requestedSize = requestedSize;
    this.requestType = requestType;
    this.componentType = componentType;
    this.componentVertexName = componentVertexName;
  }

  public long getRequestedSize() {
    return requestedSize;
  }

  public RequestType getRequestType() {
    return requestType;
  }

  public ComponentType getComponentType() {
    return componentType;
  }

  public String getComponentVertexName() {
    return componentVertexName;
  }
}
