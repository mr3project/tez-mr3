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

import org.apache.tez.runtime.library.common.InputAttemptIdentifier;

import java.util.Map;

public class FetchResult {

  private final long shuffleManagerId;
  private final String host;
  private final int port;

  private Map<InputAttemptIdentifier, InputHost.PartitionRange> pendingInputs;

  public FetchResult(long shuffleManagerId, String host, int port,
                     Map<InputAttemptIdentifier, InputHost.PartitionRange> pendingInputs) {
    this.shuffleManagerId = shuffleManagerId;
    this.host = host;
    this.port = port;
    this.pendingInputs = pendingInputs;
  }

  public long getShuffleManagerId() {
    return shuffleManagerId;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public Map<InputAttemptIdentifier, InputHost.PartitionRange> getPendingInputs() {
    return pendingInputs;
  }
}
