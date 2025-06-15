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

import org.apache.hadoop.classification.InterfaceAudience.Private;

@Private
public class HostPort {

  // Since containerId decides a unique host, use containerId in equals() and hashCode().
  private final String host;
  private final String containerId;
  private final int port;

  public HostPort(String host, String containerId, int port) {
    this.host = host;
    this.containerId = containerId;
    this.port = port;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((containerId == null) ? 0 : containerId.hashCode());
    result = prime * result + port;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    // do not compare with getClass() because HostPort can be compared with InputHost
    // see ShuffleServer.onSuccess()
    if (!(obj instanceof HostPort))
      return false;
    HostPort other = (HostPort) obj;
    if (containerId == null) {
      if (other.containerId != null)
        return false;
    } else if (!containerId.equals(other.containerId))
      return false;
    if (port != other.port)
      return false;
    return true;
  }

  public String getHost() {
    return host;
  }

  public String getContainerId() {
    return containerId;
  }

  public int getPort() {
    return port;
  }

  @Override
  public String toString() {
    return host + ":" + containerId + ":" + port;
  }
}
