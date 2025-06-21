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

package org.apache.tez.common.security;

/**
 * Access controls for the DAG
 */
public class DAGAccessControls {

  // only for compiling Hive-MR3
  /**
   * Helper API to support YARN and MapReduce format for specifying users and groups.
   * Format supports a comma-separated list of users and groups with the users and groups separated
   * by whitespace. e.g. "user1,user2 group1,group2"
   * If the value specified is "*", all users are allowed to do the operation.
   * @param viewACLsStr
   * @param modifyACLsStr
   */
  public DAGAccessControls(String viewACLsStr, String modifyACLsStr) {
  }
}
