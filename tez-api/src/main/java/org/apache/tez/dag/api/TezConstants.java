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

package org.apache.tez.dag.api;

import org.apache.hadoop.classification.InterfaceAudience.Private;

/**
 * Specifies all constant values in Tez
 */
@Private
public class TezConstants {

  /*
   * Tez AM Service Authorization
   * These are the same as MR which allows Tez to run in secure
   * mode without configuring service ACLs
   */
  public static final String   
  TEZ_AM_SECURITY_SERVICE_AUTHORIZATION_TASK_UMBILICAL =
      "security.job.task.protocol.acl";
  public static final String   
  TEZ_AM_SECURITY_SERVICE_AUTHORIZATION_CLIENT =
      "security.job.client.protocol.acl";

  /*
   * Logger properties
   */
  public static final String TEZ_CONTAINER_LOG4J_PROPERTIES_FILE = "tez-container-log4j.properties";
  public static final String TEZ_CONTAINER_LOGGER_NAME = "CLA";
  public static final String TEZ_ROOT_LOGGER_NAME = "tez.root.logger";

  /**
   * The service id for the NodeManager plugin used to share intermediate data
   * between vertices.
   */
  public static final String TEZ_SHUFFLE_HANDLER_SERVICE_ID = "tez_shuffle";

  // Configuration keys used internally and not set by the users
  
  // These are session specific DAG ACL's. Currently here because these can only be specified
  // via code in the API.
  /**
   * DAG view ACLs. This allows the specified users/groups to view the status of the given DAG.
   */
  public static final String TEZ_DAG_VIEW_ACLS = TezConfiguration.TEZ_AM_PREFIX + "dag.view-acls";
  /**
   * DAG modify ACLs. This allows the specified users/groups to run modify operations on the DAG
   * such as killing the DAG.
   */
  public static final String TEZ_DAG_MODIFY_ACLS = TezConfiguration.TEZ_AM_PREFIX + "dag.modify-acls";

}
