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

import org.apache.tez.common.annotation.ConfigurationClass;
import org.apache.tez.common.annotation.ConfigurationProperty;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.LocalResource;

/**
 * Defines the configurations for Tez. These configurations are typically specified in 
 * tez-site.xml on the client machine where TezClient is used to launch the Tez application.
 * tez-site.xml is expected to be picked up from the classpath of the client process.
 * @see <a href="../../../../../configs/TezConfiguration.html">Detailed Configuration Information</a>
 * @see <a href="../../../../../configs/tez-default-template.xml">XML-based Config Template</a>
 */
@ConfigurationClass(templateFileName = "tez-default-template.xml")
public class TezConfiguration extends Configuration {

  public final static String TEZ_SITE_XML = "tez-site.xml";

  public TezConfiguration() {
    this(true);
  }

  public TezConfiguration(Configuration conf) {
    super(conf);
    addResource(TEZ_SITE_XML);
  }

  public TezConfiguration(boolean loadDefaults) {
    super(loadDefaults);
    if (loadDefaults) {
      addResource(TEZ_SITE_XML);
    }
  }

  public static final String TEZ_PREFIX = "tez.";
  public static final String TEZ_AM_PREFIX = TEZ_PREFIX + "am.";
  public static final String TEZ_TASK_PREFIX = TEZ_PREFIX + "task.";

  /**
   * String value. Specifies the name of the shuffle auxiliary service.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID = TEZ_AM_PREFIX +
      "shuffle.auxiliary-service.id";
  public static final String TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT =
      TezConstants.TEZ_SHUFFLE_HANDLER_SERVICE_ID;

  // read in Hive
  /**
   * String value. Specifies a directory where Tez can create temporary job artifacts.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_AM_STAGING_DIR = TEZ_PREFIX + "staging-dir";
  public static final String TEZ_AM_STAGING_DIR_DEFAULT = "/tmp/"
      + System.getProperty("user.name") + "/tez/staging";

  // read in Hive
  /** String value. Env settings for the Tez AppMaster process.
   * Should be specified as a comma-separated of key-value pairs where each pair
   * is defined as KEY=VAL
   * e.g. "LD_LIBRARY_PATH=.,USERNAME=foo"
   * These take least precedence compared to other methods of setting env.
   * These get added to the app master environment prior to launching it.
   * This setting will prepend existing settings in the cluster default
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_AM_LAUNCH_ENV = TEZ_AM_PREFIX + "launch.env";
  public static final String TEZ_AM_LAUNCH_ENV_DEFAULT = "";

  // used in UtilsForConfTez.scala, MR3
  /** Int value. The amount of memory in MB to be used by the AppMaster */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="integer")
  // do not remove because MR3 test code (UtilsForConfTez) uses it
  public static final String TEZ_AM_RESOURCE_MEMORY_MB = TEZ_AM_PREFIX + "resource.memory.mb";
  public static final int TEZ_AM_RESOURCE_MEMORY_MB_DEFAULT = 1024;

  // used in UtilsForConfTez.scala, MR3
  /** Int value. The amount of memory in MB to be used by tasks. This applies to all tasks across
   * all vertices. Setting it to the same value for all tasks is helpful for container reuse and 
   * thus good for performance typically. */
  @ConfigurationScope(Scope.DAG)
  @ConfigurationProperty(type="integer")
  public static final String TEZ_TASK_RESOURCE_MEMORY_MB = TEZ_TASK_PREFIX + "resource.memory.mb";
  public static final int TEZ_TASK_RESOURCE_MEMORY_MB_DEFAULT = 1024;

  // read in Hive
  /**
   * Int value. The maximum heartbeat interval, in milliseconds, between the app master and tasks.
   * Increasing this can help improve app master scalability for a large number of concurrent tasks.
   * Expert level setting.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="integer")
  public static final String TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS = TEZ_TASK_PREFIX
      + "am.heartbeat.interval-ms.max";
  public static final int TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS_DEFAULT = 100;

  // read in Hive
  /**
   * Int value. Interval, in milliseconds, after which counters are sent to AM in heartbeat from
   * tasks. This reduces the amount of network traffice between AM and tasks to send high-volume
   * counters. Improves AM scalability. Expert level setting.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="integer")
  public static final String TEZ_TASK_AM_HEARTBEAT_COUNTER_INTERVAL_MS = TEZ_TASK_PREFIX
      + "am.heartbeat.counter.interval-ms.max";
  public static final int TEZ_TASK_AM_HEARTBEAT_COUNTER_INTERVAL_MS_DEFAULT =
      4000;

  // read in Hive
  /**
   * Int value. Maximum number of events to fetch from the AM by the tasks in a single heartbeat.
   * Expert level setting. Expert level setting.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="integer")
  public static final String TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT = TEZ_TASK_PREFIX
      + "max-events-per-heartbeat";
  public static final int TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT_DEFAULT = 500;

  /**
   * Whether to scale down memory requested by each component if the total
   * exceeds the available JVM memory
   */
  @ConfigurationScope(Scope.VERTEX)
  @ConfigurationProperty(type="boolean")
  public static final String TEZ_TASK_SCALE_MEMORY_ENABLED = TEZ_TASK_PREFIX
      + "scale.memory.enabled";
  public static final boolean TEZ_TASK_SCALE_MEMORY_ENABLED_DEFAULT = true;

  /**
   * The allocator to use for initial memory allocation
   */
  @ConfigurationScope(Scope.VERTEX)
  @ConfigurationProperty
  public static final String TEZ_TASK_SCALE_MEMORY_ALLOCATOR_CLASS = TEZ_TASK_PREFIX
      + "scale.memory.allocator.class";
  public static final String TEZ_TASK_SCALE_MEMORY_ALLOCATOR_CLASS_DEFAULT =
      "org.apache.tez.runtime.library.resources.WeightedScalingMemoryDistributor";

  /**
   * The fraction of the JVM memory which will not be considered for allocation.
   * No defaults, since there are pre-existing defaults based on different scenarios.
   */
  @ConfigurationScope(Scope.VERTEX)
  @ConfigurationProperty(type="double")
  public static final String TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION = TEZ_TASK_PREFIX
      + "scale.memory.reserve-fraction";
  public static final double TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION_DEFAULT = 0.3d;

  /**
   * Fraction of available memory to reserve per input/output. This amount is
   * removed from the total available pool before allocation and is for factoring in overheads.
   */
  @ConfigurationScope(Scope.VERTEX)
  @ConfigurationProperty(type="float")
  public static final String TEZ_TASK_SCALE_MEMORY_ADDITIONAL_RESERVATION_FRACTION_PER_IO =
      TEZ_TASK_PREFIX + "scale.memory.additional-reservation.fraction.per-io";

  /**
   * Max cumulative total reservation for additional IOs.
   */
  @ConfigurationScope(Scope.VERTEX)
  @ConfigurationProperty(type="float")
  public static final String TEZ_TASK_SCALE_MEMORY_ADDITIONAL_RESERVATION_FRACTION_MAX =
      TEZ_TASK_PREFIX + "scale.memory.additional-reservation.fraction.max";
  /*
   * Weighted ratios for individual component types in the RuntimeLibrary.
   * e.g. PARTITIONED_UNSORTED_OUTPUT:0,UNSORTED_INPUT:1,UNSORTED_OUTPUT:0,SORTED_OUTPUT:2,
   * SORTED_MERGED_INPUT:3,PROCESSOR:1,OTHER:1
   */
  @ConfigurationScope(Scope.VERTEX)
  @ConfigurationProperty
  public static final String TEZ_TASK_SCALE_MEMORY_WEIGHTED_RATIOS =
      TEZ_TASK_PREFIX + "scale.memory.ratios";

  // read in Hive
  /**
   * Int value. The minimum number of containers that will be held in session mode. Not active in 
   * non-session mode. Enables an idle session (not running any DAG) to hold on to a minimum number
   * of containers to provide fast response times for the next DAG.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="integer")
  public static final String TEZ_AM_SESSION_MIN_HELD_CONTAINERS =
      TEZ_AM_PREFIX + "session.min.held-containers";
  public static final int TEZ_AM_SESSION_MIN_HELD_CONTAINERS_DEFAULT = 0;

  // read in Hive
  /**
   * String value to a file path.
   * The location of the Tez libraries which will be localized for DAGs.
   * This follows the following semantics
   * <ol>
   * <li> To use .tar.gz or .tgz files (generated by the tez or hadoop builds), the full path to this
   * file (including filename) should be specified. The internal structure of the uncompressed tgz
   * will be defined by 'tez.lib.uris.classpath'</li>
   *
   * <li> If a single file is specified without the above mentioned extensions - it will be treated as
   * a regular file. This means it will not be uncompressed during runtime. </li>
   *
   * <li> If multiple entries exist
   * <ul>
   * <li> Regular Files: will be treated as regular files (not uncompressed during runtime) </li>
   * <li> Archive Files: will be treated as archives and will be uncompressed during runtime </li>
   * <li> Directories: all files under the directory (non-recursive) will be made available (but not
   * uncompressed during runtime). </li>
   * </ul>
   * </ol>
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_LIB_URIS = TEZ_PREFIX + "lib.uris";

  // read in Hive
  /**
   * Boolean value. Allows to ignore 'tez.lib.uris'. Useful during development as well as 
   * raw Tez application where classpath is propagated with application
   * via {@link LocalResource}s. This is mainly useful for developer/debugger scenarios.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="boolean")
  public static final String TEZ_IGNORE_LIB_URIS = TEZ_PREFIX + "ignore.lib.uris";

  // read in Hive
  /**
   * String value. The queue name for all jobs being submitted from a given client.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_QUEUE_NAME = TEZ_PREFIX + "queue.name";

  // read in Hive
  /**
   * String value. Tags for the job that will be passed to YARN at submission
   * time. Queries to YARN for applications can filter on these tags.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_APPLICATION_TAGS = TEZ_PREFIX + "application.tags";

  // read in Hive
  /**
   *  Boolean value. Enable local mode execution in Tez. Enables tasks to run in the same
process as
   *  the app master. Primarily used for debugging.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="boolean")
  public static final String TEZ_LOCAL_MODE = TEZ_PREFIX + "local.mode";
  public static final boolean TEZ_LOCAL_MODE_DEFAULT = false;

  // read in Hive
  /**
   * String value. 
   * AM view ACLs. This allows the specified users/groups to view the status of the AM and all DAGs
   * that run within this AM.
   * Comma separated list of users, followed by whitespace, followed by a comma separated list of 
   * groups
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_AM_VIEW_ACLS = TEZ_AM_PREFIX + "view-acls";

  // read in Hive
  /**
   * String value.
   * AM modify ACLs. This allows the specified users/groups to run modify operations on the AM
   * such as submitting DAGs, pre-warming the session, killing DAGs or shutting down the session.
   * Comma separated list of users, followed by whitespace, followed by a comma separated list of 
   * groups
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_AM_MODIFY_ACLS = TEZ_AM_PREFIX + "modify-acls";

  /**
   *  Comma-separated list of properties that MRReaderMapred should return (if present) when calling for config updates.
   */
  @ConfigurationScope(Scope.VERTEX)
  @ConfigurationProperty
  public static final String TEZ_MRREADER_CONFIG_UPDATE_PROPERTIES = "tez.mrreader.config.update.properties";

  public static void validateProperty(String property, Scope usedScope) {
    // unused in MR3
  }
}
