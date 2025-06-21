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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.tez.common.annotation.ConfigurationClass;
import org.apache.tez.common.annotation.ConfigurationProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  private final static Logger LOG = LoggerFactory.getLogger(TezConfiguration.class);

  private static Map<String, Scope> PropertyScope = new HashMap<String, Scope>();

  static {
    setupConfigurationScope(TezConfiguration.class);
  }

  static void setupConfigurationScope(Class<?> clazz) {
    for (Field field : clazz.getFields()) {
      if (field.isAnnotationPresent(ConfigurationScope.class)) {
        ConfigurationScope confScope = field.getAnnotation(ConfigurationScope.class);
        if (field.getType() == String.class) {
          try {
            PropertyScope.put(field.get(null).toString(), confScope.value());
          } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
          } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        } else {
          throw new RuntimeException(field.getName() + " is not String type, should not been annotated with "
              + ConfigurationScope.class.getSimpleName());
        }
      }
    }
  }

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

  /**
   * String value. Specifies a directory where Tez can create temporary job artifacts.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_AM_STAGING_DIR = TEZ_PREFIX + "staging-dir";
  public static final String TEZ_AM_STAGING_DIR_DEFAULT = "/tmp/"
      + System.getProperty("user.name") + "/tez/staging";

  /**
   * Boolean value. Execution mode for the Tez application. True implies session mode. If the client
   * code is written according to best practices then the same code can execute in either mode based
   * on this configuration. Session mode is more aggressive in reserving execution resources and is
   * typically used for interactive applications where multiple DAGs are submitted in quick succession
   * by the same user. For long running applications, one-off executions, batch jobs etc non-session 
   * mode is recommended. If session mode is enabled then container reuse is recommended.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="boolean")
  public static final String TEZ_AM_SESSION_MODE = TEZ_AM_PREFIX + "mode.session";
  public static final boolean TEZ_AM_SESSION_MODE_DEFAULT = false;

  /**
   * String value. Command line options provided during the launch of the Tez
   * AppMaster process. Its recommended to not set any Xmx or Xms in these launch opts so that
   * Tez can determine them automatically.
   * */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_AM_LAUNCH_CMD_OPTS = TEZ_AM_PREFIX +  "launch.cmd-opts";
  public static final String TEZ_AM_LAUNCH_CMD_OPTS_DEFAULT = 
      "-XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps -XX:+UseNUMA -XX:+UseParallelGC";

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
  public static final String TEZ_AM_LAUNCH_ENV = TEZ_AM_PREFIX
      + "launch.env";
  public static final String TEZ_AM_LAUNCH_ENV_DEFAULT = "";

  /**
   * Int value. The number of threads used to listen to task heartbeat requests.
   * Expert level setting.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="integer")
  public static final String TEZ_AM_TASK_LISTENER_THREAD_COUNT =
      TEZ_AM_PREFIX + "task.listener.thread-count";
  public static final int TEZ_AM_TASK_LISTENER_THREAD_COUNT_DEFAULT = 30;

  /**
   * Int value. Specifies the number of times the app master can be launched in order to recover 
   * from app master failure. Typically app master failures are non-recoverable. This parameter 
   * is for cases where the app master is not at fault but is lost due to system errors.
   * Expert level setting.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="integer")
  public static final String TEZ_AM_MAX_APP_ATTEMPTS = TEZ_AM_PREFIX +
      "max.app.attempts";
  public static final int TEZ_AM_MAX_APP_ATTEMPTS_DEFAULT = 2;

  /**
   * Boolean value. Enabled blacklisting of nodes of nodes that are considered faulty. These nodes 
   * will not be used to execute tasks.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="boolean")
  public static final String TEZ_AM_NODE_BLACKLISTING_ENABLED = TEZ_AM_PREFIX
      + "node-blacklisting.enabled";
  public static final boolean TEZ_AM_NODE_BLACKLISTING_ENABLED_DEFAULT = true;
  
  /**
   * String value. Range of ports that the AM can use when binding for task connections. Leave blank
   * to use all possible ports. Expert level setting. It's hadoop standard range configuration.
   * For example 50000-50050,50100-50200
   */
  @ConfigurationScope(Scope.AM)
  public static final String TEZ_AM_TASK_AM_PORT_RANGE =
      TEZ_AM_PREFIX + "task.am.port-range";

  /** Int value. The amount of memory in MB to be used by the AppMaster */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="integer")
  // do not remove because MR3 test code (UtilsForConfTez) uses it
  public static final String TEZ_AM_RESOURCE_MEMORY_MB = TEZ_AM_PREFIX
      + "resource.memory.mb";
  public static final int TEZ_AM_RESOURCE_MEMORY_MB_DEFAULT = 1024;

  /** Int value. The number of virtual cores to be used by the app master */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="integer")
  public static final String TEZ_AM_RESOURCE_CPU_VCORES = TEZ_AM_PREFIX
      + "resource.cpu.vcores";
  public static final int TEZ_AM_RESOURCE_CPU_VCORES_DEFAULT = 1;

  // used in UtilsForConfTez.scala, MR3 
  /** Int value. The amount of memory in MB to be used by tasks. This applies to all tasks across
   * all vertices. Setting it to the same value for all tasks is helpful for container reuse and 
   * thus good for performance typically. */
  @ConfigurationScope(Scope.DAG)  // TODO vertex level
  @ConfigurationProperty(type="integer")
  public static final String TEZ_TASK_RESOURCE_MEMORY_MB = TEZ_TASK_PREFIX
      + "resource.memory.mb";
  public static final int TEZ_TASK_RESOURCE_MEMORY_MB_DEFAULT = 1024;

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

  /**
   * Auxiliary resources to be localized for the Tez AM and all its containers.
   *
   * Value is comma-separated list of fully-resolved directories or file paths. All resources
   * are made available into the working directory of the AM and/or containers i.e. $CWD.
   *
   * If directories are specified, they are not traversed recursively. Only files directly under the
   * specified directory are localized.
   *
   * All duplicate resources are ignored.
   *
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_AUX_URIS = TEZ_PREFIX + "aux.uris";

  /**
   * Boolean value. Allows to ignore 'tez.lib.uris'. Useful during development as well as 
   * raw Tez application where classpath is propagated with application
   * via {@link LocalResource}s. This is mainly useful for developer/debugger scenarios.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="boolean")
  public static final String TEZ_IGNORE_LIB_URIS = TEZ_PREFIX + "ignore.lib.uris";

  /**
   * Boolean value.
   * Specify whether hadoop libraries required to run Tez should be the ones deployed on the cluster.
   * This is disabled by default - with the expectation being that tez.lib.uris has a complete
   * tez-deployment which contains the hadoop libraries.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="boolean")
  public static final String TEZ_USE_CLUSTER_HADOOP_LIBS = TEZ_PREFIX + "use.cluster.hadoop-libs";
  public static final boolean TEZ_USE_CLUSTER_HADOOP_LIBS_DEFAULT = false;

  /**
   * String value. The queue name for all jobs being submitted from a given client.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_QUEUE_NAME = TEZ_PREFIX + "queue.name";

  /**
   * String value. Tags for the job that will be passed to YARN at submission
   * time. Queries to YARN for applications can filter on these tags.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_APPLICATION_TAGS = TEZ_PREFIX + "application.tags";

  /**
   * Enum value. Config to limit the type of events published to the history logging service.
   * The valid log levels are defined in the enum {@link HistoryLogLevel}. The default value is
   * defined in {@link HistoryLogLevel#DEFAULT}.
   */
  @ConfigurationScope(Scope.DAG)
  @ConfigurationProperty
  public static final String TEZ_HISTORY_LOGGING_LOGLEVEL =
      TEZ_PREFIX + "history.logging.log.level";

  /**
   *  Boolean value. Enable local mode execution in Tez. Enables tasks to run in the same process as
   *  the app master. Primarily used for debugging.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="boolean")
  public static final String TEZ_LOCAL_MODE =
    TEZ_PREFIX + "local.mode";

  public static final boolean TEZ_LOCAL_MODE_DEFAULT = false;

  // ACLs related configuration
  // Format supports a comma-separated list of users and groups with the users and groups separated
  // by whitespace. e.g. "user1,user2 group1,group2"
  // All users/groups that have access to do operations on the AM also have access to similar
  // operations on all DAGs within that AM/session.
  // By default, the "owner" i.e. the user who started the session will always have full admin
  // access to the AM. Also, the user that submitted the DAG has full admin access to all operations
  // on that DAG.
  //
  // If no value is specified or an invalid configuration is specified,
  // only the user who submitted the AM and/or DAG can do the appropriate operations.
  // For example, "user1,user2 group1, group2" is an invalid configuration value as splitting by
  // whitespace produces 3 lists instead of 2.

  // If the value specified is "*", all users are allowed to do the operation.

  /**
   * Boolean value. Configuration to enable/disable ACL checks.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="boolean")
  public static final String TEZ_AM_ACLS_ENABLED = TEZ_AM_PREFIX + "acls.enabled";
  public static final boolean TEZ_AM_ACLS_ENABLED_DEFAULT = true;

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

  // TODO only validate property here, value can also be validated if necessary
  public static void validateProperty(String property, Scope usedScope) {
    Scope validScope = PropertyScope.get(property);
    if (validScope == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(property + " is not standard configuration property of tez, can not been validated");
      }
    } else {
      if (usedScope.ordinal() > validScope.ordinal()) {
        throw new IllegalStateException(property + " is set at the scope of " + usedScope
            + ", but it is only valid in the scope of " + validScope);
      }
    }
  }

  static Set<String> getPropertySet() {
    return PropertyScope.keySet();
  }

  /**
   * Long value.
   * Time to wait (in seconds) for apps to complete on MiniTezCluster shutdown.
   */
  @ConfigurationScope(Scope.TEST)
  @ConfigurationProperty(type="long")
  public static final String TEZ_TEST_MINI_CLUSTER_APP_WAIT_ON_SHUTDOWN_SECS =
      TEZ_PREFIX + "test.minicluster.app.wait.on.shutdown.secs";
  public static final long TEZ_TEST_MINI_CLUSTER_APP_WAIT_ON_SHUTDOWN_SECS_DEFAULT = 30;

  /**
   *  Comma-separated list of properties that MRReaderMapred should return (if present) when calling for config updates.
   */
  @ConfigurationScope(Scope.VERTEX)
  @ConfigurationProperty
  public static final String TEZ_MRREADER_CONFIG_UPDATE_PROPERTIES = "tez.mrreader.config.update.properties";

}
