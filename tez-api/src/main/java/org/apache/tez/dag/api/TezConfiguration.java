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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tez.common.annotation.ConfigurationClass;
import org.apache.tez.common.annotation.ConfigurationProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.LocalResource;

import com.google.common.annotations.VisibleForTesting;


/**
 * Defines the configurations for Tez. These configurations are typically specified in 
 * tez-site.xml on the client machine where TezClient is used to launch the Tez application.
 * tez-site.xml is expected to be picked up from the classpath of the client process.
 * @see <a href="../../../../../configs/TezConfiguration.html">Detailed Configuration Information</a>
 * @see <a href="../../../../../configs/tez-default-template.xml">XML-based Config Template</a>
 */
@Public
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

  @Private
  public static final String TEZ_PREFIX = "tez.";
  @Private
  public static final String TEZ_AM_PREFIX = TEZ_PREFIX + "am.";
  @Private
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
   * String value that is a file path.
   * Path to a credentials file (with serialized credentials) located on the local file system.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_CREDENTIALS_PATH = TEZ_PREFIX + "credentials.path";

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

  /**
   * String value. Command line options which will be prepended to {@link
   * #TEZ_TASK_LAUNCH_CMD_OPTS} during the launch of Tez tasks.  This property will typically be configured to
   * include default options meant to be used by all jobs in a cluster. If required, the values can
   * be overridden per job.
   */
  @ConfigurationScope(Scope.AM) // TODO DAG/Vertex level
  @ConfigurationProperty
  public static final String TEZ_TASK_LAUNCH_CLUSTER_DEFAULT_CMD_OPTS =
      TEZ_TASK_PREFIX + "launch.cluster-default.cmd-opts";
  public static final String TEZ_TASK_LAUNCH_CLUSTER_DEFAULT_CMD_OPTS_DEFAULT =
      "-server -Djava.net.preferIPv4Stack=true -Dhadoop.metrics.log.level=WARN";

  /**
   * String value. Command line options provided during the launch of Tez Task
   * processes. Its recommended to not set any Xmx or Xms in these launch opts
   * so that Tez can determine them automatically.
   */
  @ConfigurationScope(Scope.AM) // TODO DAG/Vertex level
  @ConfigurationProperty
  public static final String TEZ_TASK_LAUNCH_CMD_OPTS = TEZ_TASK_PREFIX
      + "launch.cmd-opts";
  public static final String TEZ_TASK_LAUNCH_CMD_OPTS_DEFAULT =
      "-XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps -XX:+UseNUMA -XX:+UseParallelGC";

  /**
   * Double value. Tez automatically determines the Xmx for the JVMs used to run
   * Tez tasks and app masters. This feature is enabled if the user has not
   * specified Xmx or Xms values in the launch command opts. Doing automatic Xmx
   * calculation is preferred because Tez can determine the best value based on
   * actual allocation of memory to tasks the cluster. The value if used as a
   * fraction that is applied to the memory allocated Factor to size Xmx based
   * on container memory size. Value should be greater than 0 and less than 1.
   *
   * Set this value to -1 to allow Tez to use different default max heap fraction
   * for different container memory size. Current policy is to use 0.7 for container
   * smaller than 4GB and use 0.8 for larger container.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="float")
  public static final String TEZ_CONTAINER_MAX_JAVA_HEAP_FRACTION =
      TEZ_PREFIX + "container.max.java.heap.fraction";
  public static final double TEZ_CONTAINER_MAX_JAVA_HEAP_FRACTION_DEFAULT = 0.8;

  private static final String NATIVE_LIB_PARAM_DEFAULT = Shell.WINDOWS ?
    "PATH=%PATH%;%HADOOP_COMMON_HOME%\\bin":
    "LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_COMMON_HOME/lib/native/";

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
   * String value. Env settings will be merged with {@link #TEZ_TASK_LAUNCH_ENV}
   * during the launch of the task process. This property will typically be configured to
   * include default system env meant to be used by all jobs in a cluster. If required, the values can
   * be appended to per job.
   */
  @ConfigurationScope(Scope.VERTEX)
  @ConfigurationProperty
  public static final String TEZ_TASK_LAUNCH_CLUSTER_DEFAULT_ENV =
      TEZ_TASK_PREFIX + "launch.cluster-default.env";
  public static final String TEZ_TASK_LAUNCH_CLUSTER_DEFAULT_ENV_DEFAULT =
      NATIVE_LIB_PARAM_DEFAULT;

  /** String value. Env settings for the Tez Task processes.
   * Should be specified as a comma-separated of key-value pairs where each pair
   * is defined as KEY=VAL
   * e.g. "LD_LIBRARY_PATH=.,USERNAME=foo"
   * These take least precedence compared to other methods of setting env
   * These get added to the task environment prior to launching it.
   * This setting will prepend existing settings in the cluster default
   */
  @ConfigurationScope(Scope.VERTEX)
  @ConfigurationProperty
  public static final String TEZ_TASK_LAUNCH_ENV = TEZ_TASK_PREFIX
      + "launch.env";
  public static final String TEZ_TASK_LAUNCH_ENV_DEFAULT = "";

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
   * Int value. Configuration to limit the counters per dag (AppMaster and Task). This can be used
   * to
   * limit the amount of memory being used in the app master to store the
   * counters. Expert level setting.
   */
  @Unstable
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="integer")
  public static final String TEZ_COUNTERS_MAX = TEZ_PREFIX + "counters.max";
  public static final int TEZ_COUNTERS_MAX_DEFAULT = 1200;

  /**
   * Int value. Configuration to limit the number of counter groups for a DAG. This can be used to
   * limit the amount of memory being used in the app master to store the
   * counters. Expert level setting.
   */
  @Unstable
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="integer")
  public static final String TEZ_COUNTERS_MAX_GROUPS = TEZ_PREFIX + "counters.max.groups";
  public static final int TEZ_COUNTERS_MAX_GROUPS_DEFAULT = 500;

  /**
   * Int value. Configuration to limit the length of counter names. This can be used to
   * limit the amount of memory being used in the app master to store the
   * counters. Expert level setting.
   */
  @Unstable
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="integer")
  public static final String TEZ_COUNTERS_COUNTER_NAME_MAX_LENGTH =
      TEZ_PREFIX + "counters.counter-name.max-length";
  public static final int TEZ_COUNTERS_COUNTER_NAME_MAX_LENGTH_DEFAULT = 64;

  /**
   * Int value. Configuration to limit the counter group names per app master. This can be used to
   * limit the amount of memory being used in the app master to store the
   * counters. Expert level setting.
   */
  @Unstable
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="integer")
  public static final String TEZ_COUNTERS_GROUP_NAME_MAX_LENGTH =
      TEZ_PREFIX + "counters.group-name.max-length";
  public static final int TEZ_COUNTERS_GROUP_NAME_MAX_LENGTH_DEFAULT = 256;

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
  public static final String TEZ_AM_RESOURCE_MEMORY_MB = TEZ_AM_PREFIX
      + "resource.memory.mb";
  public static final int TEZ_AM_RESOURCE_MEMORY_MB_DEFAULT = 1024;

  /** Int value. The number of virtual cores to be used by the app master */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="integer")
  public static final String TEZ_AM_RESOURCE_CPU_VCORES = TEZ_AM_PREFIX
      + "resource.cpu.vcores";
  public static final int TEZ_AM_RESOURCE_CPU_VCORES_DEFAULT = 1;

  /** Int value. The amount of memory in MB to be used by tasks. This applies to all tasks across
   * all vertices. Setting it to the same value for all tasks is helpful for container reuse and 
   * thus good for performance typically. */
  @ConfigurationScope(Scope.DAG)  // TODO vertex level
  @ConfigurationProperty(type="integer")
  public static final String TEZ_TASK_RESOURCE_MEMORY_MB = TEZ_TASK_PREFIX
      + "resource.memory.mb";
  public static final int TEZ_TASK_RESOURCE_MEMORY_MB_DEFAULT = 1024;

  /**
   * Int value. The number of virtual cores to be used by tasks.
   */
  @ConfigurationScope(Scope.DAG)  // TODO vertex level
  @ConfigurationProperty(type="integer")
  public static final String TEZ_TASK_RESOURCE_CPU_VCORES = TEZ_TASK_PREFIX
      + "resource.cpu.vcores";
  public static final int TEZ_TASK_RESOURCE_CPU_VCORES_DEFAULT = 1; 

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
   * Int value. Maximum number of pending task events before a task will stop
   * asking for more events in the task heartbeat.
   * Expert level setting.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="integer")
  public static final String TEZ_TASK_MAX_EVENT_BACKLOG = TEZ_TASK_PREFIX +
      "max-event-backlog";
  public static final int TEZ_TASK_MAX_EVENT_BACKLOG_DEFAULT = 10000;

  /**
   * Boolean value. Backwards compatibility setting for initializing IO processor before
   * inputs and outputs.
   * Expert level setting.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="boolean")
  public static final String TEZ_TASK_INITIALIZE_PROCESSOR_FIRST = TEZ_TASK_PREFIX +
      "initialize-processor-first";
  public static final boolean TEZ_TASK_INITIALIZE_PROCESSOR_FIRST_DEFAULT = false;

  /**
   * Boolean value. Backwards compatibility setting for initializing inputs and outputs
   * serially instead of the parallel default.
   * Expert level setting.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="boolean")
  public static final String TEZ_TASK_INITIALIZE_PROCESSOR_IO_SERIALLY = TEZ_TASK_PREFIX +
      "initialize-processor-io-serially";
  public static final boolean TEZ_TASK_INITIALIZE_PROCESSOR_IO_SERIALLY_DEFAULT = false;

  /**
   * Whether to generate counters per IO or not. Enabling this will rename
   * CounterGroups / CounterNames to making them unique per Vertex +
   * Src|Destination
   */
  @Unstable
  @Private
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="boolean")
  public static final String TEZ_TASK_GENERATE_COUNTERS_PER_IO = TEZ_TASK_PREFIX
      + "generate.counters.per.io";
  @Private
  public static final boolean TEZ_TASK_GENERATE_COUNTERS_PER_IO_DEFAULT = false;

  /**
   * Whether to scale down memory requested by each component if the total
   * exceeds the available JVM memory
   */
  @Private
  @Unstable
  @ConfigurationScope(Scope.VERTEX)
  @ConfigurationProperty(type="boolean")
  public static final String TEZ_TASK_SCALE_MEMORY_ENABLED = TEZ_TASK_PREFIX
      + "scale.memory.enabled";
  @Private
  public static final boolean TEZ_TASK_SCALE_MEMORY_ENABLED_DEFAULT = true;

  /**
   * The allocator to use for initial memory allocation
   */
  @Private
  @Unstable
  @ConfigurationScope(Scope.VERTEX)
  @ConfigurationProperty
  public static final String TEZ_TASK_SCALE_MEMORY_ALLOCATOR_CLASS = TEZ_TASK_PREFIX
      + "scale.memory.allocator.class";
  @Private
  public static final String TEZ_TASK_SCALE_MEMORY_ALLOCATOR_CLASS_DEFAULT =
      "org.apache.tez.runtime.library.resources.WeightedScalingMemoryDistributor";

  /**
   * The fraction of the JVM memory which will not be considered for allocation.
   * No defaults, since there are pre-existing defaults based on different scenarios.
   */
  @Private
  @Unstable
  @ConfigurationScope(Scope.VERTEX)
  @ConfigurationProperty(type="double")
  public static final String TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION = TEZ_TASK_PREFIX
      + "scale.memory.reserve-fraction";
  @Private
  public static final double TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION_DEFAULT = 0.3d;

  /**
   * Fraction of available memory to reserve per input/output. This amount is
   * removed from the total available pool before allocation and is for factoring in overheads.
   */
  @Private
  @Unstable
  @ConfigurationScope(Scope.VERTEX)
  @ConfigurationProperty(type="float")
  public static final String TEZ_TASK_SCALE_MEMORY_ADDITIONAL_RESERVATION_FRACTION_PER_IO =
      TEZ_TASK_PREFIX + "scale.memory.additional-reservation.fraction.per-io";

  @Private
  @Unstable
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
  @Private
  @Unstable
  @ConfigurationScope(Scope.VERTEX)
  @ConfigurationProperty
  public static final String TEZ_TASK_SCALE_MEMORY_WEIGHTED_RATIOS =
      TEZ_TASK_PREFIX + "scale.memory.ratios";

  /**
   * Concurrent input/output memory allocation control. When enabled memory
   * distributions assume that inputs and outputs will use their memory
   * simultaneously. When disabled the distributions assume that outputs are not
   * initialized until inputs release memory buffers, allowing inputs to
   * leverage memory normally set aside for outputs and vice-versa.
   * NOTE: This property currently is not supported by the ScalingAllocator
   *       memory distributor.
   */
  @Private
  @Unstable
  @ConfigurationScope(Scope.VERTEX)
  public static final String TEZ_TASK_SCALE_MEMORY_INPUT_OUTPUT_CONCURRENT =
      TEZ_TASK_PREFIX + "scale.memory.input-output-concurrent";
  public static final boolean TEZ_TASK_SCALE_MEMORY_INPUT_OUTPUT_CONCURRENT_DEFAULT = true;

  /**
   * Controls distributing output memory to inputs when non-concurrent I/O
   * memory allocation is being used.  When enabled inputs will receive the
   * same memory allocation as if concurrent I/O memory allocation were used.
   * NOTE: This property currently is not supported by the ScalingAllocator
   *       memory distributor.
   */
  @Private
  @Unstable
  @ConfigurationScope(Scope.VERTEX)
  public static final String TEZ_TASK_SCALE_MEMORY_NON_CONCURRENT_INPUTS_ENABLED =
      TEZ_TASK_PREFIX + "scale.memory.non-concurrent-inputs.enabled";
  public static final boolean TEZ_TASK_SCALE_MEMORY_NON_CONCURRENT_INPUTS_ENABLED_DEFAULT = false;

  @Private
  @Unstable
  /**
   * Defines the ProcessTree implementation which will be used to collect resource utilization.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_TASK_RESOURCE_CALCULATOR_PROCESS_TREE_CLASS =
      TEZ_TASK_PREFIX + "resource.calculator.process-tree.class";


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
   *
   * Specify additional user classpath information to be used for Tez AM and all containers.
   * This will be appended to the classpath after PWD
   * 
   * 'tez.lib.uris.classpath' defines the relative classpath into the archives
   * that are set in 'tez.lib.uris'
   *
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_LIB_URIS_CLASSPATH = TEZ_PREFIX + "lib.uris.classpath";

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
  @Unstable
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
   * Boolean value.
   * Specify whether the user classpath takes precedence over the Tez framework
   * classpath.
   */
  @ConfigurationScope(Scope.CLIENT)
  @ConfigurationProperty(type="boolean")
  public static final String TEZ_USER_CLASSPATH_FIRST = TEZ_PREFIX + "user.classpath.first";
  public static final boolean TEZ_USER_CLASSPATH_FIRST_DEFAULT = true;

  /**
   * String value.
   *
   * Specify additional classpath information to be used for Tez AM and all containers.
   * If {@link #TEZ_USER_CLASSPATH_FIRST} is true then this will be added to the classpath
   * before all framework specific components have been specified, otherwise this will
   * be added after the framework specific components.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_CLUSTER_ADDITIONAL_CLASSPATH_PREFIX =
      TEZ_PREFIX + "cluster.additional.classpath.prefix";

  /**
   * Boolean value.
   * If this value is true then tez explicitly adds hadoop conf directory into classpath for AM and
   * task containers. Default is false.
   */
  @Private
  @Unstable
  @ConfigurationScope(Scope.CLIENT)
  @ConfigurationProperty(type="boolean")
  public static final String TEZ_CLASSPATH_ADD_HADOOP_CONF = TEZ_PREFIX +
      "classpath.add-hadoop-conf";
  public static final boolean TEZ_CLASSPATH_ADD_HADOOP_CONF_DEFAULT = false;

  /**
   * Session-related properties
   */
  @Private
  @ConfigurationProperty
  public static final String TEZ_SESSION_PREFIX =
      TEZ_PREFIX + "session.";

  /**
   * Int value. Time (in seconds) for which the Tez AM should wait for a DAG to be submitted before
   * shutting down. Only relevant in session mode. Any negative value will disable this check and
   * allow the AM to hang around forever in idle mode.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="integer")
  public static final String TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS =
      TEZ_SESSION_PREFIX + "am.dag.submit.timeout.secs";
  public static final int TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS_DEFAULT =
      300;

  /**
   * String value. The queue name for all jobs being submitted from a given client.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_QUEUE_NAME = TEZ_PREFIX + "queue.name";

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

  /**
   * Int value.
   * The maximium number of tasks running in parallel within the app master process.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="integer")
  public static final String TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS =
    TEZ_AM_PREFIX + "inline.task.execution.max-tasks";

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

  @VisibleForTesting
  static Set<String> getPropertySet() {
    return PropertyScope.keySet();
  }

  /**
   * Long value.
   * Time to wait (in seconds) for apps to complete on MiniTezCluster shutdown.
   */
  @Private
  @ConfigurationScope(Scope.TEST)
  @ConfigurationProperty(type="long")
  public static final String TEZ_TEST_MINI_CLUSTER_APP_WAIT_ON_SHUTDOWN_SECS =
      TEZ_PREFIX + "test.minicluster.app.wait.on.shutdown.secs";
  public static final long TEZ_TEST_MINI_CLUSTER_APP_WAIT_ON_SHUTDOWN_SECS_DEFAULT = 30;

  /**
   * String value. Determines what JVM properties will be logged for debugging purposes
   * in the AM and Task runtime logs.
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_JVM_SYSTEM_PROPERTIES_TO_LOG  =
      TEZ_PREFIX + "tez.jvm.system-properties-to-log";
  public static final List<String> TEZ_JVM_SYSTEM_PROPERTIES_TO_LOG_DEFAULT =
      Collections.unmodifiableList(Arrays.asList(
          "os.name","os.version","java.home","java.runtime.version",
          "java.vendor","java.version","java.vm.name","java.class.path",
          "java.io.tmpdir","user.dir","user.name"));

  /**
   * Int value. Time interval (in seconds). If the Tez AM does not receive a heartbeat from the
   * client within this time interval, it will kill any running DAG and shut down. Required to
   * re-cycle orphaned Tez applications where the client is no longer alive. A negative value
   * can be set to disable this check. For a positive value, the minimum value is 10 seconds.
   * Values between 0 and 10 seconds will be reset to the minimum value.
   * Only relevant in session mode.
   * This is disabled by default i.e. by default, the Tez AM will go on to
   * complete the DAG and only kill itself after hitting the DAG submission timeout defined by
   * {@link #TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS}
   */
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty(type="integer")
  public static final String TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS =
      TEZ_PREFIX + "am.client.heartbeat.timeout.secs";
  public static final int TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS_DEFAULT = -1;


  @Private
  @ConfigurationScope(Scope.AM)
  public static final String TEZ_AM_CLIENT_HEARTBEAT_POLL_INTERVAL_MILLIS =
      TEZ_PREFIX + "am.client.heartbeat.poll.interval.millis";
  public static final int TEZ_AM_CLIENT_HEARTBEAT_POLL_INTERVAL_MILLIS_DEFAULT = -1;

  /**
   * Int value. Minimum number of threads to be allocated by TezSharedExecutor.
   */
  @Private
  @ConfigurationScope(Scope.AM)
  public static final String TEZ_SHARED_EXECUTOR_MIN_THREADS = "tez.shared-executor.min-threads";
  public static final int TEZ_SHARED_EXECUTOR_MIN_THREADS_DEFAULT = 0;

  /**
   * Int value. Maximum number of threads to be allocated by TezSharedExecutor. If value is negative
   * then Integer.MAX_VALUE is used as the limit.
   * Default: Integer.MAX_VALUE.
   */
  @Private
  @ConfigurationScope(Scope.AM)
  public static final String TEZ_SHARED_EXECUTOR_MAX_THREADS = "tez.shared-executor.max-threads";
  public static final int TEZ_SHARED_EXECUTOR_MAX_THREADS_DEFAULT = -1;

  /**
   *  Comma-separated list of properties that MRReaderMapred should return (if present) when calling for config updates.
   */
  @ConfigurationScope(Scope.VERTEX)
  @ConfigurationProperty
  public static final String TEZ_MRREADER_CONFIG_UPDATE_PROPERTIES = "tez.mrreader.config.update.properties";

}
