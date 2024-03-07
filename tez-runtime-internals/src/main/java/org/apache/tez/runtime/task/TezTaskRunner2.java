/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.task;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.Multimap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.common.TezExecutors;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.ObjectRegistry;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.internals.api.TaskReporterInterface;

public class TezTaskRunner2 {

  @Deprecated
  public TezTaskRunner2(Configuration tezConf, UserGroupInformation ugi, String[] localDirs,
      TaskSpec taskSpec, int appAttemptNumber,
      Map<String, ByteBuffer> serviceConsumerMetadata,
      Map<String, String> serviceProviderEnvMap,
      Multimap<String, String> startedInputsMap,
      TaskReporterInterface taskReporter, ExecutorService executor,
      ObjectRegistry objectRegistry, String pid,
      ExecutionContext executionContext, long memAvailable,
      boolean updateSysCounters) throws IOException {
    this(tezConf, ugi, localDirs, taskSpec, appAttemptNumber, serviceConsumerMetadata,
        serviceProviderEnvMap, startedInputsMap, taskReporter, executor, objectRegistry,
        pid, executionContext, memAvailable, updateSysCounters, null);
  }

  public TezTaskRunner2(Configuration tezConf, UserGroupInformation ugi, String[] localDirs,
                        TaskSpec taskSpec, int appAttemptNumber,
                        Map<String, ByteBuffer> serviceConsumerMetadata,
                        Map<String, String> serviceProviderEnvMap,
                        Multimap<String, String> startedInputsMap,
                        TaskReporterInterface taskReporter, ExecutorService executor,
                        ObjectRegistry objectRegistry, String pid,
                        ExecutionContext executionContext, long memAvailable,
                        boolean updateSysCounters,
                        TezExecutors sharedExecutor) throws
      IOException {
  }

  /**
   * Throws an exception only when there was a communication error reported by
   * the TaskReporter.
   *
   * Otherwise, this takes care of all communication with the AM for a a running task - which
   * includes informing the AM about Failures and Success.
   *
   * If a kill request is made to the task, it will not communicate this information to
   * the AM - since a task KILL is an external event, and whoever invoked it should
   * be able to track it.
   *
   * @return the taskRunner result
   */
  public TaskRunner2Result run() {
    // called from llap-server
    return null;
  }

  /**
   * Attempt to kill the running task, if it hasn't already completed for some other reason.
   * @return true if the task kill was honored, false otherwise
   */
  public boolean killTask() {
    return false;
  }

}
