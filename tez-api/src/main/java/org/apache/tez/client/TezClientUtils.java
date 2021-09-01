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

package org.apache.tez.client;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.common.JavaOptsChecker;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

@Private
public class TezClientUtils {

  private static Logger LOG = LoggerFactory.getLogger(TezClientUtils.class);

  @Private
  public static String addDefaultsToTaskLaunchCmdOpts(String vOpts, Configuration conf,
      JavaOptsChecker javaOptsChecker) throws TezException {
    String vConfigOpts = "";
    String taskDefaultOpts = conf.get(TezConfiguration.TEZ_TASK_LAUNCH_CLUSTER_DEFAULT_CMD_OPTS,
        TezConfiguration.TEZ_TASK_LAUNCH_CLUSTER_DEFAULT_CMD_OPTS_DEFAULT);
    if (taskDefaultOpts != null && !taskDefaultOpts.isEmpty()) {
      vConfigOpts = taskDefaultOpts + " ";
    }
    String defaultTaskCmdOpts = TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS_DEFAULT;
    if (vOpts != null && !vOpts.isEmpty()) {
      // Only use defaults if nothing is specified by the user
      defaultTaskCmdOpts = "";
    }

    vConfigOpts = vConfigOpts + conf.get(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS,
        defaultTaskCmdOpts);
    if (vConfigOpts != null && !vConfigOpts.isEmpty()) {
      // Add options specified in the DAG at the end.
      vOpts = vConfigOpts + " " + vOpts;
    }

    if (javaOptsChecker != null) {
      javaOptsChecker.checkOpts(vOpts);
    }

    return vOpts;
  }

  @Private
  @VisibleForTesting
  public static void addLog4jSystemProperties(String logLevel,
      List<String> vargs) {
    vargs.add("-Dlog4j.configuratorClass=org.apache.tez.common.TezLog4jConfigurator");
    vargs.add("-Dlog4j.configuration="
        + TezConstants.TEZ_CONTAINER_LOG4J_PROPERTIES_FILE);
    vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "="
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    vargs.add("-D" + TezConstants.TEZ_ROOT_LOGGER_NAME + "=" + logLevel
        + "," + TezConstants.TEZ_CONTAINER_LOGGER_NAME);
  }

  public static byte[] getResourceSha(URI uri, Configuration conf) throws IOException {
    InputStream is = null;
    try {
      is = FileSystem.get(uri, conf).open(new Path(uri));
      return DigestUtils.sha256(is);
    } finally {
      if (is != null) {
        is.close();
      }
    }
  }
}
