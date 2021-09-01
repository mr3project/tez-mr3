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
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.dag.api.*;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * TezClient is used to submit Tez DAGs for execution. DAG's are executed via a
 * Tez App Master. TezClient can run the App Master in session or non-session
 * mode. <br>
 * In non-session mode, each DAG is executed in a different App Master that
 * exits after the DAG execution completes. <br>
 * In session mode, the TezClient creates a single instance of the App Master
 * and all DAG's are submitted to the same App Master.<br>
 * Session mode may give better performance when a series of DAGs need to
 * executed because it enables resource re-use across those DAGs. Non-session
 * mode should be used when the user wants to submit a single DAG or wants to
 * disconnect from the cluster after submitting a set of unrelated DAGs. <br>
 * If API recommendations are followed, then the choice of running in session or
 * non-session mode is transparent to writing the application. By changing the
 * session mode configuration, the same application can be running in session or
 * non-session mode.
 */
@Public
public class TezClient {

  private static final String appIdStrPrefix = "application";
  private static final String APPLICATION_ID_PREFIX = appIdStrPrefix + '_';
  
  @VisibleForTesting
  static final String NO_CLUSTER_DIAGNOSTICS_MSG = "No cluster diagnostics found.";

  private TezClient(String name, TezConfiguration tezConf) {
    throw new TezUncheckedException("TezClient not supported");
  }

  @Private
  TezClient(String name, TezConfiguration tezConf,
            @Nullable Map<String, LocalResource> localResources,
            @Nullable Credentials credentials) {
    throw new TezUncheckedException("TezClient not supported");
  }

  private TezClient(String name, TezConfiguration tezConf, boolean isSession) {
    throw new TezUncheckedException("TezClient not supported");
  }

  @Private
  protected TezClient(String name, TezConfiguration tezConf, boolean isSession,
                      @Nullable Map<String, LocalResource> localResources,
                      @Nullable Credentials credentials) {
    throw new TezUncheckedException("TezClient not supported");
  }

  @Private
  protected TezClient(String name, TezConfiguration tezConf, boolean isSession,
            @Nullable Map<String, LocalResource> localResources,
            @Nullable Credentials credentials, ServicePluginsDescriptor servicePluginsDescriptor) {
    throw new TezUncheckedException("TezClient not supported");
  }

  public static TezClientBuilder newBuilder(String name, TezConfiguration tezConf) {
    throw new TezUncheckedException("TezClient not supported");
  }

  public static TezClient create(String name, TezConfiguration tezConf) {
    throw new TezUncheckedException("TezClient not supported");
  }

  public static TezClient create(String name, TezConfiguration tezConf,
                                 @Nullable Map<String, LocalResource> localFiles,
                                 @Nullable Credentials credentials) {
    throw new TezUncheckedException("TezClient not supported");
  }

  public static TezClient create(String name, TezConfiguration tezConf, boolean isSession) {
    throw new TezUncheckedException("TezClient not supported");
  }

  public static TezClient create(String name, TezConfiguration tezConf, boolean isSession,
                                 @Nullable Map<String, LocalResource> localFiles,
                                 @Nullable Credentials credentials) {
    throw new TezUncheckedException("TezClient not supported");
  }

  public synchronized void addAppMasterLocalFiles(Map<String, LocalResource> localFiles) {
    throw new TezUncheckedException("TezClient not supported");
  }
  
  public synchronized void clearAppMasterLocalFiles() {
    throw new TezUncheckedException("TezClient not supported");
  }
  
  public synchronized void setAppMasterCredentials(Credentials credentials) {
    throw new TezUncheckedException("TezClient not supported");
  }

  public synchronized void start() throws TezException, IOException {
    throw new TezUncheckedException("TezClient not supported");
  }

  public synchronized TezClient getClient(String appIdStr) throws IOException, TezException {
    throw new TezUncheckedException("TezClient not supported");
  }

  public synchronized TezClient getClient(ApplicationId appId) throws TezException, IOException {
    throw new TezUncheckedException("TezClient not supported");
  }

  public synchronized DAGClient submitDAG(DAG dag) throws TezException, IOException {
    throw new TezUncheckedException("TezClient not supported");
  }

  public synchronized void stop() throws TezException, IOException {
    throw new TezUncheckedException("TezClient not supported");
  }

  public String getClientName() {
    throw new TezUncheckedException("TezClient not supported");
  }
  
  public synchronized ApplicationId getAppMasterApplicationId() {
    throw new TezUncheckedException("TezClient not supported");
  }

  public synchronized TezAppMasterStatus getAppMasterStatus() throws TezException, IOException {
    throw new TezUncheckedException("TezClient not supported");
  }
  
  public synchronized void preWarm(PreWarmVertex preWarmVertex) throws TezException, IOException {
    throw new TezUncheckedException("TezClient not supported");
  }

  public synchronized void preWarm(PreWarmVertex preWarmVertex,
      long timeout, TimeUnit unit)
      throws TezException, IOException {
    throw new TezUncheckedException("TezClient not supported");
  }

  
  public synchronized void waitTillReady() throws IOException, TezException, InterruptedException {
    throw new TezUncheckedException("TezClient not supported");
  }

  public synchronized boolean waitTillReady(long timeout, TimeUnit unit)
      throws IOException, TezException, InterruptedException {
    throw new TezUncheckedException("TezClient not supported");
  }

  /**
   * A builder for setting up an instance of {@link org.apache.tez.client.TezClient}
   */
  @Public
  public static class TezClientBuilder {

    private TezClientBuilder(String name, TezConfiguration tezConf) {
      throw new TezUncheckedException("TezClient not supported");
    }

    public TezClientBuilder setIsSession(boolean isSession) {
      throw new TezUncheckedException("TezClient not supported");
    }

    public TezClientBuilder setLocalResources(Map<String, LocalResource> localResources) {
      throw new TezUncheckedException("TezClient not supported");
    }

    public TezClientBuilder setCredentials(Credentials credentials) {
      throw new TezUncheckedException("TezClient not supported");
    }

    public TezClientBuilder setServicePluginDescriptor(ServicePluginsDescriptor servicePluginsDescriptor) {
      throw new TezUncheckedException("TezClient not supported");
    }

    public TezClient build() {
      throw new TezUncheckedException("TezClient not supported");
    }
  }

  //Copied this helper method from 
  //org.apache.hadoop.yarn.api.records.ApplicationId in Hadoop 2.8+
  //to simplify implementation on 2.7.x
  @Public
  @Unstable
  public static ApplicationId appIdfromString(String appIdStr) {
    if (!appIdStr.startsWith(APPLICATION_ID_PREFIX)) {
      throw new IllegalArgumentException("Invalid ApplicationId prefix: "
              + appIdStr + ". The valid ApplicationId should start with prefix "
              + appIdStrPrefix);
    }
    try {
      int pos1 = APPLICATION_ID_PREFIX.length() - 1;
      int pos2 = appIdStr.indexOf('_', pos1 + 1);
      if (pos2 < 0) {
        throw new IllegalArgumentException("Invalid ApplicationId: "
                + appIdStr);
      }
      long rmId = Long.parseLong(appIdStr.substring(pos1 + 1, pos2));
      int appId = Integer.parseInt(appIdStr.substring(pos2 + 1));
      ApplicationId applicationId = ApplicationId.newInstance(rmId, appId);
      return applicationId;
    } catch (NumberFormatException n) {
      throw new IllegalArgumentException("Invalid ApplicationId: "
              + appIdStr, n);
    }
  }
}
