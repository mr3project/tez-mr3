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

package org.apache.tez.runtime.library.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.runtime.api.FetcherConfig;
import org.apache.tez.runtime.api.FetcherConfigCommon;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;

import com.datamonad.mr3.common.security.JobTokenSecretManager;

import javax.crypto.SecretKey;

public final class CodecUtils {

  private CodecUtils() {
  }

  // conf is specific to each RuntimeTask
  public static FetcherConfigCommon constructFetcherConfigCommon(
      Configuration conf, TaskContext taskContext) throws IOException {
    String auxiliaryService = ShuffleUtils.getTezShuffleHandlerServiceId(conf);
    SecretKey shuffleSecret = ShuffleUtils.getJobTokenSecretFromTokenBytes(
        taskContext.getServiceConsumerMetaData(auxiliaryService));
    JobTokenSecretManager jobTokenSecretMgr = new JobTokenSecretManager(shuffleSecret);

    boolean compositeFetch = ShuffleUtils.isTezShuffleHandler(conf);
    HttpConnectionParams httpConnectionParams = ShuffleUtils.getHttpConnectionParams(conf, compositeFetch);

    RawLocalFileSystem localFs = (RawLocalFileSystem) FileSystem.getLocal(conf).getRaw();
    LocalDirAllocator localDirAllocator = new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);
    String localHostName = taskContext.getExecutionContext().getHostName();

    boolean localDiskFetchEnabled = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH,
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_DEFAULT);
    boolean localDiskFetchOrderedEnabled = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_ORDERED,
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_ORDERED_DEFAULT);
    boolean verifyDiskChecksum = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_VERIFY_DISK_CHECKSUM,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_VERIFY_DISK_CHECKSUM_DEFAULT);
    boolean connectionFailAllInput = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_CONNECTION_FAIL_ALL_INPUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_CONNECTION_FAIL_ALL_INPUT_DEFAULT);

    return new FetcherConfigCommon(
        jobTokenSecretMgr,
        httpConnectionParams,
        localFs,
        localDirAllocator,
        localHostName,
        localDiskFetchEnabled,
        localDiskFetchOrderedEnabled,
        verifyDiskChecksum,
        compositeFetch,
        connectionFailAllInput);
  }

  // conf is specific to each RuntimeTask, called from TezContainerWorkerEnv
  public static FetcherConfig constructFetcherConfig(Configuration conf) {
    boolean ifileReadAhead = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD,
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT);
    int ifileReadAheadLength = ifileReadAhead ? conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT) : 0;

    long speculativeExecutionWaitMillis = (long)conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_SPECULATIVE_FETCH_WAIT_MILLIS,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_SPECULATIVE_FETCH_WAIT_MILLIS_DEFAULT);
    int stuckFetcherThresholdMillis = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_STUCK_FETCHER_THRESHOLD_MILLIS,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_STUCK_FETCHER_THRESHOLD_MILLIS_DEFAULT);
    int stuckFetcherReleaseMillis = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_STUCK_FETCHER_RELEASE_MILLIS,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_STUCK_FETCHER_RELEASE_MILLIS_DEFAULT);
    int maxSpeculativeFetchAttempts = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_SPECULATIVE_FETCH_ATTEMPTS,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_SPECULATIVE_FETCH_ATTEMPTS_DEFAULT);

    return new FetcherConfig(
        ifileReadAhead,
        ifileReadAheadLength,
        speculativeExecutionWaitMillis,
        stuckFetcherThresholdMillis,
        stuckFetcherReleaseMillis,
        maxSpeculativeFetchAttempts);
  }

}
