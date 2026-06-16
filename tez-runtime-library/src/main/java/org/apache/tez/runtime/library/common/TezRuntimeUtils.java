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

package org.apache.tez.runtime.library.common;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.http.BaseHttpConnection;
import org.apache.tez.http.HttpConnection;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.http.SSLFactory;
import org.apache.tez.runtime.library.common.security.SecureShuffleUtils;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.partitioner.ValueHashPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.api.TezTaskOutput;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;

public class TezRuntimeUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TezRuntimeUtils.class);

  // Shared by multiple threads
  private static volatile SSLFactory sslFactory;

  public static Partitioner instantiatePartitioner(Configuration conf) throws IOException {
    String className = conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS);

    // BROADCAST_EDGE, ONE_TO_ONE_EDGE: CustomPartitioner (set in UnorderedKVOutput.initialize())
    // CUSTOM_EDGE: HashPartitioner
    // CUSTOM_SIMPLE_EDGE: HashPartitioner
    // XPROD_EDGE: ValueHashPartitioner
    // SIMPLE_EDGE and default: HashPartitioner

    if (HashPartitioner.class.getName().equals(className)) {
      return new HashPartitioner();
    } else if (ValueHashPartitioner.class.getName().equals(className)) {
      return new ValueHashPartitioner();
    } else if (UnorderedKVOutput.CustomPartitioner.class.getName().equals(className)) {
      return new UnorderedKVOutput.CustomPartitioner();
    } else {
      throw new TezUncheckedException("Unsupported Partitioner class: " + className);
    }
  }

  public static TezTaskOutput instantiateTaskOutputManager(
      Configuration conf, OutputContext outputContext,
      boolean isCompositeFetch) {
    return new TezTaskOutputFiles(conf,
        outputContext.getUniqueIdentifierForOutputFiles(),
        outputContext.getDagIdentifier(),
        outputContext.getExecutionContext().getEnvContainerId(),
        outputContext.getTaskVertexIndex(),
        isCompositeFetch);
  }

  public static HttpConnectionParams getHttpConnectionParams(
      Configuration conf, boolean compositeFetch) {
    int connectionTimeout = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_STALLED_COPY_TIMEOUT_DEFAULT);

    int readTimeout = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT_DEFAULT);

    int bufferSize = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE_DEFAULT);

    boolean keepAlive = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_ENABLED,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_ENABLED_DEFAULT);

    int keepAliveMaxConnections = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_MAX_CONNECTIONS,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_MAX_CONNECTIONS_DEFAULT);

    if (keepAlive) {
      System.setProperty("sun.net.http.errorstream.enableBuffering", "true");
      System.setProperty("http.maxConnections", String.valueOf(keepAliveMaxConnections));
    }

    boolean sslShuffle = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL_DEFAULT);
    if (sslShuffle) {
      if (sslFactory == null) {
        synchronized (HttpConnectionParams.class) {
          //Create sslFactory if it is null or if it was destroyed earlier
          if (sslFactory == null || sslFactory.getKeystoresFactory().getTrustManagers() == null) {
            sslFactory =
                new SSLFactory(org.apache.hadoop.security.ssl.SSLFactory.Mode.CLIENT, conf);
            try {
              sslFactory.init();
            } catch (Exception ex) {
              sslFactory.destroy();
              sslFactory = null;
              throw new RuntimeException(ex);
            }
          }
        }
      }
    }

    // skipVerifyRequest is false if compositeFetch == false, i.e, when using mapreduce_shuffle
    boolean skipVerifyRequest = compositeFetch && conf.getBoolean(
        SecureShuffleUtils.SHUFFLE_SKIP_VERIFY_REQUEST, SecureShuffleUtils.SHUFFLE_SKIP_VERIFY_REQUEST_DEFAULT);

    return new HttpConnectionParams(keepAlive,
        keepAliveMaxConnections, connectionTimeout, readTimeout, bufferSize,
        sslShuffle, sslFactory, skipVerifyRequest);
  }

  public static BaseHttpConnection getHttpConnection(
      URL url, HttpConnectionParams params, String logIdentifier, JobTokenSecretManager jobTokenSecretManager) {
    return new HttpConnection(url, params, logIdentifier, jobTokenSecretManager);
  }

  public static int[] deserializeShuffleProviderMetaData(ByteBuffer meta)
      throws IOException {
    DataInputByteBuffer in = new DataInputByteBuffer();
    try {
      in.reset(meta);
      int numPorts = in.getLength() / 4;
      int[] ports = new int[numPorts];
      for (int i = 0; i < numPorts; i++) {
        int port = in.readInt();
        ports[i] = port;
      }
      return ports;
    } finally {
      in.close();
    }
  }
}
