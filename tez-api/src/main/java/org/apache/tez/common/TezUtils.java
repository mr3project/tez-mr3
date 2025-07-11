/*
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

package org.apache.tez.common;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.protobuf.ByteString;

import com.google.protobuf.UnsafeByteOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.records.DAGProtos;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

/**
 * Utility methods for setting up a DAG. Has helpers for setting up log4j configuration, converting
 * {@link org.apache.hadoop.conf.Configuration} to {@link org.apache.tez.dag.api.UserPayload} etc.
 */
@InterfaceAudience.Public
public class TezUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TezUtils.class);

  /**
   * Allows changing the log level for task / AM logging. </p>
   *
   * Adds the JVM system properties necessary to configure
   * {@link org.apache.hadoop.yarn.ContainerLogAppender}.
   *
   * @param logLevel the desired log level (eg INFO/WARN/DEBUG)
   * @param vargs    the argument list to append to
   */
  public static void addLog4jSystemProperties(String logLevel,
                                              List<String> vargs) {
    TezClientUtils.addLog4jSystemProperties(logLevel, vargs);
  }

  /**
   * Convert a Configuration to compressed ByteString using Protocol buffer
   *
   * @param conf
   *          : Configuration to be converted
   * @return PB ByteString (compressed)
   * @throws java.io.IOException
   */
  public static ByteString createByteStringFromConf(Configuration conf) throws IOException {
    Objects.requireNonNull(conf, "Configuration must be specified");
    ByteString.Output os = ByteString.newOutput();
    SnappyOutputStream compressOs = new SnappyOutputStream(os);
    try {
      writeConfInPB(compressOs, conf);
    } finally {
      if (compressOs != null) {
        compressOs.close();
      }
    }
    return os.toByteString();
  }

  /**
   * Convert a Configuration to a {@link org.apache.tez.dag.api.UserPayload} </p>
   *
   *
   * @param conf configuration to be converted
   * @return an instance of {@link org.apache.tez.dag.api.UserPayload}
   * @throws java.io.IOException
   */
  public static UserPayload createUserPayloadFromConf(Configuration conf) throws IOException {
    return UserPayload.create(ByteBuffer.wrap(createByteStringFromConf(conf).toByteArray()));
  }

  /**
   * Convert a byte string to a Configuration object
   *
   * @param byteString byteString representation of the conf created using {@link
   *                   #createByteStringFromConf(org.apache.hadoop.conf.Configuration)}
   * @return Configuration
   * @throws java.io.IOException
   */
  public static Configuration createConfFromByteString(ByteString byteString) throws IOException {
    Objects.requireNonNull(byteString, "ByteString must be specified");
    try(SnappyInputStream uncompressIs = new SnappyInputStream(byteString.newInput())) {
      DAGProtos.ConfigurationProto confProto = DAGProtos.ConfigurationProto.parseFrom(uncompressIs);
      Configuration conf = new Configuration(false);
      readConfFromPB(confProto, conf);
      return conf;
    }
  }

  /**
   * Convert an instance of {@link org.apache.tez.dag.api.UserPayload} to {@link
   * org.apache.hadoop.conf.Configuration}
   *
   * @param payload {@link org.apache.tez.dag.api.UserPayload} created using {@link
   *                #createUserPayloadFromConf(org.apache.hadoop.conf.Configuration)}
   * @return Configuration
   * @throws java.io.IOException
   */
  public static Configuration createConfFromUserPayload(UserPayload payload) throws IOException {
    return createConfFromByteString(UnsafeByteOperations.unsafeWrap(payload.getPayload()));
  }


  private static void writeConfInPB(OutputStream dos, Configuration conf) throws IOException {
    DAGProtos.ConfigurationProto.Builder confProtoBuilder = DAGProtos.ConfigurationProto.newBuilder();
    populateConfProtoFromEntriesWithVarExpansion(conf, confProtoBuilder);
    DAGProtos.ConfigurationProto confProto = confProtoBuilder.build();
    confProto.writeTo(dos);
  }

  private static void readConfFromPB(DAGProtos.ConfigurationProto confProto, Configuration conf) {
    List<DAGProtos.PlanKeyValuePair> settingList = confProto.getConfKeyValuesList();
    for (DAGProtos.PlanKeyValuePair setting : settingList) {
      conf.set(setting.getKey(), setting.getValue());
    }
  }

  private static void populateConfProtoFromEntriesWithVarExpansion(Configuration conf,
      DAGProtos.ConfigurationProto.Builder confBuilder) {
    for(Map.Entry<String, String> entry : conf) {
      String key = entry.getKey();
      String val = conf.get(key);   // automatically expands variables
      if(val != null) {
        DAGProtos.PlanKeyValuePair.Builder kvp = DAGProtos.PlanKeyValuePair.newBuilder();
        kvp.setKey(key);
        kvp.setValue(val);
        confBuilder.addConfKeyValues(kvp);
      } else {
        LOG.debug("null value for key={}. Skipping.", key);
      }
    }
  }
}
