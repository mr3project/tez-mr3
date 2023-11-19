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

package org.apache.tez.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;

import com.google.protobuf.ByteString;

@Private
public class TezCommonUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TezClient.class);

  public static void addAdditionalLocalResources(Map<String, LocalResource> additionalLrs,
      Map<String, LocalResource> originalLRs, String logContext) {
    // TODO TEZ-1798. Handle contents of Tez archives for duplicate LocalResource checks
    if (additionalLrs != null && !additionalLrs.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<String, LocalResource> lrEntry : additionalLrs.entrySet()) {
        LocalResource originalLr = originalLRs.get(lrEntry.getKey());
        if (originalLr != null) {
          LocalResource additionalLr = lrEntry.getValue();
          if (originalLr.getSize() != additionalLr.getSize()) {
            throw new TezUncheckedException(
                "Duplicate Resources found with different size for [" + logContext + "]: " + lrEntry.getKey() +
                    " : " + "[" + additionalLr.getResource() + "=" + additionalLr.getSize() +
                    "],[" + originalLr.getResource() + "=" + originalLr.getSize());
          } else {
            if (originalLr.getResource().equals(additionalLr.getResource())) {
              sb.append("[").append(lrEntry.getKey()).append(" : Duplicate]");
            } else {
              sb.append("[").append(lrEntry.getKey()).append(" : DuplicateDifferentPath]");
            }
          }
        }
        // The LR either does not exist, or is an 'equivalent' dupe.
        // Prefer the tez specified LR instead of the equivalent user specified LR for container reuse matching
        originalLRs.put(lrEntry.getKey(), lrEntry.getValue());
      }
      String logString = sb.toString();
      if (!logString.isEmpty()) {
        LOG.warn("Found Resources Duplication in " + logContext + " after including resources from " +
            TezConfiguration.TEZ_LIB_URIS + " and " + TezConfiguration.TEZ_AUX_URIS + ": " +
            logString);
      }
    }
  }

  private static final boolean NO_WRAP = true;

  @Private
  public static Deflater newBestCompressionDeflater() {
    return new Deflater(Deflater.BEST_COMPRESSION, NO_WRAP);
  }

  @Private
  public static Deflater newBestSpeedDeflater() {
    return new Deflater(Deflater.BEST_SPEED, NO_WRAP);
  }

  @Private
  public static Inflater newInflater() {
    return new Inflater(NO_WRAP);
  }

  @Private
  public static ByteString compressByteArrayToByteString(byte[] inBytes) throws IOException {
    return compressByteArrayToByteString(inBytes, newBestCompressionDeflater());
  }

  @Private
  public static ByteString compressByteArrayToByteString(byte[] inBytes, Deflater deflater) throws IOException {
    deflater.reset();
    ByteString.Output os = ByteString.newOutput();
    DeflaterOutputStream compressOs = null;
    try {
      compressOs = new DeflaterOutputStream(os, deflater);
      compressOs.write(inBytes);
      compressOs.finish();
      ByteString byteString = os.toByteString();
      return byteString;
    } finally {
      if (compressOs != null) {
        compressOs.close();
      }
    }
  }

  @Private
  public static byte[] decompressByteStringToByteArray(ByteString byteString) throws IOException {
    Inflater inflater = newInflater();
    try {
      return decompressByteStringToByteArray(byteString, inflater);
    } finally {
      inflater.end();
    }
  }

  @Private
  public static byte[] decompressByteStringToByteArray(ByteString byteString, Inflater inflater) throws IOException {
    inflater.reset();
    try (InflaterInputStream inflaterInputStream = new InflaterInputStream(byteString.newInput(), inflater)) {
      return IOUtils.toByteArray(inflaterInputStream);
    }
  }

  // called from hive.llap.daemon.impl.TaskRunnerCallable
  public static ByteBuffer convertJobTokenToBytes(
      Token<JobTokenIdentifier> jobToken) throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    jobToken.write(dob);
    ByteBuffer bb = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    return bb;
  }

  /**
   * Splits a comma separated value <code>String</code>, trimming leading and trailing whitespace on each value.
   * @param str a comma separated <String> with values
   * @return an array of <code>String</code> values
   */
  public static String[] getTrimmedStrings(String str) {
    if (null == str || (str = str.trim()).isEmpty()) {
      return ArrayUtils.EMPTY_STRING_ARRAY;
    }

    return str.split("\\s*,\\s*");
  }
}
