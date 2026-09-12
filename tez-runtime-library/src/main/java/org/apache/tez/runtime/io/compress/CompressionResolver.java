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
package org.apache.tez.runtime.io.compress;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.tez.runtime.api.CompressionAlgorithm;
import org.apache.tez.runtime.api.CompressionProvider;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public final class CompressionResolver {

  private static final Logger LOG = LoggerFactory.getLogger(CompressionResolver.class);

  private static final String SNAPPY_CODEC = "org.apache.hadoop.io.compress.SnappyCodec";
  private static final String ZSTD_CODEC = "org.apache.hadoop.io.compress.ZStandardCodec";

  // Because all producers and consumers use the same buffer size,
  // the uncompressed array is bounded by that configured size, and
  // the compressed array is bounded by the codec’s maximum compressed size for that configured size.
  // Hence, there is no unbounded or configuration-independent memory consumption by compressors/decompressors.

  @Nullable
  public static CompressionProvider createProvider(Configuration conf) throws IOException {
    if (!conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, false)) {
      return null;
    }

    String codecClassName = conf.getTrimmed(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
    if (codecClassName == null || codecClassName.isEmpty()) {
      throw new IOException(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC
          + " must be set when intermediate compression is enabled");
    }

    CompressionAlgorithm algorithm = resolveAlgorithm(codecClassName);
    if (algorithm == CompressionAlgorithm.SNAPPY) {
      int bufferSize = conf.getInt(
          CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY,
          CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT);
      LOG.info("Using codec {}, buffer size = {}", algorithm, bufferSize);
      return new XerialSnappyCompressionProvider(bufferSize);
    }

    if (algorithm == CompressionAlgorithm.ZSTD) {
      int bufferSize = conf.getInt(
          CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_KEY,
          CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_DEFAULT);
     int compressionLevel = conf.getInt(
         CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_LEVEL_KEY,
         CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_LEVEL_DEFAULT);
      LOG.info("Using codec {}, buffer size = {}, compression level = {}", algorithm, bufferSize, compressionLevel);
      return new ZstdCompressionProvider(bufferSize, compressionLevel);
    }

    return null;  // no compression
  }

  private static CompressionAlgorithm resolveAlgorithm(String codecClassName) throws IOException {
    if (SNAPPY_CODEC.equals(codecClassName)) {
      return CompressionAlgorithm.SNAPPY;
    }
    if (ZSTD_CODEC.equals(codecClassName)) {
      return CompressionAlgorithm.ZSTD;
    }
    throw new IOException("Unsupported IFile compression codec: " + codecClassName);
  }
}
