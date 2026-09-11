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

import org.apache.hadoop.io.compress.CompressionCodec;

/** Central codec-name to compression provider resolver */
public final class CompressionResolver {

  public static final int DEFAULT_BUFFER_SIZE = 256 * 1024;

  private static final String SNAPPY_CODEC = "org.apache.hadoop.io.compress.SnappyCodec";
  private static final String ZSTD_CODEC = "org.apache.hadoop.io.compress.ZStandardCodec";

  private static final CompressionProvider SNAPPY_PROVIDER = new XerialSnappyCompressionProvider();

  private CompressionResolver() {
  }

  public static CompressionAlgorithm resolveAlgorithm(CompressionCodec codec) throws IOException {
    if (codec == null) {
      throw new IOException("Cannot resolve compression algorithm for a null codec");
    }
    return resolveAlgorithm(codec.getClass().getName());
  }

  public static CompressionAlgorithm resolveAlgorithm(String codecClassName) throws IOException {
    if (SNAPPY_CODEC.equals(codecClassName)) {
      return CompressionAlgorithm.SNAPPY;
    }
    if (ZSTD_CODEC.equals(codecClassName)) {
      return CompressionAlgorithm.ZSTD;
    }
    throw new IOException("Unsupported IFile compression codec: " + codecClassName);
  }

  public static CompressionProvider getProvider(CompressionAlgorithm algorithm) throws IOException {
    if (algorithm == CompressionAlgorithm.SNAPPY) {
      return SNAPPY_PROVIDER;
    }
    throw new IOException("Tez IFile compression algorithm is recognized but has no provider: "
        + algorithm);
  }

  public static CompressionProvider getProvider(CompressionCodec codec) throws IOException {
    return getProvider(resolveAlgorithm(codec));
  }
}
