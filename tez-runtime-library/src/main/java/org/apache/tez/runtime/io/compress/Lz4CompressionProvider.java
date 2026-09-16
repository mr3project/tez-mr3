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
import java.io.InputStream;
import java.io.OutputStream;

import net.jpountz.lz4.LZ4Factory;
import org.apache.tez.runtime.api.CompressionAlgorithm;
import org.apache.tez.runtime.api.CompressionProvider;

public final class Lz4CompressionProvider implements CompressionProvider {

  private final int maxBufferSize;
  private final boolean useHighCompression;
  private final LZ4Factory lz4Factory;
  private final int maxCompressedLength;

  public Lz4CompressionProvider(int maxBufferSize, boolean useHighCompression, boolean useNativeInstance) {
    this.maxBufferSize = maxBufferSize;
    this.useHighCompression = useHighCompression;
    if (useNativeInstance) {
      this.lz4Factory = LZ4Factory.nativeInstance();
    } else {
      this.lz4Factory = LZ4Factory.fastestJavaInstance();
    }
    this.maxCompressedLength = lz4Factory.fastCompressor().maxCompressedLength(maxBufferSize);
    if (maxCompressedLength < maxBufferSize) {
      throw new IllegalStateException("Invalid LZ4 maximum compressed length: "
          + maxCompressedLength + " for buffer size " + maxBufferSize);
    }
  }

  @Override
  public CompressionAlgorithm getAlgorithm() {
    return CompressionAlgorithm.LZ4;
  }

  @Override
  public int getMaxBufferSize() {
    return maxBufferSize;
  }

  @Override
  public Compressor createCompressor() {
    return new Lz4JniCompressor(
        lz4Factory, useHighCompression, maxBufferSize, maxCompressedLength);
  }

  @Override
  public Decompressor createDecompressor() {
    return new Lz4JniDecompressor(lz4Factory, maxBufferSize, maxCompressedLength, false);
  }

  @Override
  public Decompressor createDecompressorPerDag() {
    return new Lz4JniDecompressor(lz4Factory, maxBufferSize, maxCompressedLength, true);
  }

  @Override
  public CompressionOutputStream createOutputStream(
      OutputStream output, Compressor compressor) throws IOException {
    assert compressor.getAlgorithm() == CompressionAlgorithm.LZ4;

    return new Lz4CompressionOutputStream(
        output, (Lz4JniCompressor) compressor, maxBufferSize);
  }

  @Override
  public InputStream createInputStream(
      InputStream input, Decompressor decompressor) throws IOException {
    assert decompressor.getAlgorithm() == CompressionAlgorithm.LZ4;

    return new Lz4CompressionInputStream(
        input, (Lz4JniDecompressor) decompressor, maxBufferSize, maxCompressedLength);
  }
}
