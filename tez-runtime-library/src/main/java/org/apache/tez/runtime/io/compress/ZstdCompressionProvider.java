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

import org.apache.tez.runtime.api.CompressionAlgorithm;
import org.apache.tez.runtime.api.CompressionProvider;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class ZstdCompressionProvider implements CompressionProvider {

  // Hadoop uses zero to request the codec's recommended native buffer size.
  private static final int RECOMMENDED_BUFFER_SIZE = 128 * 1024;

  private final int bufferSize;
  private final int compressionLevel;

  public ZstdCompressionProvider(int bufferSize, int compressionLevel) {
    this.bufferSize = bufferSize == 0 ? RECOMMENDED_BUFFER_SIZE : bufferSize;
    this.compressionLevel = compressionLevel;
  }

  @Override
  public CompressionAlgorithm getAlgorithm() {
    return CompressionAlgorithm.ZSTD;
  }

  @Override
  public int getBufferSize() {
    return bufferSize;
  }

  @Override
  public Compressor createCompressor() {
    return new ZstdJniCompressor(compressionLevel);
  }

  @Override
  public Decompressor createDecompressor() {
    return new ZstdJniDecompressor();
  }

  @Override
  public CompressionOutputStream createOutputStream(
      OutputStream output, Compressor compressor) throws IOException {
    assert compressor.getAlgorithm() == CompressionAlgorithm.ZSTD;
    assert compressor instanceof ZstdJniCompressor;

    return new ZstdCompressionOutputStream(
        output, (ZstdJniCompressor) compressor, bufferSize);
  }

  @Override
  public InputStream createInputStream(
      InputStream input, Decompressor decompressor) throws IOException {
    assert decompressor.getAlgorithm() == CompressionAlgorithm.ZSTD;
    assert decompressor instanceof ZstdJniDecompressor;

    return new ZstdCompressionInputStream(
        input, (ZstdJniDecompressor) decompressor, bufferSize);
  }
}
