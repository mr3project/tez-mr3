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

/** Placeholder until the Tez ZStandard implementation is available. */
public final class DummyZstdCompressionProvider implements CompressionProvider {
  private final int bufferSize;

  public DummyZstdCompressionProvider(int bufferSize) {
    this.bufferSize = bufferSize;
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
  public Compressor createCompressor() throws IOException {
    throw unavailable();
  }

  @Override
  public Decompressor createDecompressor() throws IOException {
    throw unavailable();
  }

  @Override
  public CompressionOutputStream createOutputStream(
      OutputStream output, Compressor compressor) throws IOException {
    throw unavailable();
  }

  @Override
  public InputStream createInputStream(
      InputStream input, Decompressor decompressor) throws IOException {
    throw unavailable();
  }

  private IOException unavailable() {
    return new IOException("ZStandard IFile compression is not implemented");
  }
}
