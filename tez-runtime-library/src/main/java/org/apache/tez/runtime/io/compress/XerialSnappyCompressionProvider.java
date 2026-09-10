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

/** Provider for the Tez chunk-framed xerial Snappy stream format. */
public final class XerialSnappyCompressionProvider implements CompressionProvider {
  @Override
  public CompressionAlgorithm getAlgorithm() {
    return CompressionAlgorithm.SNAPPY;
  }

  @Override
  public Compressor createCompressor() {
    return new XerialSnappyCompressor();
  }

  @Override
  public Decompressor createDecompressor() {
    return new XerialSnappyDecompressor();
  }

  @Override
  public CompressionOutputStream createOutputStream(
      OutputStream output, Compressor compressor, int bufferSize) throws IOException {
    requireAlgorithm(compressor.getAlgorithm());
    if (!(compressor instanceof XerialSnappyCompressor)) {
      throw new IOException("Expected " + XerialSnappyCompressor.class.getName()
          + ", got " + compressor.getClass().getName());
    }
    return new SnappyCompressionOutputStream(
        output, (XerialSnappyCompressor) compressor, bufferSize);
  }

  @Override
  public InputStream createInputStream(
      InputStream input, Decompressor decompressor, int bufferSize) throws IOException {
    requireAlgorithm(decompressor.getAlgorithm());
    if (!(decompressor instanceof XerialSnappyDecompressor)) {
      throw new IOException("Expected " + XerialSnappyDecompressor.class.getName()
          + ", got " + decompressor.getClass().getName());
    }
    return new SnappyCompressionInputStream(
        input, (XerialSnappyDecompressor) decompressor, bufferSize);
  }

  private void requireAlgorithm(CompressionAlgorithm algorithm) throws IOException {
    if (algorithm != getAlgorithm()) {
      throw new IOException("Expected " + getAlgorithm() + " object, got " + algorithm);
    }
  }
}
