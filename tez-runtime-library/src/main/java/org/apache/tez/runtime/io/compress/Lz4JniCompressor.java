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

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.tez.runtime.api.CompressionAlgorithm;

/** Raw lz4-java JNI block compressor. A scratch buffer prevents partial caller output. */
public final class Lz4JniCompressor implements Compressor {

  private byte[] input;
  private byte[] scratch;
  private LZ4Compressor compressor;
  private boolean closed;

  Lz4JniCompressor(LZ4Factory factory, boolean useHighCompression, int bufferSize) {
    assert bufferSize > 0;
    assert bufferSize <= BlockCompressionOutputStream.MAX_BLOCK_SIZE;
    compressor = useHighCompression ? factory.highCompressor() : factory.fastCompressor();
    input = new byte[bufferSize];
    scratch = new byte[maxCompressedLength(bufferSize)];
    closed = false;
  }

  @Override
  public CompressionAlgorithm getAlgorithm() {
    return CompressionAlgorithm.LZ4;
  }

  private int maxCompressedLength(int uncompressedLength) {
    assert !closed;
    assert uncompressedLength >= 0;
    int maximum = compressor.maxCompressedLength(uncompressedLength);
    assert maximum >= uncompressedLength;
    return maximum;
  }

  byte[] ensureInputCapacity(int length) {
    assert !closed;
    assert length > 0;
    assert input.length >= length;
    return input;
  }

  int compressBuffered(int inputLength) throws IOException {
    assert !closed;
    CompressionStreamUtils.checkRange(input, 0, inputLength, "input");
    int maximum = maxCompressedLength(inputLength);
    assert scratch.length >= maximum;
    try {
      return compressor.compress(input, 0, inputLength, scratch, 0, scratch.length);
    } catch (Throwable e) {
      throw new IOException("lz4-java JNI compression failed", e);
    }
  }

  byte[] getCompressedBuffer() {
    assert !closed;
    return scratch;
  }

  @Override
  public void reset() {
    assert !closed;
  }

  @Override
  public void close() {
    closed = true;
    input = null;
    scratch = null;
    compressor = null;
  }
}
