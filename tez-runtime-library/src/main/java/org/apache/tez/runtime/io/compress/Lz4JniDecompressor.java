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

import at.yawk.lz4.LZ4Compressor;
import at.yawk.lz4.LZ4Factory;
import at.yawk.lz4.LZ4FastDecompressor;
import org.apache.tez.runtime.api.CompressionAlgorithm;

/** Raw lz4-java JNI block decompressor. */
public final class Lz4JniDecompressor implements Decompressor {

  private byte[] compressed = new byte[0];
  private byte[] scratch = new byte[0];
  private LZ4Compressor compressor;
  private LZ4FastDecompressor decompressor;
  private boolean closed;

  public Lz4JniDecompressor() {
    LZ4Factory factory = LZ4Factory.nativeInstance();
    compressor = factory.fastCompressor();
    decompressor = factory.fastDecompressor();
    closed = false;
  }

  @Override
  public CompressionAlgorithm getAlgorithm() {
    return CompressionAlgorithm.LZ4;
  }

  int maximumCompressedLength(int uncompressedLength) throws IOException {
    try {
      return compressor.maxCompressedLength(uncompressedLength);
    } catch (RuntimeException e) {
      throw new IOException("Invalid LZ4 chunk length: " + uncompressedLength, e);
    }
  }

  byte[] ensureCompressedCapacity(int length) {
    assert !closed;
    if (length < 0) {
      throw new IllegalArgumentException("Negative compressed buffer length: " + length);
    }
    if (compressed.length < length) {
      compressed = new byte[length];
    }
    return compressed;
  }

  int decompressBuffered(int inputLength, int outputCapacity) throws IOException {
    assert !closed;
    CompressionStreamUtils.checkRange(compressed, 0, inputLength, "input");
    if (outputCapacity < 0) {
      throw new IllegalArgumentException("Negative output capacity: " + outputCapacity);
    }
    if (scratch.length < outputCapacity) {
      scratch = new byte[outputCapacity];
    }
    try {
      int consumed = decompressor.decompress(compressed, 0, scratch, 0, outputCapacity);
      if (consumed != inputLength) {
        throw new IOException(
            "LZ4 block length mismatch: expected " + inputLength
                + " compressed bytes, consumed " + consumed);
      }
      return outputCapacity;
    } catch (IOException e) {
      throw e;
    } catch (Throwable e) {
      throw new IOException("Invalid or corrupt lz4-java JNI block", e);
    }
  }

  byte[] getDecompressedBuffer() {
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
    compressed = new byte[0];
    scratch = new byte[0];
    compressor = null;
    decompressor = null;
  }
}
