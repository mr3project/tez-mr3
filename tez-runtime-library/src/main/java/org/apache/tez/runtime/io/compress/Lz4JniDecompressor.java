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
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.tez.runtime.api.CompressionAlgorithm;

/** Raw lz4-java JNI block decompressor. */
final class Lz4JniDecompressor implements Decompressor {

  private byte[] compressed;
  private byte[] scratch;
  private final int maxBufferSize;
  private final LZ4Compressor compressor;
  private LZ4FastDecompressor decompressor;
  private boolean closed;

  Lz4JniDecompressor(
      LZ4Factory factory, int maxBufferSize, int maxCompressedLength, boolean perDag) {
    compressed = new byte[perDag ? 0 : maxCompressedLength];
    scratch = new byte[perDag ? 0 : maxBufferSize];
    this.maxBufferSize = maxBufferSize;
    compressor = factory.fastCompressor();
    decompressor = factory.fastDecompressor();
    closed = false;
  }

  @Override
  public CompressionAlgorithm getAlgorithm() {
    return CompressionAlgorithm.LZ4;
  }

  byte[] ensureCapacity(int compressedLength, int rawLength) {
    assert !closed;
    if (rawLength <= scratch.length) {
      return compressed;
    }

    int newScratchLength = CompressionStreamUtils.nextBufferSize(
        scratch.length, rawLength, maxBufferSize);
    if (newScratchLength != scratch.length) {
      int newCompressedLength = compressor.maxCompressedLength(newScratchLength);
      assert compressedLength <= newCompressedLength;   // because rawLength < scratch.length
      scratch = new byte[newScratchLength];
      compressed = new byte[newCompressedLength];
    }
    return compressed;
  }

  int decompressBuffered(int inputLength, int outputCapacity) throws IOException {
    assert !closed;
    CompressionStreamUtils.checkRange(compressed, 0, inputLength, "input");
    assert outputCapacity >= 0;
    assert scratch.length >= outputCapacity;
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
    compressed = null;
    scratch = null;
    decompressor = null;
  }
}
