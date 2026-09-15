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

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.tez.runtime.api.CompressionAlgorithm;

/** Raw lz4-java JNI block decompressor. */
public final class Lz4JniDecompressor implements Decompressor {

  private byte[] compressed;
  private byte[] scratch;
  private LZ4FastDecompressor decompressor;
  private boolean closed;

  Lz4JniDecompressor(LZ4Factory factory, int bufferSize, int maxCompressedLength) {
    assert bufferSize > 0;
    assert bufferSize <= BlockCompressionOutputStream.MAX_BLOCK_SIZE;
    assert maxCompressedLength >= bufferSize;
    compressed = new byte[maxCompressedLength];
    scratch = new byte[bufferSize];
    decompressor = factory.fastDecompressor();
    closed = false;
  }

  @Override
  public CompressionAlgorithm getAlgorithm() {
    return CompressionAlgorithm.LZ4;
  }

  byte[] ensureCompressedCapacity(int length) {
    assert !closed;
    assert length >= 0;
    assert compressed.length >= length;
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
