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

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDecompressCtx;
import org.apache.tez.runtime.api.CompressionAlgorithm;

/** Raw zstd-jni block decompressor. */
final class ZstdJniDecompressor implements Decompressor {

  private byte[] compressed;
  private byte[] scratch;
  private ZstdDecompressCtx context;
  private boolean closed;

  ZstdJniDecompressor(int bufferSize, int maxCompressedLength) {
    compressed = new byte[maxCompressedLength];
    scratch = new byte[bufferSize];
    context = new ZstdDecompressCtx();
    closed = false;
  }

  @Override
  public CompressionAlgorithm getAlgorithm() {
    return CompressionAlgorithm.ZSTD;
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
      long result = context.decompressByteArray(
          scratch, 0, outputCapacity, compressed, 0, inputLength);
      if (Zstd.isError(result)) {
        throw new IOException("Invalid or corrupt Zstd block: " + Zstd.getErrorName(result));
      }
      if (result != outputCapacity) {
        throw new IOException(
            "Zstd block length mismatch: expected " + outputCapacity + ", decoded " + result);
      }
      return (int) result;
    } catch (IOException e) {
      throw e;
    } catch (Throwable e) {
      throw new IOException("Invalid or corrupt Zstd block", e);
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
    if (!closed) {
      closed = true;
      compressed = null;
      scratch = null;
      context.close();
      context = null;
    }
  }
}
