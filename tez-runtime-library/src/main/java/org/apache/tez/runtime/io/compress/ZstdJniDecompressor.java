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
public final class ZstdJniDecompressor implements Decompressor {

  private byte[] compressed = new byte[0];
  private byte[] scratch = new byte[0];
  private ZstdDecompressCtx context = new ZstdDecompressCtx();
  private boolean closed;

  @Override
  public CompressionAlgorithm getAlgorithm() {
    return CompressionAlgorithm.ZSTD;
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
    ZstdJniCompressor.checkRange(compressed, 0, inputLength, "input");
    if (outputCapacity < 0) {
      throw new IllegalArgumentException("Negative output capacity: " + outputCapacity);
    }
    if (scratch.length < outputCapacity) {
      scratch = new byte[outputCapacity];
    }
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
      throw new IOException("Invalid or corrupt zstd-jni block", e);
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
      compressed = new byte[0];
      scratch = new byte[0];
      context.close();
      context = null;
    }
  }
}
