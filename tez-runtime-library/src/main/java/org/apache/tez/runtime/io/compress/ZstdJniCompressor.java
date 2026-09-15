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
import com.github.luben.zstd.ZstdCompressCtx;
import org.apache.tez.runtime.api.CompressionAlgorithm;

/** Raw zstd-jni block compressor. A scratch buffer prevents partial caller output. */
final class ZstdJniCompressor implements Compressor {

  private byte[] input;
  private byte[] scratch;
  private ZstdCompressCtx context;
  private boolean closed;

  ZstdJniCompressor(int compressionLevel, int bufferSize, int maxCompressedLength) {
    context = new ZstdCompressCtx();
    context.setLevel(compressionLevel);
    context.setChecksum(true);
    input = new byte[bufferSize];
    scratch = new byte[maxCompressedLength];
    closed = false;
  }

  @Override
  public CompressionAlgorithm getAlgorithm() {
    return CompressionAlgorithm.ZSTD;
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
    assert inputLength <= input.length;
    try {
      long result = context.compressByteArray(
          scratch, 0, scratch.length, input, 0, inputLength);
      if (Zstd.isError(result)) {
        throw new IOException("Zstd compression failed: " + Zstd.getErrorName(result));
      }
      if (result < 0 || result > Integer.MAX_VALUE) {
        throw new IOException("Invalid Zstd compressed length: " + result);
      }
      return (int) result;
    } catch (IOException e) {
      throw e;
    } catch (Throwable e) {
      throw new IOException("zstd-jni compression failed", e);
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
    if (!closed) {
      closed = true;
      input = null;
      scratch = null;
      context.close();
      context = null;
    }
  }
}
