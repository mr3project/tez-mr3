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
public final class ZstdJniCompressor implements Compressor {

  private byte[] input = new byte[0];
  private byte[] scratch = new byte[0];
  private ZstdCompressCtx context;
  private boolean closed;

  public ZstdJniCompressor(int compressionLevel) {
    context = new ZstdCompressCtx();
    context.setLevel(compressionLevel);
    context.setChecksum(true);
    closed = false;
  }

  @Override
  public CompressionAlgorithm getAlgorithm() {
    return CompressionAlgorithm.ZSTD;
  }

  private int maxCompressedLength(int uncompressedLength) {
    assert !closed;
    if (uncompressedLength < 0) {
      throw new IllegalArgumentException("Negative uncompressed length: " + uncompressedLength);
    }
    try {
      long maximum = Zstd.compressBound(uncompressedLength);
      if (maximum < 0 || maximum > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("Compressed length overflow for " + uncompressedLength);
      }
      return (int) maximum;
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Unsupported Zstd input length: " + uncompressedLength, e);
    }
  }

  byte[] ensureInputCapacity(int length) {
    assert !closed;
    if (length < 0) {
      throw new IllegalArgumentException("Negative input buffer length: " + length);
    }
    if (input.length < length) {
      input = new byte[length];
    }
    return input;
  }

  int compressBuffered(int inputLength) throws IOException {
    assert !closed;
    checkRange(input, 0, inputLength, "input");
    int maximum = maxCompressedLength(inputLength);
    if (scratch.length < maximum) {
      scratch = new byte[maximum];
    }
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

  static void checkRange(byte[] array, int offset, int length, String name) {
    if (offset < 0 || length < 0 || offset > array.length - length) {
      throw new IndexOutOfBoundsException(
          name + " range: offset=" + offset + ", length=" + length + ", arrayLength=" + array.length);
    }
  }

  @Override
  public void reset() {
    assert !closed;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      input = new byte[0];
      scratch = new byte[0];
      context.close();
      context = null;
    }
  }
}
