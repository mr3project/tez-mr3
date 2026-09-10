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

import org.xerial.snappy.Snappy;

/** Raw xerial Snappy block compressor. A scratch buffer prevents partial caller output. */
public final class XerialSnappyCompressor implements Compressor {
  private byte[] input = new byte[0];
  private byte[] scratch = new byte[0];
  private boolean closed;

  @Override
  public CompressionAlgorithm getAlgorithm() {
    return CompressionAlgorithm.SNAPPY;
  }

  @Override
  public int maxCompressedLength(int uncompressedLength) {
    ensureOpen();
    assert uncompressedLength >= 0;
    // Snappy's documented bound is 32 + n + n / 6. Check it in long arithmetic before
    // entering xerial so an overflowing Java int can never be mistaken for a valid capacity.
    long calculatedMaximum = 32L + uncompressedLength + uncompressedLength / 6L;
    assert calculatedMaximum <= Integer.MAX_VALUE;
    try {
      int result = Snappy.maxCompressedLength(uncompressedLength);
      assert result >= uncompressedLength;
      return result;
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Unsupported Snappy input length: " + uncompressedLength, e);
    }
  }

  @Override
  public int compress(byte[] input, int inputOffset, int inputLength,
      byte[] output, int outputOffset, int outputCapacity) throws IOException {
    ensureOpen();
    checkRange(input, inputOffset, inputLength, "input");
    checkRange(output, outputOffset, outputCapacity, "output");
    int maximum = maxCompressedLength(inputLength);
    if (scratch.length < maximum) {
      scratch = new byte[maximum];
    }
    try {
      int length = Snappy.rawCompress(input, inputOffset, inputLength, scratch, 0);
      assert length <= outputCapacity;
      System.arraycopy(scratch, 0, output, outputOffset, length);
      return length;
    } catch (IOException e) {
      throw e;
    } catch (Throwable e) {
      throw new IOException("xerial Snappy compression failed", e);
    }
  }

  byte[] ensureInputCapacity(int length) {
    assert !closed;
    assert length >= 0;
    if (input.length < length) {
      input = new byte[length];
    }
    return input;
  }

  int compressBuffered(int inputLength) throws IOException {
    assert !closed;
    assert inputLength >= 0 && inputLength <= input.length;
    int maximum = maxCompressedLength(inputLength);
    if (scratch.length < maximum) {
      scratch = new byte[maximum];
    }
    try {
      return Snappy.rawCompress(input, 0, inputLength, scratch, 0);
    } catch (Throwable e) {
      throw new IOException("xerial Snappy compression failed", e);
    }
  }

  byte[] getCompressedBuffer() {
    assert !closed;
    return scratch;
  }

  static void checkRange(byte[] array, int offset, int length, String name) {
    assert array != null : name;
    assert offset >= 0;
    assert length >= 0;
    assert offset <= array.length - length;
  }

  private void ensureOpen() {
    assert !closed;
  }

  @Override
  public void reset() {
    ensureOpen();
  }

  @Override
  public void close() {
    closed = true;
    input = new byte[0];
    scratch = new byte[0];
  }
}
