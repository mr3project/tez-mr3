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

  public XerialSnappyCompressor() {
    this.closed = false;
  }

  @Override
  public CompressionAlgorithm getAlgorithm() {
    return CompressionAlgorithm.SNAPPY;
  }

  // Returns a safe bound for one complete block without changing compressor state.
  // Negative, overflowing, and unsupported lengths are rejected.
  private int maxCompressedLength(int uncompressedLength) {
    assert !closed;
    if (uncompressedLength < 0) {
      throw new IllegalArgumentException("Negative uncompressed length: " + uncompressedLength);
    }
    // Snappy's documented bound is 32 + n + n / 6. Check it in long arithmetic before
    // entering xerial so an overflowing Java int can never be mistaken for a valid capacity.
    long calculatedMaximum = 32L + uncompressedLength + uncompressedLength / 6L;
    if (calculatedMaximum > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Compressed length overflow for " + uncompressedLength);
    }
    try {
      int result = Snappy.maxCompressedLength(uncompressedLength);
      if (result < 0 || result < uncompressedLength) {
        throw new IllegalArgumentException("Compressed length overflow for " + uncompressedLength);
      }
      return result;
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Unsupported Snappy input length: " + uncompressedLength, e);
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
      return Snappy.rawCompress(input, 0, inputLength, scratch, 0);
    } catch (Throwable e) {
      throw new IOException("Xerial Snappy compression failed", e);
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
    closed = true;
    input = new byte[0];
    scratch = new byte[0];
  }
}
