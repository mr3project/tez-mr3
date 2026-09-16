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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/** Common reader for Tez's provider-private block-compression framing. */
abstract class BlockCompressionInputStream extends InputStream {

  private final DataInputStream input;
  private final int maxBlockSize;
  private final int maxCompressedLength;
  private final String codecName;
  private int position;
  private int limit;
  private boolean ended;
  private boolean closed;

  BlockCompressionInputStream(InputStream input, int bufferSize, int maxCompressedLength,
      int magic, String codecName) throws IOException {
    BlockCompressionOutputStream.validateBlockSize(bufferSize, codecName);
    this.input = new DataInputStream(input);
    this.maxBlockSize = bufferSize;
    this.maxCompressedLength = maxCompressedLength;
    this.codecName = codecName;
    try {
      int actualMagic = this.input.readInt();
      if (actualMagic != magic) {
        throw new IOException("Invalid " + codecName + " stream magic");
      }
    } catch (EOFException e) {
      throw new IOException("Truncated " + codecName + " stream header", e);
    }
  }

  @Override
  public int read() throws IOException {
    if (!ensureData()) {
      return -1;
    }
    return getDecompressedBuffer()[position++] & 0xff;
  }

  @Override
  public int read(byte[] data, int offset, int length) throws IOException {
    CompressionStreamUtils.checkRange(data, offset, length, "output");
    if (length == 0) {
      return 0;
    }
    if (!ensureData()) {
      return -1;
    }
    int copied = Math.min(length, limit - position);
    System.arraycopy(getDecompressedBuffer(), position, data, offset, copied);
    position += copied;
    return copied;
  }

  private boolean ensureData() throws IOException {
    assert !closed;
    while (position == limit && !ended) {
      readChunk();
    }
    return position < limit;
  }

  private void readChunk() throws IOException {
    final int rawLength = input.readInt();
    final int compressedLength = input.readInt();
    if (rawLength == 0 && compressedLength == 0) {
      if (input.read() != -1) {
        throw new IOException("Trailing data after " + codecName + " stream terminator");
      }
      ended = true;
      return;
    }
    if (rawLength <= 0 || rawLength > maxBlockSize) {
      throw new IOException(
          "Invalid " + codecName + " uncompressed chunk length: " + rawLength);
    }
    if (compressedLength <= 0 || compressedLength > maxCompressedLength) {
      throw new IOException(
          "Invalid " + codecName + " compressed chunk length: " + compressedLength);
    }
    byte[] compressed = ensureCapacity(compressedLength, rawLength);
    input.readFully(compressed, 0, compressedLength);
    int actual = decompressBuffered(compressedLength, rawLength);
    if (actual != rawLength) {
      throw new IOException(codecName + " chunk length mismatch: expected " + rawLength
          + ", decoded " + actual);
    }
    position = 0;
    limit = actual;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      try {
        byte[] discard = new byte[8192];
        while (read(discard) != -1) {
          // Parse all framing and force checksum validation before releasing the decompressor.
        }
      } finally {
        closed = true;
        try {
          resetDecompressor();
        } finally {
          input.close();
        }
      }
    }
  }

  abstract byte[] ensureCapacity(int compressedLength, int rawLength) throws IOException;

  abstract int decompressBuffered(int inputLength, int outputCapacity) throws IOException;

  abstract byte[] getDecompressedBuffer();

  abstract void resetDecompressor();
}
