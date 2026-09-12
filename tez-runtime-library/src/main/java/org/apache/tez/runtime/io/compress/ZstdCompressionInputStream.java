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

import com.github.luben.zstd.Zstd;

/** Decoder for the provider-private chunk framing documented by the output stream. */
final class ZstdCompressionInputStream extends InputStream {

  private final DataInputStream input;
  private final ZstdJniDecompressor decompressor;
  private final int maximumBlockSize;
  private int position;
  private int limit;
  private boolean ended;
  private boolean closed;

  ZstdCompressionInputStream(
      InputStream input, ZstdJniDecompressor decompressor, int bufferSize)
      throws IOException {
    if (bufferSize <= 0 || bufferSize > ZstdCompressionOutputStream.MAX_BLOCK_SIZE) {
      throw new IOException("Invalid Zstd block size: " + bufferSize);
    }
    this.input = new DataInputStream(input);
    this.decompressor = decompressor;
    this.maximumBlockSize = bufferSize;
    try {
      int magic = this.input.readInt();
      if (magic != ZstdCompressionOutputStream.MAGIC) {
        throw new IOException("Invalid Zstd stream magic");
      }
    } catch (EOFException e) {
      throw new IOException("Truncated Zstd stream header", e);
    }
  }

  @Override
  public int read() throws IOException {
    if (!ensureData()) {
      return -1;
    }
    return decompressor.getDecompressedBuffer()[position++] & 0xff;
  }

  @Override
  public int read(byte[] data, int offset, int length) throws IOException {
    ZstdJniCompressor.checkRange(data, offset, length, "output");
    if (length == 0) {
      return 0;
    }
    if (!ensureData()) {
      return -1;
    }
    int copied = Math.min(length, limit - position);
    System.arraycopy(decompressor.getDecompressedBuffer(), position, data, offset, copied);
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
    final int rawLength;
    final int compressedLength;
    try {
      rawLength = input.readInt();
      compressedLength = input.readInt();
    } catch (EOFException e) {
      throw new IOException("Truncated Zstd chunk header", e);
    }
    if (rawLength == 0 && compressedLength == 0) {
      if (input.read() != -1) {
        throw new IOException("Trailing data after Zstd stream terminator");
      }
      ended = true;
      return;
    }
    if (rawLength <= 0 || rawLength > maximumBlockSize) {
      throw new IOException("Invalid Zstd uncompressed chunk length: " + rawLength);
    }
    final long maximumCompressed;
    try {
      maximumCompressed = Zstd.compressBound(rawLength);
    } catch (RuntimeException e) {
      throw new IOException("Invalid Zstd chunk length: " + rawLength, e);
    }
    if (maximumCompressed < 0 || maximumCompressed > Integer.MAX_VALUE) {
      throw new IOException("Invalid Zstd chunk length: " + rawLength);
    }
    if (compressedLength <= 0 || compressedLength > maximumCompressed) {
      throw new IOException("Invalid Zstd compressed chunk length: " + compressedLength);
    }
    byte[] compressed = decompressor.ensureCompressedCapacity(compressedLength);
    try {
      input.readFully(compressed, 0, compressedLength);
    } catch (EOFException e) {
      throw new IOException("Truncated Zstd chunk payload", e);
    }
    int actual = decompressor.decompressBuffered(compressedLength, rawLength);
    if (actual != rawLength) {
      throw new IOException(
          "Zstd chunk length mismatch: expected " + rawLength + ", decoded " + actual);
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
          decompressor.reset();
        } finally {
          input.close();
        }
      }
    }
  }
}
