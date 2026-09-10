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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Tez Snappy framing: {@code TSN1}, followed by zero or more chunks containing
 * big-endian uncompressed length, compressed length and raw Snappy bytes, then
 * a zero/zero terminator. Each chunk is independently decompressible.
 */
final class SnappyCompressionOutputStream extends TezCompressionOutputStream {
  static final int MAGIC = 0x54534e31; // TSN1
  static final int MAX_BLOCK_SIZE = 64 * 1024 * 1024;

  private final DataOutputStream output;
  private final Compressor compressor;
  private final byte[] inputBuffer;
  private byte[] compressedBuffer;
  private int inputLength;
  private boolean started;
  private boolean finished;
  private boolean closed;

  SnappyCompressionOutputStream(OutputStream output, Compressor compressor, int bufferSize)
      throws IOException {
    if (output == null || compressor == null) {
      throw new NullPointerException();
    }
    if (bufferSize <= 0 || bufferSize > MAX_BLOCK_SIZE) {
      throw new IOException("Invalid Snappy block size: " + bufferSize);
    }
    this.output = new DataOutputStream(output);
    this.compressor = compressor;
    this.inputBuffer = new byte[bufferSize];
    this.compressedBuffer = new byte[compressor.maxCompressedLength(bufferSize)];
  }

  @Override
  public void write(int value) throws IOException {
    ensureWritable();
    if (inputLength == inputBuffer.length) {
      writeChunk();
    }
    inputBuffer[inputLength++] = (byte) value;
  }

  @Override
  public void write(byte[] data, int offset, int length) throws IOException {
    XerialSnappyCompressor.checkRange(data, offset, length, "input");
    ensureWritable();
    while (length > 0) {
      int copied = Math.min(length, inputBuffer.length - inputLength);
      System.arraycopy(data, offset, inputBuffer, inputLength, copied);
      inputLength += copied;
      offset += copied;
      length -= copied;
      if (inputLength == inputBuffer.length) {
        writeChunk();
      }
    }
  }

  private void start() throws IOException {
    if (!started) {
      output.writeInt(MAGIC);
      started = true;
    }
  }

  private void writeChunk() throws IOException {
    if (inputLength == 0) {
      return;
    }
    start();
    int maximum = compressor.maxCompressedLength(inputLength);
    if (compressedBuffer.length < maximum) {
      compressedBuffer = new byte[maximum];
    }
    int compressedLength = compressor.compress(
        inputBuffer, 0, inputLength, compressedBuffer, 0, compressedBuffer.length);
    output.writeInt(inputLength);
    output.writeInt(compressedLength);
    output.write(compressedBuffer, 0, compressedLength);
    inputLength = 0;
  }

  @Override
  public void flush() throws IOException {
    ensureOpen();
    writeChunk();
    output.flush();
  }

  @Override
  public void finish() throws IOException {
    ensureOpen();
    if (!finished) {
      start();
      writeChunk();
      output.writeInt(0);
      output.writeInt(0);
      output.flush();
      finished = true;
    }
  }

  @Override
  public void resetState() throws IOException {
    ensureOpen();
    if (!finished) {
      throw new IOException("Cannot reset an unfinished Snappy stream");
    }
    compressor.reset();
    inputLength = 0;
    started = false;
    finished = false;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      try {
        finish();
      } finally {
        closed = true;
        output.close();
      }
    }
  }

  private void ensureWritable() throws IOException {
    ensureOpen();
    if (finished) {
      throw new IOException("Snappy stream has been finished");
    }
  }

  private void ensureOpen() throws IOException {
    if (closed) {
      throw new IOException("Snappy stream is closed");
    }
  }
}
