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

/** Common writer for Tez's provider-private block-compression framing. */
abstract class BlockCompressionOutputStream extends CompressionOutputStream {

  static final int MIN_BLOCK_SIZE = 4 * 1024;
  static final int MAX_BLOCK_SIZE = 64 * 1024 * 1024;

  private final DataOutputStream output;
  private final int magic;
  private final byte[] inputBuffer;
  private int inputLength;
  private boolean started;
  private boolean finished;
  private boolean closed;

  BlockCompressionOutputStream(OutputStream output, byte[] inputBuffer,
      int bufferSize, int magic, String codecName) throws IOException {
    validateBlockSize(bufferSize, codecName);
    this.output = new DataOutputStream(output);
    this.magic = magic;
    this.inputBuffer = inputBuffer;
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
    CompressionStreamUtils.checkRange(data, offset, length, "input");
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
      output.writeInt(magic);
      started = true;
    }
  }

  private void writeChunk() throws IOException {
    start();
    if (inputLength == 0) {
      return;
    }
    int compressedLength = compressBuffered(inputLength);
    output.writeInt(inputLength);
    output.writeInt(compressedLength);
    output.write(getCompressedBuffer(), 0, compressedLength);
    inputLength = 0;
  }

  @Override
  public void flush() throws IOException {
    assert !closed;
    writeChunk();
    output.flush();
  }

  @Override
  public void finish() throws IOException {
    assert !closed;
    if (!finished) {
      writeChunk();
      output.writeInt(0);
      output.writeInt(0);
      output.flush();
      finished = true;
    }
  }

  @Override
  public void resetState() {
    assert !closed;
    assert finished;
    resetCompressor();
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

  private void ensureWritable() {
    assert !closed;
    assert !finished;
  }

  abstract int compressBuffered(int inputLength) throws IOException;

  abstract byte[] getCompressedBuffer();

  abstract void resetCompressor();

  static int validateBlockSize(int bufferSize, String codecName) throws IOException {
    if (bufferSize <= 0 || bufferSize > MAX_BLOCK_SIZE) {
      throw new IOException("Invalid " + codecName + " block size: " + bufferSize);
    }
    return bufferSize;
  }
}
