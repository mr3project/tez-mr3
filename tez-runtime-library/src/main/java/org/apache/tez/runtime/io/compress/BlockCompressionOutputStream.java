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
public abstract class BlockCompressionOutputStream extends CompressionOutputStream {

  static final int MIN_BLOCK_SIZE = 4 * 1024;
  static final int MAX_BLOCK_SIZE = 64 * 1024 * 1024;

  private final DataOutputStream output;
  private final int magic;
  private final byte[] inputBuffer;
  private int inputLength;
  // IFile fills inputBuffer itself in this mode; no second position is maintained here.
  private boolean preparedInput;

  private enum State {
    NEW,
    STARTED,
    FINISHED,
    CLOSED
  }

  private State state = State.NEW;

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
    assert !preparedInput;
    if (inputLength == inputBuffer.length) {
      writeChunk();
    }
    inputBuffer[inputLength++] = (byte) value;
  }

  @Override
  public void write(byte[] data, int offset, int length) throws IOException {
    CompressionStreamUtils.checkRange(data, offset, length, "input");
    ensureWritable();
    assert !preparedInput;
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
    if (state == State.NEW) {
      output.writeInt(magic);
      state = State.STARTED;
    }
  }

  // The caller owns the position in this array until a block is submitted.
  // The compression stream must not also accept ordinary writes in this mode.
  public final byte[] beginPreparedBlocks() {
    assert state == State.NEW;
    assert inputLength == 0 && !preparedInput;
    preparedInput = true;
    return inputBuffer;
  }

  public final void writePreparedBlock(int rawLength) throws IOException {
    ensureWritable();
    assert preparedInput && inputLength == 0;
    assert rawLength > 0 && rawLength <= inputBuffer.length;
    start();
    int compressedLength = compressBuffered(rawLength);
    writeFramedChunk(rawLength, compressedLength);
  }

  // Full blocks from caller arrays do not need to pass through inputBuffer.
  public final void writeDirectBlock(byte[] source, int offset, int rawLength)
      throws IOException {
    CompressionStreamUtils.checkRange(source, offset, rawLength, "input");
    ensureWritable();
    assert preparedInput && inputLength == 0;
    assert rawLength == inputBuffer.length;
    start();
    int compressedLength = compressFrom(source, offset, rawLength);
    writeFramedChunk(rawLength, compressedLength);
  }

  private void writeChunk() throws IOException {
    assert !preparedInput;
    assert inputLength >= 0 && inputLength <= inputBuffer.length;
    start();
    if (inputLength == 0) {
      return;
    }
    int compressedLength = compressBuffered(inputLength);
    writeFramedChunk(inputLength, compressedLength);
    inputLength = 0;
  }

  private void writeFramedChunk(int rawLength, int compressedLength) throws IOException {
    assert rawLength > 0 && rawLength <= inputBuffer.length;
    assert compressedLength > 0 && compressedLength <= getCompressedBuffer().length;
    output.writeInt(rawLength);
    output.writeInt(compressedLength);
    output.write(getCompressedBuffer(), 0, compressedLength);
  }

  @Override
  public void flush() throws IOException {
    assert state != State.CLOSED;
    if (preparedInput) {
      assert inputLength == 0;
      start();
    } else {
      writeChunk();
    }
    output.flush();
  }

  @Override
  public void finish() throws IOException {
    assert state != State.CLOSED;
    if (state != State.FINISHED) {
      if (preparedInput) {
        assert inputLength == 0; // IFile submitted the final partial block.
        start();
      } else {
        writeChunk();
      }
      output.writeInt(0);
      output.writeInt(0);
      output.flush();
      state = State.FINISHED;
    }
  }

  @Override
  public void resetState() {
    assert state == State.FINISHED;
    resetCompressor();
    inputLength = 0;
    preparedInput = false;
    state = State.NEW;
  }

  @Override
  public void close() throws IOException {
    if (state != State.CLOSED) {
      try {
        finish();
      } finally {
        state = State.CLOSED;
        output.close();
      }
    }
  }

  private void ensureWritable() {
    assert state == State.NEW || state == State.STARTED;
    assert !preparedInput || inputLength == 0;
  }

  abstract int compressBuffered(int inputLength) throws IOException;

  abstract int compressFrom(byte[] source, int offset, int inputLength) throws IOException;

  abstract byte[] getCompressedBuffer();

  abstract void resetCompressor();

  static int validateBlockSize(int bufferSize, String codecName) throws IOException {
    if (bufferSize <= 0 || bufferSize > MAX_BLOCK_SIZE) {
      throw new IOException("Invalid " + codecName + " block size: " + bufferSize);
    }
    return bufferSize;
  }
}
