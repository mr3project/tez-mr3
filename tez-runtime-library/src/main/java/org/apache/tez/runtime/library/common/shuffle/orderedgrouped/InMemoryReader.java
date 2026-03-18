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

package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.tez.common.io.NonSyncByteArrayInputStream;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.IFile.SectionLayout;
import org.apache.tez.runtime.library.common.sort.impl.IFile.KeyState;

public class InMemoryReader implements IFile.ReaderRead {

  private static final String INCOMPLETE_READ = "Requested to read %d, but got %d";

  private static class ByteArrayDataInput extends NonSyncByteArrayInputStream implements DataInput {

    private final DataInputStream dataIn;

    ByteArrayDataInput(byte[] buf, int offset, int length) {
      super(buf, offset, length);
      this.dataIn = new DataInputStream(this);
    }

    void reset(byte[] input, int start, int length) {
      this.buf = input;
      this.pos = start;
      this.mark = start;
      this.count = start + length;
    }

    byte[] getData() {
      return buf;
    }

    int getPosition() {
      return pos;
    }

    @Override
    public void readFully(byte[] b) throws IOException {
      dataIn.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
      dataIn.readFully(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
      return dataIn.skipBytes(n);
    }

    @Override
    public boolean readBoolean() throws IOException {
      return dataIn.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
      return dataIn.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
      return dataIn.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException {
      return dataIn.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
      return dataIn.readUnsignedShort();
    }

    @Override
    public char readChar() throws IOException {
      return dataIn.readChar();
    }

    @Override
    public int readInt() throws IOException {
      return dataIn.readInt();
    }

    @Override
    public long readLong() throws IOException {
      return dataIn.readLong();
    }

    @Override
    public float readFloat() throws IOException {
      return dataIn.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
      return dataIn.readDouble();
    }

    @Override
    public String readLine() throws IOException {
      return dataIn.readLine();
    }

    @Override
    public String readUTF() throws IOException {
      return dataIn.readUTF();
    }
  }

  private final MergeManager merger;
  private final int length;
  private final SectionLayout layout;
  private final int usedMemoryForMergeManager;

  private final ByteArrayDataInput valuesIn;
  private final ByteArrayDataInput keysIn;
  private final ByteArrayDataInput lengthsIn;

  private long bytesRead = 0;
  private long numRecordsRead = 0;
  private boolean isEof = false;
  private boolean closed = false;

  private int currentKeyLength = 0;
  private int currentValueLength = 0;

  public InMemoryReader(MergeManager merger,
                        byte[] data, int start, int length, SectionLayout layout,
                        int usedMemoryForMergeManager) {
    assert start >= 0 && length > 0 && start + length <= data.length;
    assert data[start] == 'T' && data[start + 1] == 'I' && data[start + 2] == 'F';
    assert data[start + 3] == 0;
    assert layout.totalLength <= length;
    assert (long)start + layout.totalLength <= data.length;

    this.merger = merger;
    this.length = length;
    this.layout = layout;
    this.usedMemoryForMergeManager = usedMemoryForMergeManager;

    final int valuesOffset = start + (int)layout.valuesStart;
    final int keysOffset = start + (int)layout.keysStart;
    final int lengthsOffset = start + (int)layout.lengthsStart;

    this.valuesIn = new ByteArrayDataInput(data, valuesOffset, (int)layout.valuesLength);
    this.keysIn = new ByteArrayDataInput(data, keysOffset, (int)layout.keysLength);
    this.lengthsIn = new ByteArrayDataInput(data, lengthsOffset, (int)layout.lengthsLength);
  }

  @Override
  public long getPosition() {
    return closed ? 0 : bytesRead;
  }

  @Override
  public long getLength() {
    return closed ? 0 : layout.payloadLength();
  }

  @Override
  public KeyState readRawKey(DataInputBuffer key) throws IOException {
    if (!positionToNextRecord()) {
      return KeyState.NO_KEY;
    }

    final int keyPos = keysIn.getPosition();
    final int n = keysIn.skipBytes(currentKeyLength);
    if (n != currentKeyLength) {
      throw new IOException(String.format(INCOMPLETE_READ, currentKeyLength, n));
    }

    key.reset(keysIn.getData(), keyPos, currentKeyLength);
    bytesRead += currentKeyLength;
    return KeyState.NEW_KEY;
  }

  @Override
  public boolean nextRawKey(DataInputBuffer key) throws IOException {
    return readRawKey(key) != KeyState.NO_KEY;
  }

  @Override
  public void nextRawValue(DataInputBuffer value) throws IOException {
    final int valuePos = valuesIn.getPosition();
    final int n = valuesIn.skipBytes(currentValueLength);
    if (n != currentValueLength) {
      throw new IOException(String.format(INCOMPLETE_READ, currentValueLength, n));
    }

    value.reset(valuesIn.getData(), valuePos, currentValueLength);

    bytesRead += currentValueLength;
    ++numRecordsRead;

    if (numRecordsRead == layout.totalNumRecordsWritten) {
      isEof = true;
    }
  }

  @Override
  public void close() {
    assert !closed;
    closed = true;

    if (merger != null) {
      merger.releaseCommittedMemory(length, usedMemoryForMergeManager);
    }
  }

  private boolean positionToNextRecord() throws IOException {
    if (isEof || numRecordsRead >= layout.totalNumRecordsWritten) {
      isEof = true;
      return false;
    }

    readKeyValueLength(lengthsIn);

    if (currentKeyLength < 0) {
      throw new IOException("Negative key-length: " + currentKeyLength);
    }
    if (currentValueLength < 0) {
      throw new IOException("Negative value-length: " + currentValueLength);
    }
    return true;
  }

  private void readKeyValueLength(DataInput in) throws IOException {
    currentKeyLength = in.readInt();
    currentValueLength = in.readInt();
    bytesRead += 2L * IFile.INT_SIZE;
  }
}