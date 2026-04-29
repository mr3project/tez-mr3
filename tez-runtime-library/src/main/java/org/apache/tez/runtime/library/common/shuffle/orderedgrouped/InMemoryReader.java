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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.tez.runtime.api.TezOffsetRecord;
import org.apache.tez.common.io.NonSyncByteArrayInputStream;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Reader.KeyState;

/**
 * <code>IFile.InMemoryReader</code> to read map-outputs present in-memory.
 */
public class InMemoryReader implements IFile.KeyValueReader {

  private static class ByteArrayDataInput extends NonSyncByteArrayInputStream {

    public ByteArrayDataInput(byte buf[], int offset, int length) {
      super(buf, offset, length);
    }

    public byte[] getData() { return buf; }
    public int getPosition() { return pos; }

    public byte readByte() {
      return (byte)read();
    }

    public int readInt() {
      if (pos + 4 > count) {
        throw new RuntimeException("Not enough bytes to read an int");
      }
      int value = ((buf[pos] & 0xFF) << 24) |
                  ((buf[pos + 1] & 0xFF) << 16) |
                  ((buf[pos + 2] & 0xFF) << 8) |
                  (buf[pos + 3] & 0xFF);
      pos += 4;
      return value;
    }

    public long readLong() {
      if (pos + 8 > count) {
        throw new RuntimeException("Not enough bytes to read a long");
      }
      long value = ((long)(buf[pos] & 0xFF) << 56) |
                   ((long)(buf[pos + 1] & 0xFF) << 48) |
                   ((long)(buf[pos + 2] & 0xFF) << 40) |
                   ((long)(buf[pos + 3] & 0xFF) << 32) |
                   ((long)(buf[pos + 4] & 0xFF) << 24) |
                   ((long)(buf[pos + 5] & 0xFF) << 16) |
                   ((long)(buf[pos + 6] & 0xFF) << 8) |
                   (buf[pos + 7] & 0xFF);
      pos += 8;
      return value;
    }
  }

  private final MergeManager merger;
  private final InputAttemptIdentifier taskAttemptId;
  private int originalKeyPos, originalKeyLength;

  private boolean eof = false;
  private int recNo = 1;
  private int currentKeyLength;
  private int currentValueLength;
  private long bytesRead;

  private byte[] buffer;
  private final int length;
  private final ByteArrayDataInput memDataIn;
  private final boolean isRleEnabled;

  private final TezOffsetRecord tezOffsetRecord;

  private final int usedMemoryForMergeManager;

  public InMemoryReader(MergeManager merger, InputAttemptIdentifier taskAttemptId,
                        byte[] data, int start, int length, int usedMemoryForMergeManager,
                        TezOffsetRecord tezOffsetRecord) {
    this.merger = merger;
    this.taskAttemptId = taskAttemptId;

    this.buffer = data;
    this.length = length;

    if (length < IFile.getHeaderLength()) {
      throw new IllegalArgumentException("Missing IFile header");
    }
    if (!(data[start] == 'T' && data[start + 1] == 'I' && data[start + 2] == 'F')) {
      throw new IllegalArgumentException("Not a valid IFile header");
    }

    byte flag = data[start + 3];
    int dataStart = start + IFile.getHeaderLength();
    int dataLength = length - IFile.getHeaderLength();
    this.memDataIn = new ByteArrayDataInput(buffer, dataStart, dataLength);
    this.isRleEnabled = (flag & IFile.FLAG_RLE_ENABLED) != 0;

    assert !(tezOffsetRecord != null) || !isRleEnabled;
    this.tezOffsetRecord = tezOffsetRecord;

    this.usedMemoryForMergeManager = usedMemoryForMergeManager;
  }

  @Override
  public long getLength() {
    return length;
  }

  private void dumpOnError() {
    File dumpFile = new File("../output/" + taskAttemptId + ".dump");
    System.err.println("Dumping corrupt map-output of " + taskAttemptId +
                       " to " + dumpFile.getAbsolutePath());
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(dumpFile);
      fos.write(buffer, 0, length);
    } catch (IOException ioe) {
      System.err.println("Failed to dump map-output of " + taskAttemptId);
    } finally {
      if (fos != null) {
        try {
          fos.close();
        } catch (IOException e) {
          System.err.println("Failed to dump map-output of " + taskAttemptId);
        }
      }
    }
  }

  private void readKeyValueLengthNoRle() {
    if (tezOffsetRecord != null) {
      int recordOffset = (int) bytesRead;

      if (recordOffset == tezOffsetRecord.getEofPos()) {
        currentKeyLength = memDataIn.readInt();
        currentValueLength = memDataIn.readInt();
        bytesRead += Integer.BYTES + Integer.BYTES;
        return;
      }

      if (recordOffset < tezOffsetRecord.getFirstKeyOffset()) {
        currentKeyLength = tezOffsetRecord.getMaxKeyLen();
      } else {
        currentKeyLength = memDataIn.readInt();
        bytesRead += Integer.BYTES;
      }

      if (recordOffset < tezOffsetRecord.getFirstValOffset()) {
        currentValueLength = tezOffsetRecord.getMaxValLen();
      } else {
        currentValueLength = memDataIn.readInt();
        bytesRead += Integer.BYTES;
      }
    } else {
      currentKeyLength = memDataIn.readInt();
      currentValueLength = memDataIn.readInt();
      bytesRead += Integer.BYTES + Integer.BYTES;
    }
  }

  private void readKeyValueLengthRle() {
    currentKeyLength = memDataIn.readInt();
    currentValueLength = memDataIn.readInt();
    if (currentKeyLength != IFile.RLE_MARKER) {
      originalKeyLength = currentKeyLength;
      originalKeyPos = memDataIn.getPosition();
    }
    bytesRead += Integer.BYTES + Integer.BYTES;
  }

  private void readValueLengthRle() {
    currentValueLength = memDataIn.readInt();
    bytesRead += Integer.BYTES;
    if (currentValueLength == IFile.V_END_MARKER) {
      readKeyValueLengthRle();
    }
  }

  private boolean positionToNextRecordNoRle() throws IOException {
    if (eof) {
      throw new IOException(String.format("Reached EOF. Completed reading %d", bytesRead));
    }
    int prevKeyLength = currentKeyLength;
    readKeyValueLengthNoRle();

    if (currentKeyLength == IFile.EOF_MARKER && currentValueLength == IFile.EOF_MARKER) {
      eof = true;
      return false;
    }

    if (currentKeyLength < 0) {
      throw new IOException("Rec# " + recNo + ": Negative key-length: " +
          currentKeyLength + " PreviousKeyLen: " + prevKeyLength);
    }
    if (currentValueLength < 0) {
      throw new IOException("Rec# " + recNo + ": Negative value-length: " +
          currentValueLength);
    }
    return true;
  }

  private boolean positionToNextRecordRle() throws IOException {
    if (eof) {
      throw new IOException(String.format("Reached EOF. Completed reading %d", bytesRead));
    }
    int prevKeyLength = currentKeyLength;

    if (prevKeyLength == IFile.RLE_MARKER) {
      readValueLengthRle();
    } else {
      readKeyValueLengthRle();
    }

    if (currentKeyLength == IFile.EOF_MARKER && currentValueLength == IFile.EOF_MARKER) {
      eof = true;
      return false;
    }

    boolean isAllowedNegativeKeyLength = currentKeyLength == IFile.RLE_MARKER;
    if (!isAllowedNegativeKeyLength && currentKeyLength < 0) {
      throw new IOException("Rec# " + recNo + ": Negative key-length: " +
          currentKeyLength + " PreviousKeyLen: " + prevKeyLength);
    }
    if (currentValueLength < 0) {
      throw new IOException("Rec# " + recNo + ": Negative value-length: " +
          currentValueLength);
    }
    return true;
  }

  @Override
  public KeyState readRawKey(DataInputBuffer key) throws IOException {
    if (isRleEnabled) {
      return readRawKeyRle(key);
    } else {
      return readRawKeyNoRle(key);
    }
  }

  private KeyState readRawKeyNoRle(DataInputBuffer key) throws IOException {
    if (!positionToNextRecordNoRle()) {
      return KeyState.NO_KEY;
    }
    int pos = memDataIn.getPosition();
    byte[] data = memDataIn.getData();
    key.reset(data, pos, currentKeyLength);
    long skipped = memDataIn.skip(currentKeyLength);
    if (skipped != currentKeyLength) {
      throw new IOException("Rec# " + recNo +
          ": Failed to skip past key of length: " +
          currentKeyLength);
    }
    bytesRead += currentKeyLength;
    return KeyState.NEW_KEY;
  }

  private KeyState readRawKeyRle(DataInputBuffer key) throws IOException {
    if (!positionToNextRecordRle()) {
      return KeyState.NO_KEY;
    }
    // Setup the key
    int pos = memDataIn.getPosition();
    byte[] data = memDataIn.getData();
    if (currentKeyLength == IFile.RLE_MARKER) {
      // get key length from original key
      key.reset(data, originalKeyPos, originalKeyLength);
      return KeyState.SAME_KEY;
    }
    key.reset(data, pos, currentKeyLength);
    // Position for the next value
    long skipped = memDataIn.skip(currentKeyLength);
    if (skipped != currentKeyLength) {
      throw new IOException("Rec# " + recNo +
          ": Failed to skip past key of length: " +
          currentKeyLength);
    }
    bytesRead += currentKeyLength;
    return KeyState.NEW_KEY;
  }

  public KeyState readRawKey(BytesWritable key) throws IOException {
    if (isRleEnabled) {
      return readRawKeyRle(key);
    } else {
      return readRawKeyNoRle(key);
    }
  }

  private KeyState readRawKeyNoRle(BytesWritable key) throws IOException {
    if (!positionToNextRecordNoRle()) {
      return KeyState.NO_KEY;
    }

    int pos = memDataIn.getPosition();
    byte[] data = memDataIn.getData();
    key.setDirect(data, pos, currentKeyLength);

    // Position for the next value
    long skipped = memDataIn.skip(currentKeyLength);
    if (skipped != currentKeyLength) {
      throw new IOException("Rec# " + recNo + ": Failed to skip past key of length: " + currentKeyLength);
    }

    bytesRead += currentKeyLength;
    return KeyState.NEW_KEY;
  }

  private KeyState readRawKeyRle(BytesWritable key) throws IOException {
    if (!positionToNextRecordRle()) {
      return KeyState.NO_KEY;
    }
    if (currentKeyLength == IFile.RLE_MARKER) {
      return KeyState.SAME_KEY;
    }

    int pos = memDataIn.getPosition();
    byte[] data = memDataIn.getData();
    key.setDirect(data, pos, currentKeyLength);

    // Position for the next value
    long skipped = memDataIn.skip(currentKeyLength);
    if (skipped != currentKeyLength) {
      throw new IOException("Rec# " + recNo + ": Failed to skip past key of length: " + currentKeyLength);
    }

    bytesRead += currentKeyLength;
    return KeyState.NEW_KEY;
  }

  @Override
  public void nextRawValue(DataInputBuffer value) throws IOException {
    int pos = memDataIn.getPosition();
    byte[] data = memDataIn.getData();
    value.reset(data, pos, currentValueLength);

    // Position for the next record
    long skipped = memDataIn.skip(currentValueLength);
    if (skipped != currentValueLength) {
      throw new IOException("Rec# " + recNo +
          ": Failed to skip past value of length: " +
          currentValueLength);
    }
    // Record the byte
    bytesRead += currentValueLength;
    ++recNo;
  }

  public void nextRawValue(BytesWritable value) throws IOException {
    int pos = memDataIn.getPosition();
    byte[] data = memDataIn.getData();
    value.setDirect(data, pos, currentValueLength);

    // Position for the next record
    long skipped = memDataIn.skip(currentValueLength);
    if (skipped != currentValueLength) {
      throw new IOException("Rec# " + recNo + ": Failed to skip past value of length: " + currentValueLength);
    }

    bytesRead += currentValueLength;
    ++recNo;
  }

  @Override
  public long consumeAll(BiConsumer<BytesWritable, BytesWritable> consumer) throws IOException {
    assert recNo == 1;  // must not be mixed with next()
    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();
    long recordCount = 0;
    if (isRleEnabled) {
      while (readRawKeyRle(key) != KeyState.NO_KEY) {
        nextRawValue(value);
        consumer.accept(key, value);
        recordCount++;
      }
    } else {
      while (readRawKeyNoRle(key) != KeyState.NO_KEY) {
        nextRawValue(value);
        consumer.accept(key, value);
        recordCount++;
      }
    }
    return recordCount;
  }

  @Override
  public IFile.KeyStateCount consumeValuesForCurrentKey(
      BytesWritable key, BytesWritable value, Consumer<BytesWritable> consumer) throws IOException {
    KeyState nextKeyState;
    long count = 0;
    do {
      nextRawValue(value);
      consumer.accept(value);
      count++;
      nextKeyState = isRleEnabled ? readRawKeyRle(key) : readRawKeyNoRle(key);
    } while (nextKeyState == KeyState.SAME_KEY);
    return new IFile.KeyStateCount(nextKeyState, count);
  }

  @Override
  public void close() {
    // Release
    buffer = null;
    // Inform the MergeManager
    if (merger != null) {
      merger.releaseCommittedMemory(length, usedMemoryForMergeManager);
    }
  }
}
