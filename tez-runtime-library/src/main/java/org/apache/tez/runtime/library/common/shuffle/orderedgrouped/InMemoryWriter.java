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

import java.io.IOException;
import java.util.zip.CRC32;

import org.apache.tez.runtime.library.common.sort.impl.RawDataBuffer;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.IFileOutputStream;
import org.apache.tez.util.FastByteComparisons;

public class InMemoryWriter implements IFile.WriterAppendDataInputBuffer {

  private final byte[] array;
  private final CRC32 checksum = new CRC32();
  private int pos;

  private byte[] previousKeyData = new byte[0];
  private int previousKeyOffset = 0;
  private int previousKeyLength = 0;
  private byte[] previousKeyCopy = new byte[0];
  private boolean previousWasRepeat = false;

  // InMemoryWriter is used only in MergeManager.IntermediateMemoryToMemoryMerger with isRleEnabled = true.
  private final boolean isRleEnabled = true;

  // InMemoryWriter does not use another byte[] buffer, unlike IFile.Writer
  public InMemoryWriter(byte[] array) throws IOException {
    this.array = array;
    writeBytes(IFile.HEADER, 0, IFile.HEADER.length - 1);
    byte flag = 0;
    if (isRleEnabled) {
      flag |= IFile.FLAG_RLE_ENABLED;
    }
    writeByte(flag);
  }

  public boolean isRleEnabled() {
      return isRleEnabled;
  }

  public void appendNoRle(RawDataBuffer key, RawDataBuffer value) throws IOException {
    assert false;
  }

  public void appendNoRleTez(RawDataBuffer key, RawDataBuffer value) throws IOException {
    assert false;
  }

  @Override
  public void appendRle(RawDataBuffer key, RawDataBuffer value, boolean keyStable) throws IOException {
    assert isRleEnabled;
    int keyPosition = key.getPosition();
    int keyLength = key.getLength();
    int valueLength = value.getLength();

    boolean sameKey = key == IFile.REPEAT_KEY;
    if (!sameKey) {
      sameKey = (keyLength != 0) && FastByteComparisons.compareEqual(
          previousKeyData, previousKeyOffset, previousKeyLength, key.getData(), keyPosition, keyLength);
    }

    if (!sameKey) {
      // Normal key-value pair
      // Write V_END_MARKER if needed (if previous was a REPEAT_KEY)
      if (previousWasRepeat) {
        writeInt(IFile.V_END_MARKER);
      }

      long combined = ((long) valueLength << 32) | (keyLength & 0xFFFFFFFFL);
      writeLong(combined);
      writeBytes(key.getData(), keyPosition, keyLength);
      writeBytes(value.getData(), value.getPosition(), valueLength);
      populatePreviousKey(key.getData(), keyPosition, keyLength, keyStable);
    } else {
      // Repeated key
      if (!previousWasRepeat) {
        // First repeated key, write RLE marker
        writeInt(IFile.RLE_MARKER);
      }

      // Write just the value length and value
      writeInt(valueLength);
      writeBytes(value.getData(), value.getPosition(), valueLength);
    }

    previousWasRepeat = sameKey;
  }

  private void populatePreviousKey(byte[] keyData, int keyOffset, int keyLength, boolean keyStable) {
    if (keyStable) {
      previousKeyData = keyData;
      previousKeyOffset = keyOffset;
      previousKeyLength = keyLength;
    } else {
      if (previousKeyCopy.length < keyLength) {
        previousKeyCopy = new byte[keyLength];
      }
      System.arraycopy(keyData, keyOffset, previousKeyCopy, 0, keyLength);
      previousKeyData = previousKeyCopy;
      previousKeyOffset = 0;
      previousKeyLength = keyLength;
    }
  }

  public void close() throws IOException {
    if (isRleEnabled) {
      closeRle();
    }

    // Write EOF_MARKER for key/value length
    long combined = ((long) IFile.EOF_MARKER << 32) | (IFile.EOF_MARKER & 0xFFFFFFFFL);
    writeLong(combined);
    writeChecksum();
  }

  private void closeRle() throws IOException {
    // Write V_END_MARKER if needed
    if (previousWasRepeat) {
      writeInt(IFile.V_END_MARKER);
    }
  }

  private void writeByte(int value) throws IOException {
    ensureAvailable(1);
    array[pos] = (byte) value;
    checksum.update(value & 0xff);
    pos++;
  }

  private void writeBytes(byte[] data, int offset, int length) throws IOException {
    ensureAvailable(length);
    System.arraycopy(data, offset, array, pos, length);
    checksum.update(array, pos, length);
    pos += length;
  }

  private void writeInt(int value) throws IOException {
    ensureAvailable(Integer.BYTES);
    FastByteComparisons.theUnsafe.putInt(array,
        FastByteComparisons.BYTE_ARRAY_BASE_OFFSET + (long) pos, value);
    checksum.update(array, pos, Integer.BYTES);
    pos += Integer.BYTES;
  }

  private void writeLong(long value) throws IOException {
    ensureAvailable(Long.BYTES);
    FastByteComparisons.theUnsafe.putLong(array,
        FastByteComparisons.BYTE_ARRAY_BASE_OFFSET + (long) pos, value);
    checksum.update(array, pos, Long.BYTES);
    pos += Long.BYTES;
  }

  private void writeChecksum() throws IOException {
    ensureAvailable(IFileOutputStream.CHECKSUM_SIZE);
    long value = checksum.getValue();
    for (int i = 0; i < IFileOutputStream.CHECKSUM_SIZE; i++) {
      array[pos++] = (byte) (value >>> (Byte.SIZE * i));
    }
  }

  private void ensureAvailable(int length) throws IOException {
    if (length < 0 || pos > array.length - length) {
      throw new IOException("Insufficient space in in-memory IFile buffer");
    }
  }
}
