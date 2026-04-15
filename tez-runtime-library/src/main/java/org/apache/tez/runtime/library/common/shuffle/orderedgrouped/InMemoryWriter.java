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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.tez.common.io.NonSyncDataOutputStream;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.IFileOutputStream;
import org.apache.tez.runtime.library.utils.BufferUtils;

public class InMemoryWriter implements IFile.WriterAppendDataInputBuffer {

  // BoundedByteArrayOutputStream(array, 0, array.length) is protected and cannot be used directly
  private static class InMemoryBoundedByteArrayOutputStream extends BoundedByteArrayOutputStream {
    InMemoryBoundedByteArrayOutputStream(byte[] array) {
      super(array, 0, array.length);
    }
  }

  private DataOutputStream out;
  private final boolean isRleEnabled;
  private int maxKeyLen;
  private int maxValLen;
  private boolean keyLenTransitioned = false;
  private boolean valLenTransitioned = false;
  private final boolean exactMaxLensKnownAtInit;

  private DataInputBuffer prevKey = null;
  private final DataOutputBuffer previous = new DataOutputBuffer();

  // InMemoryWriter does not use another byte[] buffer, unlike IFile.Writer
  public InMemoryWriter(byte[] array, boolean isRleEnabled, int maxKeyLen, int maxValLen) throws IOException {
    BoundedByteArrayOutputStream arrayStream = new InMemoryBoundedByteArrayOutputStream(array);
    this.out = new NonSyncDataOutputStream(new IFileOutputStream(arrayStream));
    this.isRleEnabled = isRleEnabled;
    this.maxKeyLen = maxKeyLen;
    this.maxValLen = maxValLen;
    this.exactMaxLensKnownAtInit = (maxKeyLen >= 0 && maxValLen >= 0);
    this.out.write(IFile.HEADER, 0, IFile.HEADER.length - 1);
    byte flag = 0;
    if (isRleEnabled) {
      flag |= IFile.FLAG_RLE_ENABLED;
    }
    this.out.write(flag);
  }

  public boolean isRleEnabled() {
      return isRleEnabled;
  }

  public void appendNoRle(DataInputBuffer key, DataInputBuffer value) throws IOException {
      int keyLength = key.getLength() - key.getPosition();
      int valueLength = value.getLength() - value.getPosition();

      long combined = ((long) keyLength << 32) | (valueLength & 0xFFFFFFFFL);
      out.writeLong(combined);
      out.write(key.getData(), key.getPosition(), keyLength);
      out.write(value.getData(), value.getPosition(), valueLength);
  }

  public void appendNoRleTez(DataInputBuffer key, DataInputBuffer value) throws IOException {
      int keyLength = key.getLength() - key.getPosition();
      int valueLength = value.getLength() - value.getPosition();

      if (maxKeyLen < 0) {
        maxKeyLen = keyLength;
      }
      if (maxValLen < 0) {
        maxValLen = valueLength;
      }
      if (!exactMaxLensKnownAtInit) {
        out.writeInt(keyLength);
        out.writeInt(valueLength);
      } else {
        if (!keyLenTransitioned && keyLength != maxKeyLen) {
          keyLenTransitioned = true;
        }
        if (!valLenTransitioned && valueLength != maxValLen) {
          valLenTransitioned = true;
        }
        if (keyLenTransitioned) {
          out.writeInt(keyLength);
        }
        if (valLenTransitioned) {
          out.writeInt(valueLength);
        }
      }
      out.write(key.getData(), key.getPosition(), keyLength);
      out.write(value.getData(), value.getPosition(), valueLength);
  }

  public void appendRle(DataInputBuffer key, DataInputBuffer value) throws IOException {
    int keyLength = key.getLength() - key.getPosition();
    int valueLength = value.getLength() - value.getPosition();

    boolean sameKey = key == IFile.REPEAT_KEY;
    if (!sameKey) {
      sameKey = (keyLength != 0) && BufferUtils.compareEqual(previous, key);
    }

    if (!sameKey) {
      // Normal key-value pair
      // Write V_END_MARKER if needed (if previous was a REPEAT_KEY)
      if (prevKey == IFile.REPEAT_KEY) {
        out.writeInt(IFile.V_END_MARKER);
      }

      long combined = ((long) keyLength << 32) | (valueLength & 0xFFFFFFFFL);
      out.writeLong(combined);
      out.write(key.getData(), key.getPosition(), keyLength);
      out.write(value.getData(), value.getPosition(), valueLength);
      BufferUtils.copy(key, previous);
    } else {
      // Repeated key
      if (prevKey != IFile.REPEAT_KEY) {
        // First repeated key, write RLE marker
        out.writeInt(IFile.RLE_MARKER);
      }

      // Write just the value length and value
      out.writeInt(valueLength);
      out.write(value.getData(), value.getPosition(), valueLength);
    }

    prevKey = sameKey ? IFile.REPEAT_KEY : key;
  }

  public void close() throws IOException {
      if (isRleEnabled) {
          closeRle();
      }

      // Write EOF_MARKER for key/value length
      long combined = ((long) IFile.EOF_MARKER << 32) | (IFile.EOF_MARKER & 0xFFFFFFFFL);
      out.writeLong(combined);

      out.close();
      out = null;
  }

  private void closeRle() throws IOException {
      // Write V_END_MARKER if needed
      if (prevKey == IFile.REPEAT_KEY) {
          out.writeInt(IFile.V_END_MARKER);
      }
  }
}
