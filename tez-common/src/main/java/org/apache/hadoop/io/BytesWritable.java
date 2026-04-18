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

package org.apache.hadoop.io;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** 
 * A byte sequence that is usable as a key or value.
 * It is resizable and distinguishes between the size of the sequence and
 * the current capacity. The hash function is the front of the md5 of the 
 * buffer. The sort order is the same as memcmp.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BytesWritable extends BinaryComparable
    implements WritableComparable<BinaryComparable> {
  private static final int LENGTH_BYTES = 4;
  private static final byte[] EMPTY_BYTES = {};

  private int offset;
  private int size;
  private byte[] bytes;
  
  /**
   * Create a zero-size sequence.
   */
  public BytesWritable() {this(EMPTY_BYTES);}
  
  /**
   * Create a BytesWritable using the byte array as the initial value.
   * @param bytes This array becomes the backing storage for the object.
   */
  public BytesWritable(byte[] bytes) {
    this(bytes, 0, bytes.length);
  }

  /**
   * Create a BytesWritable using the byte array as the initial value
   * and length as the length. Use this constructor if the array is larger
   * than the value it represents.
   * @param bytes This array becomes the backing storage for the object.
   * @param length The number of bytes to use from array.
   */
  public BytesWritable(byte[] bytes, int length) {
    this(bytes, 0, length);
  }

  /**
   * Create a BytesWritable using the byte array as the initial value,
   * with an offset and length describing the logical payload.
   *
   * @param bytes This array becomes the backing storage for the object.
   * @param offset The start of the logical payload in the backing array.
   * @param length The number of bytes in the logical payload.
   */
  public BytesWritable(byte[] bytes, int offset, int length) {
    if (offset < 0 || length < 0 || offset > bytes.length - length) {
      throw new IllegalArgumentException(
          "Invalid offset/length for array. offset=" + offset
              + ", length=" + length + ", arrayLength=" + bytes.length);
    }
    this.offset = offset;
    this.bytes = bytes;
    this.size = length;
  }
  
  /**
   * Get a copy of the bytes that is exactly the length of the data.
   * See {@link #getBytes()} for faster access to the underlying array.
   *
   * @return copyBytes.
   */
  public byte[] copyBytes() {
    byte[] result = new byte[size];
    System.arraycopy(bytes, offset, result, 0, size);
    return result;
  }
  
  /**
   * Get the data backing the BytesWritable. Please use {@link #copyBytes()}
   * if you need the returned array to be precisely the length of the data.
   * @return The data is only valid between 0 and getLength() - 1.
   */
  @Override
  public byte[] getBytes() {
    normalize();
    return bytes;
  }

  /**
   * Get the data from the BytesWritable.
   * @deprecated Use {@link #getBytes()} instead.
   * @return data from the BytesWritable.
   */
  @Deprecated
  public byte[] get() {
    return getBytes();
  }

  /**
   * Get the current size of the buffer.
   */
  @Override
  public int getLength() {
    return size;
  }

  /**
   * Get the current size of the buffer.
   * @deprecated Use {@link #getLength()} instead.
   * @return current size of the buffer.
   */
  @Deprecated
  public int getSize() {
    return getLength();
  }
  
  /**
   * Change the size of the buffer. The values in the old range are preserved
   * and any new values are undefined. The capacity is changed if it is 
   * necessary.
   * @param size The new number of bytes
   */
  public void setSize(int size) {
    normalize();
    if (size > getCapacity()) {
      // Avoid overflowing the int too early by casting to a long.
      long newSize = Math.min(Integer.MAX_VALUE, (3L * size) / 2L);
      setCapacity((int) newSize);
    }
    this.size = size;
  }
  
  /**
   * Get the capacity, which is the maximum size that could handled without
   * resizing the backing storage.
   * @return The number of bytes
   */
  public int getCapacity() {
    return bytes.length;
  }
  
  /**
   * Change the capacity of the backing storage.
   * The data is preserved.
   * @param new_cap The new capacity in bytes.
   */
  public void setCapacity(int new_cap) {
    if (new_cap != getCapacity()) {
      byte[] new_data = new byte[new_cap];
      if (new_cap < size) {
        size = new_cap;
      }
      if (size != 0) {
        System.arraycopy(bytes, offset, new_data, 0, size);
      }
      bytes = new_data;
      offset = 0;
    } else {
      normalize();
    }
  }

  /**
   * Set the BytesWritable to the contents of the given newData.
   * @param newData the value to set this BytesWritable to.
   */
  public void set(BytesWritable newData) {
    set(newData.bytes, newData.offset, newData.size);
  }

  /**
   * Set the value to a copy of the given byte range
   * @param newData the new values to copy in
   * @param offset the offset in newData to start at
   * @param length the number of bytes to copy
   */
  public void set(byte[] newData, int offset, int length) {
    if (offset < 0 || length < 0 || offset > newData.length - length) {
      throw new IllegalArgumentException(
          "Invalid offset/length for array. offset=" + offset
              + ", length=" + length + ", arrayLength=" + newData.length);
    }
    byte[] newBytes = new byte[length];
    if (length != 0) {
      System.arraycopy(newData, offset, newBytes, 0, length);
    }
    this.bytes = newBytes;
    this.offset = 0;
    this.size = length;
  }

  // inherit javadoc
  @Override
  public void readFields(DataInput in) throws IOException {
    int newSize = in.readInt();
    byte[] newBytes = new byte[newSize];
    in.readFully(newBytes, 0, newSize);
    this.bytes = newBytes;
    offset = 0;
    this.size = newSize;
  }
  
  // inherit javadoc
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(size);
    out.write(bytes, offset, size);
  }
  
  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /**
   * Are the two byte sequences equal?
   */
  @Override
  public boolean equals(Object right_obj) {
    if (right_obj instanceof BytesWritable)
      return super.equals(right_obj);
    return false;
  }

  /**
   * Generate the stream of bytes as hex pairs separated by ' '.
   */
  @Override
  public String toString() { 
    StringBuilder sb = new StringBuilder(3*size);
    for (int idx = 0; idx < size; idx++) {
      // if not the first, put a blank separator in
      if (idx != 0) {
        sb.append(' ');
      }
      String num = Integer.toHexString(0xff & bytes[offset + idx]);
      // if it is only one digit, add a leading 0.
      if (num.length() < 2) {
        sb.append('0');
      }
      sb.append(num);
    }
    return sb.toString();
  }

  /** A Comparator optimized for BytesWritable. */ 
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(BytesWritable.class);
    }
    
    /**
     * Compare the buffers in serialized form.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      return compareBytes(b1, s1+LENGTH_BYTES, l1-LENGTH_BYTES, 
                          b2, s2+LENGTH_BYTES, l2-LENGTH_BYTES);
    }
  }
  
  static {                                        // register this comparator
    WritableComparator.define(BytesWritable.class, new Comparator());
  }

  /**
   * Normalize the payload so that offset is zero.
   * This preserves compatibility for existing callers that assume data starts
   * at bytes[0].
   */
  private void normalize() {
    if (offset != 0) {
      byte[] newBytes = new byte[bytes.length];
      if (size != 0) {
        System.arraycopy(bytes, offset, newBytes, 0, size);
      }
      bytes = newBytes;
      offset = 0;
    }
  }
  
}
