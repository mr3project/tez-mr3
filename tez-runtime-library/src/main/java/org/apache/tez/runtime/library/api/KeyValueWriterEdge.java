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

package org.apache.tez.runtime.library.api;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.runtime.api.WriterEdge;

public abstract class KeyValueWriterEdge implements WriterEdge {

  // Invariant:
  //   1. closeWriter() must be called and is called only after the last call of write().
  //   2. write()/closeWriter() are called from the same thread (thus never concurrently).

  public abstract void closeWriter();

  /**
   * Writes a key/value pair.
   * 
   * @param key
   *          the key to write
   * @param value
   *          the value to write
   * @throws IOException
   *           if an error occurs
   * @throws {@link IOInterruptedException} if IO was interrupted
   * @throws {@link IOInterruptedException} if IO was performing a blocking operation and was interrupted
   */
  public abstract void write(BytesWritable key, BytesWritable value) throws IOException;

  // Return:
  //   >= 0: unordered edge and Tez shuffle
  //   -1: ordered edge or MapReduce shuffle
  public abstract int getNumUnorderedPartitions();

  public abstract void writeWithPartition(BytesWritable key, BytesWritable value, int partition) throws IOException;

  // return value = 0: use key hash to get partition
  // return value = 1: use value hash to get partition
  public abstract int getPartitionerType();

  /*
    The new methods are valid only when getNumUnorderedPartitions() returns an
    integer larger than 1.

    The caller first calls requestWriteValueBytes(key, partition) with a key and
    partition for a record whose value bytes will be produced later. The method is
    a strict current-buffer peek: it does not switch buffers, reserve space, write
    metadata, write the key, or update writer state.

    If requestWriteValueBytes() returns null, no direct value-byte region is
    available in the current buffer. The caller should use a fallback path, such as
    building a BytesWritable value and calling writeWithPartition().

    If requestWriteValueBytes() returns a non-null WriteValueBytes object, then:

      buffer
          is the internal byte array where value bytes may be written.

      offsetToValueBytes
          is the first index in buffer where the caller may write value bytes.

      maxValueBytes
          is the maximum number of value bytes the caller may write starting at
          offsetToValueBytes.

    The caller may write at most maxValueBytes bytes into:

      buffer[offsetToValueBytes ... offsetToValueBytes + maxValueBytes)

    After writing the value bytes directly into the buffer, the caller must call
    completeWriteValueBytes(key, valLen, partition), where valLen is the number of
    value bytes actually written (>= 0). The key and partition must correspond to the
    earlier requestWriteValueBytes() call.

    completeWriteValueBytes() commits the record as if writeWithPartition() had
    been called with a BytesWritable value of length valLen, except that it does
    not copy the value bytes because the caller already wrote them directly into
    the exposed buffer. It writes the key and metadata, advances writer positions,
    updates counters, and updates partition bookkeeping.

    No other write operation may occur on the same writer between a successful
    requestWriteValueBytes() call and its corresponding completeWriteValueBytes() call.
   */

  public static class WriteValueBytes {
    public final byte[] buffer;
    public final int offsetToValueBytes;
    public final int maxValueBytes;

    public WriteValueBytes(byte[] buffer, int offsetToValueBytes, int maxValueBytes) {
      this.buffer = buffer;
      this.offsetToValueBytes = offsetToValueBytes;
      this.maxValueBytes = maxValueBytes;
    }
  }

  public abstract WriteValueBytes requestWriteValueBytes(BytesWritable key, int partition) throws IOException;

  public abstract void completeWriteValueBytes(BytesWritable key, int valLen, int partition) throws IOException;
}
