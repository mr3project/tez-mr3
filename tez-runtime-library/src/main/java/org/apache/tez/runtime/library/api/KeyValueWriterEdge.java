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

  public abstract WriteValueBytes requestWriteValueBytes(BytesWritable key, int partition) throws IOException;

  public abstract void completeWriteValueBytes(BytesWritable key, int valLen, int partition) throws IOException;

  // return value = 0: use key hash to get partition
  // return value = 1: use value hash to get partition
  public abstract int getPartitionerType();
}
