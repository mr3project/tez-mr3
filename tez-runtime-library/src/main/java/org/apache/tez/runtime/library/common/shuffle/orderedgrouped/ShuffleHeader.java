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
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Shuffle Header information that is sent by the TaskTracker and 
 * deciphered by the Fetcher thread of Reduce task
 *
 */
public class ShuffleHeader implements Writable {
  
  /** Header info of the shuffle http request/response */
  public static final String HTTP_HEADER_NAME = "name";
  public static final String DEFAULT_HTTP_HEADER_NAME = "mapreduce";
  public static final String HTTP_HEADER_VERSION = "version";
  public static final String DEFAULT_HTTP_HEADER_VERSION = "1.0.0";

  /**
   * The longest possible length of task attempt id that we will accept.
   */
  private static final int MAX_ID_LENGTH = 1000;

  String mapId;
  long uncompressedLength;
  long compressedLength;
  int forReduce;

  private boolean compositeFetch;

  // ShuffleHeader created by Fetcher before calling readFields()
  public ShuffleHeader(boolean compositeFetch) {
    this.compositeFetch = compositeFetch;
  }

  // ShuffleHeader created used by MR3 ShuffleHandler (but not by Hadoop shuffle service)
  public ShuffleHeader(String mapId, long compressedLength,
      long uncompressedLength, int forReduce) {
    this.mapId = mapId;
    this.compressedLength = compressedLength;
    this.uncompressedLength = uncompressedLength;
    this.forReduce = forReduce;
  }
  
  public String getMapId() {
    return this.mapId;
  }
  
  public int getPartition() {
    return this.forReduce;
  }
  
  public long getUncompressedLength() {
    return uncompressedLength;
  }

  public long getCompressedLength() {
    return compressedLength;
  }

  public void readFields(DataInput in) throws IOException {
    if (compositeFetch) {   // ShuffleHeader created by MR3 ShuffleHandler
      int length = in.readInt();  // Cf. WritableUtils.readStringSafely() calls readVInt()
      if (length < 0 || length > MAX_ID_LENGTH) {
        throw new IllegalArgumentException("Encoded byte size for String was " + length +
                                           ", which is outside of 0.." + MAX_ID_LENGTH + " range.");
      }
      byte [] bytes = new byte[length];
      in.readFully(bytes, 0, length);
      mapId = Text.decode(bytes);

      compressedLength = in.readLong();
      uncompressedLength = in.readLong();
      forReduce = in.readInt();
    } else {  // ShuffleHeader created by Hadoop shuffle service
      mapId = WritableUtils.readStringSafely(in, MAX_ID_LENGTH);
      compressedLength = WritableUtils.readVLong(in);
      uncompressedLength = WritableUtils.readVLong(in);
      forReduce = WritableUtils.readVInt(in);
    }
  }

  // called by MR3 ShuffleHandler (but not by Hadoop shuffle service)
  // do not use WritableUtils.writeVLong/Int()
  public int writeLength() throws IOException {
    int length = Text.encode(mapId).limit();
    length += 4 + 8 + 8 + 4;  // encoding of mapIdLength, compressedLength, uncompressedLength, forReduce
    return length;
  }

  // called by MR3 ShuffleHandler (but not by Hadoop shuffle service)
  // do not use WritableUtils.writeVLong/Int()
  public void write(DataOutput out) throws IOException {
    // Text.writeString(out, mapId);
    ByteBuffer bytes = Text.encode(mapId);
    int length = bytes.limit();
    out.writeInt(length);
    out.write(bytes.array(), 0, length);

    out.writeLong(compressedLength);
    out.writeLong(uncompressedLength);
    out.writeInt(forReduce);
  }
}
