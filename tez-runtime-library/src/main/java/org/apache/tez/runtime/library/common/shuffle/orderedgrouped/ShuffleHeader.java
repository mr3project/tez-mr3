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
import org.apache.tez.runtime.library.common.sort.impl.IFile;

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
  IFile.SectionLayout sectionLayout;


  // For compiling TestShuffleHandler.java in Hive-MR3
  public ShuffleHeader() {
  }


  // ShuffleHeader created used by MR3 ShuffleHandler (but not by Hadoop shuffle service)
  public ShuffleHeader(String mapId, long compressedLength,
      long uncompressedLength, int forReduce, IFile.SectionLayout sectionLayout) {
    this.mapId = mapId;
    this.compressedLength = compressedLength;
    this.uncompressedLength = uncompressedLength;
    this.forReduce = forReduce;
    this.sectionLayout = sectionLayout;
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

  public IFile.SectionLayout getSectionLayout() {
    return sectionLayout;
  }

  public void readFields(DataInput in) throws IOException {
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
    if (compressedLength > 0) {
      long valuesStart = in.readLong();
      long keysStart = in.readLong();
      long lengthsStart = in.readLong();
      long totalNumRecordsWritten = in.readLong();
      long valuesRawLength = in.readLong();
      long keysRawLength = in.readLong();
      long lengthsRawLength = in.readLong();
      sectionLayout = new IFile.SectionLayout(
          valuesStart, keysStart, lengthsStart,
          compressedLength, totalNumRecordsWritten,
          valuesRawLength, keysRawLength, lengthsRawLength);
    } else {
      sectionLayout = null;
    }
  }

  // called by MR3 ShuffleHandler (but not by Hadoop shuffle service)
  // do not use WritableUtils.writeVLong/Int()
  public int writeLength() throws IOException {
    int length = Text.encode(mapId).limit();
    length += 4 + 8 + 8 + 4;  // encoding of mapIdLength, compressedLength, uncompressedLength, forReduce
    if (compressedLength > 0) {
      length += 7 * 8;
    }
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
    if (compressedLength > 0) {
      if (sectionLayout == null) {
        throw new IOException("SectionLayout is required for non-empty shuffle header");
      }
      out.writeLong(sectionLayout.valuesStart);
      out.writeLong(sectionLayout.keysStart);
      out.writeLong(sectionLayout.lengthsStart);
      out.writeLong(sectionLayout.totalNumRecordsWritten);
      out.writeLong(sectionLayout.valuesRawLength);
      out.writeLong(sectionLayout.keysRawLength);
      out.writeLong(sectionLayout.lengthsRawLength);
    }
  }
}
