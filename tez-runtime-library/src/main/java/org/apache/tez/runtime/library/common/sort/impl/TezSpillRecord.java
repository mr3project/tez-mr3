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
package org.apache.tez.runtime.library.common.sort.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.functional.FutureIO;
import org.apache.tez.runtime.library.common.Constants;

public class TezSpillRecord {
  public static final FsPermission SPILL_FILE_PERMS = new FsPermission((short) 0640);

  /** Backing store */
  private final ByteBuffer buf;
  /** View of backing storage as longs */
  private final LongBuffer entries;

  public TezSpillRecord(int numPartitions) {
    buf = ByteBuffer.allocate(numPartitions * Constants.MAP_OUTPUT_INDEX_RECORD_LENGTH);
    entries = buf.asLongBuffer();
  }

  public TezSpillRecord(Path indexFileName, FileSystem fs, String expectedIndexOwner)
    throws IOException {
    this(indexFileName, fs);
  }

  public TezSpillRecord(Path indexFileName, FileSystem rfs) throws IOException {
    assert indexFileName != null;   // writeSpillRecord should be false in UnorderedPartitionedKVWriter/PipelinedSorter
    Checksum crc = new PureJavaCrc32();

    FileStatus fileStatus = rfs.getFileStatus(indexFileName);
    final long length = fileStatus.getLen();
    try (FSDataInputStream in = FutureIO.awaitFuture(rfs.openFile(indexFileName).withFileStatus(fileStatus).build())) {
      final int partitions = (int) length / Constants.MAP_OUTPUT_INDEX_RECORD_LENGTH;
      final int size = partitions * Constants.MAP_OUTPUT_INDEX_RECORD_LENGTH;

      buf = ByteBuffer.allocate(size);
      crc.reset();
      CheckedInputStream chk = new CheckedInputStream(in, crc);
      IOUtils.readFully(chk, buf.array(), 0, size);
      if (chk.getChecksum().getValue() != in.readLong()) {
        throw new ChecksumException("Checksum error reading spill index: " + indexFileName, -1);
      }
      entries = buf.asLongBuffer();
    }
  }

  public TezSpillRecord(ByteBuffer buf) {
    this.buf = buf;
    this.entries = buf.asLongBuffer();
  }

  public ByteBuffer getByteBuffer() {
    return buf;
  }

  public boolean equalByteBuffer(ByteBuffer otherBuf) {
      if (otherBuf == null) {
          return false;
      }
      if (buf == otherBuf) {
          return true;
      }
      if (buf.capacity() != otherBuf.capacity()) {
          return false;
      }
      for (int i = 0; i < buf.capacity(); i++) {
          if (buf.get(i) != otherBuf.get(i)) {
              return false;
          }
      }
      return true;
  }

  /**
   * Return number of IndexRecord entries in this spill.
   */
  public int size() {
    return entries.capacity() / (Constants.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8);
  }

  /**
   * Get spill offsets for given partition.
   */
  public TezIndexRecord getIndex(int partition) {
    final int pos = partition * Constants.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8;
    return new TezIndexRecord(entries.get(pos), entries.get(pos + 1), entries.get(pos + 2));
  }

  /**
   * Set spill offsets for given partition.
   */
  public void putIndex(TezIndexRecord rec, int partition) {
    final int pos = partition * Constants.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8;
    entries.put(pos, rec.getStartOffset());
    entries.put(pos + 1, rec.getRawLength());
    entries.put(pos + 2, rec.getPartLength());
  }

  /**
   * Write this spill record to the location provided.
   */
  public void writeToFile(Path loc, FileSystem rfs) throws IOException {
    PureJavaCrc32 crc = new PureJavaCrc32();
    CheckedOutputStream chk = null;
    final FSDataOutputStream out = rfs.create(loc);
    try {
      crc.reset();
      chk = new CheckedOutputStream(out, crc);
      chk.write(buf.array());
      out.writeLong(chk.getChecksum().getValue());
    } finally {
      if (chk != null) {
        chk.close();
      } else {
        out.close();
      }
      ensureSpillFilePermissions(loc, rfs);
    }
  }

  public static void ensureSpillFilePermissions(Path loc, FileSystem rfs) throws IOException {
    if (!SPILL_FILE_PERMS.equals(SPILL_FILE_PERMS.applyUMask(FsPermission.getUMask(rfs.getConf())))) {
      rfs.setPermission(loc, SPILL_FILE_PERMS);
    }
  }
}
