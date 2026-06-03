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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.CRC32;

/**
 * A Checksum output stream.
 * Checksum for the contents of the file is calculated and
 * appended to the end of the file on close of the stream.
 * Used for IFiles
 */
public class IFileOutputStream extends FilterOutputStream {

  private static final int CHECKSUM_SIZE = Integer.BYTES;

  /**
   * The output stream to be checksummed.
   */
  private final CRC32 sum;
  private final byte[] checksum = new byte[CHECKSUM_SIZE];
  private boolean closed = false;
  private boolean finished = false;

  /**
   * Create a checksum output stream that writes
   * the bytes to the given stream.
   * @param out
   */
  public IFileOutputStream(OutputStream out) {
    super(out);
    sum = new CRC32();
  }

  public static int getCheckSumSize() {
    return CHECKSUM_SIZE;
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    finish();
    out.close();
  }

  /**
   * Finishes writing data to the output stream, by writing
   * the checksum bytes to the end. The underlying stream is not closed.
   * @throws IOException
   */
  public void finish() throws IOException {
    if (finished) {
      return;
    }
    finished = true;
    writeChecksumValue(checksum, 0, sum.getValue());
    out.write(checksum, 0, CHECKSUM_SIZE);
    out.flush();
  }

  static void writeChecksumValue(byte[] b, int off, long value) {
    for (int i = 0; i < CHECKSUM_SIZE; i++) {
      b[off + i] = (byte) (value >>> (Byte.SIZE * i));
    }
  }

  static boolean checksumMatches(byte[] b, int off, long value) {
    for (int i = 0; i < CHECKSUM_SIZE; i++) {
      if (b[off + i] != (byte) (value >>> (Byte.SIZE * i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Write bytes to the stream.
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    sum.update(b, off, len);
    out.write(b, off, len);
  }

  @Override
  public void write(int b) throws IOException {
    sum.update(b & 0xff);
    out.write(b);
  }

}
