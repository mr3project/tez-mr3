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

package org.apache.tez.runtime.library.common.shuffle;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tez.runtime.library.common.sort.impl.IFile;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RssShuffleUtils {

  public static final int EOF_MARKERS_SIZE = 2 * WritableUtils.getVIntSize(IFile.EOF_MARKER);

  private static final int BUFFER_SIZE = 64 * 1024;

  public static void shuffleToMemory(InputStream inputStream, byte[] buffer, long dataLength)
      throws IOException {
    IOUtils.readFully(inputStream, buffer, 0, (int) dataLength);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    WritableUtils.writeVInt(dos, IFile.EOF_MARKER);
    WritableUtils.writeVInt(dos, IFile.EOF_MARKER);
    byte[] eofMarker = baos.toByteArray();

    System.arraycopy(eofMarker, 0, buffer, (int) dataLength, EOF_MARKERS_SIZE);
  }

  public static long shuffleToDisk(InputStream inputStream, OutputStream outputStream,
      long dataLength) throws IOException {
    byte[] buffer = new byte[BUFFER_SIZE];
    boolean reachEOF = false;
    long bytesWritten = 0L;
    while (!reachEOF) {
      int curBytesRead = inputStream.read(buffer, 0, BUFFER_SIZE);

      if (curBytesRead <= 0) {
        reachEOF = true;
      } else {
        reachEOF = curBytesRead < BUFFER_SIZE;

        outputStream.write(buffer, 0, curBytesRead);
        bytesWritten += curBytesRead;
      }
    }

    assert !(dataLength != -1L) || dataLength == bytesWritten;

    DataOutputStream dos = new DataOutputStream(outputStream);
    WritableUtils.writeVInt(dos, IFile.EOF_MARKER);
    WritableUtils.writeVInt(dos, IFile.EOF_MARKER);
    bytesWritten += EOF_MARKERS_SIZE;

    return bytesWritten;
  }
}
