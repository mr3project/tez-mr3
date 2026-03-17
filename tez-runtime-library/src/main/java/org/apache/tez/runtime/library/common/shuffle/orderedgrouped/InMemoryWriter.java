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
import org.apache.tez.runtime.library.common.sort.impl.IFile.SectionLayout;
import org.apache.tez.runtime.library.common.sort.impl.IFileOutputStream;

public class InMemoryWriter implements IFile.WriterAppend {

  // BoundedByteArrayOutputStream(array, 0, array.length) is protected and cannot be used directly
  private static class InMemoryBoundedByteArrayOutputStream extends BoundedByteArrayOutputStream {
    InMemoryBoundedByteArrayOutputStream(byte[] array) {
      super(array, 0, array.length);
    }
  }

  private final BoundedByteArrayOutputStream arrayStream;
  private final DataOutputBuffer keySectionBuffer = new DataOutputBuffer();
  private final DataOutputBuffer lengthsSectionBuffer = new DataOutputBuffer();

  private IFileOutputStream valuesChecksumOut;
  private DataOutputStream out;

  private long numRecordsWritten = 0;   // TODO: with RLE, we should use numKeysWritten

  // InMemoryWriter does not use another byte[] buffer, unlike IFile.Writer
  public InMemoryWriter(byte[] array) {
    this.arrayStream = new InMemoryBoundedByteArrayOutputStream(array);
    try {
      arrayStream.write(IFile.HEADER, 0, IFile.HEADER.length);  // assume uncompressed
      this.valuesChecksumOut = new IFileOutputStream(arrayStream);
      this.out = new NonSyncDataOutputStream(valuesChecksumOut);
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize InMemoryWriter", e);
    }
  }

  public void append(DataInputBuffer key, DataInputBuffer value) throws IOException {
    int keyLength = key.getLength() - key.getPosition();
    int valueLength = value.getLength() - value.getPosition();
    if (keyLength < 0 || valueLength < 0) {
      throw new IOException("Negative key/value lengths are not allowed. keyLength=" + keyLength
          + ", valueLength=" + valueLength);
    }

    out.write(value.getData(), value.getPosition(), valueLength);

    keySectionBuffer.write(key.getData(), key.getPosition(), keyLength);
    lengthsSectionBuffer.writeInt(keyLength);
    lengthsSectionBuffer.writeInt(valueLength);

    ++numRecordsWritten;
  }

  public void close() throws IOException {
      out.flush();
      valuesChecksumOut.finish();

      writeSection(keySectionBuffer);
      writeSection(lengthsSectionBuffer);

      out = null;
      valuesChecksumOut = null;
  }

  private void writeSection(DataOutputBuffer sectionBuffer) throws IOException {
    IFileOutputStream sectionChecksumOut = new IFileOutputStream(arrayStream);
    DataOutputStream sectionOut = new NonSyncDataOutputStream(sectionChecksumOut);
    sectionOut.write(sectionBuffer.getData(), 0, sectionBuffer.getLength());
    sectionOut.flush();
    sectionChecksumOut.finish();
  }

  // should be called after close()
  public SectionLayout getSectionLayout() {
    long keysLength = keySectionBuffer.getLength();
    long lengthsLength = lengthsSectionBuffer.getLength();
    long totalLength = arrayStream.size();

    long valuesLength = totalLength
        - IFile.HEADER.length
        - keysLength
        - lengthsLength
        - 3L * IFile.checksumSize;
    assert valuesLength >= 0;

    long valuesStart = IFile.HEADER.length;
    long keysStart = valuesStart + valuesLength + IFile.checksumSize;
    long lengthsStart = keysStart + keysLength + IFile.checksumSize;

    return new SectionLayout(
        valuesStart, valuesLength,
        keysStart, keysLength,
        lengthsStart, lengthsLength,
        totalLength, numRecordsWritten);
  }
}
