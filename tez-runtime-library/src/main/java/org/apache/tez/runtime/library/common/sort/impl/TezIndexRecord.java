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

public class TezIndexRecord {
  private long startOffset;
  private long rawLength;
  private long partLength;
  private long keySectionOffset;
  private long lengthSectionOffset;

  /**
   * @param startOffset start offset within the data file
   * @param rawLength raw data length - typically uncompressed
   * @param partLength actual data length in file - factors in checksums and compression
   */
  public TezIndexRecord(long startOffset, long rawLength, long partLength) {
    this(startOffset, rawLength, partLength, -1, -1);
  }

  /**
   * @param startOffset start offset within the data file
   * @param rawLength raw data length - typically uncompressed
   * @param partLength actual data length in file - factors in checksums and compression
   * @param keySectionOffset absolute file offset where the keys section begins
   * @param lengthSectionOffset absolute file offset where the lengths section begins
   */
  public TezIndexRecord(long startOffset, long rawLength, long partLength,
                        long keySectionOffset, long lengthSectionOffset) {
    this.startOffset = startOffset;
    this.rawLength = rawLength;
    this.partLength = partLength;
    this.keySectionOffset = keySectionOffset;
    this.lengthSectionOffset = lengthSectionOffset;
  }

  public long getStartOffset() {
    return startOffset;
  }

  public long getRawLength() {
    return rawLength;
  }

  public long getPartLength() {
    return partLength;
  }

  public long getKeySectionOffset() {
    return keySectionOffset;
  }

  public long getLengthSectionOffset() {
    return lengthSectionOffset;
  }

  public boolean hasData() {
    //TEZ-941 - Avoid writing out empty partitions
    //EOF_MARKER + Header bytes
    return !(rawLength <= (IFile.HEADER.length + 2));
  }
}
