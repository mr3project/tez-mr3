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
package org.apache.tez.runtime.api;

/**
 * Metadata needed to decode non-RLE Tez IFile records with variable length-prefix transitions.
 */
public class TezOffsetRecord {

  // values for maxKeyLen
  public static final int VECTOR_BATCH = -1;

  // not used for the field maxKeyLen
  // used in ShuffleHandler.writeCompositePartition() to mark 'no record' in the position of maxKeyLen
  public static final int NO_RECORD = -2;

  private final int maxKeyLen;
  private final int maxValLen;
  private final int firstKeyOffset;
  private final int firstValOffset;
  private final int eofPos;

  public static TezOffsetRecord vectorBatch(int eofPos) {
    return new TezOffsetRecord(VECTOR_BATCH, 0, 0, 0, eofPos);
  }

  // Invariant: We create TezOffsetRecord only for IFile with at least one logical record.
  public TezOffsetRecord(
      int maxKeyLen,
      int maxValLen,
      int firstKeyOffset,
      int firstValOffset,
      int eofPos) {
    assert eofPos > 0;  // TODO: throw IOException because data can be corrupted while fetching over network

    this.maxKeyLen = maxKeyLen;
    this.maxValLen = maxValLen;
    this.firstKeyOffset = firstKeyOffset;
    this.firstValOffset = firstValOffset;
    this.eofPos = eofPos;
  }

  // Invariants:
  //  1. For every valid internally generated TezOffsetRecord:
  //     isVectorBatch() == true iff the associated IFile contains logical vector-batch records.
  //  2. A TezOffsetRecord is created and attached only for an IFile containing at least one logical record.
  //  3. A successfully generated empty output partition is reported through DME empty-partition metadata
  //     and is not delivered to a reader as a physical FetchedInput.
  //  4. A vector TezOffsetRecord identifies logical vector-batch records. Their physical layout is
  //     the ordinary non-RLE key/value layout with a zero-length key.

  public boolean isVectorBatch() {
    return maxKeyLen == VECTOR_BATCH;
  }

  public int getMaxKeyLen() {
    return maxKeyLen;
  }

  public int getMaxValLen() {
    assert maxKeyLen != VECTOR_BATCH;
    return maxValLen;
  }

  public int getFirstKeyOffset() {
    assert maxKeyLen != VECTOR_BATCH;
    return firstKeyOffset;
  }

  public int getFirstValOffset() {
    assert maxKeyLen != VECTOR_BATCH;
    return firstValOffset;
  }

  public int getEofPos() {
    return eofPos;
  }
}
