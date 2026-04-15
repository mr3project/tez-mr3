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
  private final int maxKeyLen;
  private final int maxValLen;
  private final int firstKeyOffset;
  private final int firstValOffset;
  private final int eofPos;

  public TezOffsetRecord(
      int maxKeyLen,
      int maxValLen,
      int firstKeyOffset,
      int firstValOffset,
      int eofPos) {
    this.maxKeyLen = maxKeyLen;
    this.maxValLen = maxValLen;
    this.firstKeyOffset = firstKeyOffset;
    this.firstValOffset = firstValOffset;
    this.eofPos = eofPos;
  }

  public int getMaxKeyLen() {
    return maxKeyLen;
  }

  public int getMaxValLen() {
    return maxValLen;
  }

  public int getFirstKeyOffset() {
    return firstKeyOffset;
  }

  public int getFirstValOffset() {
    return firstValOffset;
  }

  public int getEofPos() {
    return eofPos;
  }
}
