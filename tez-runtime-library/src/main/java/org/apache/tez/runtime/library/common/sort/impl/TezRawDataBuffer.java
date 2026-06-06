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

/**
 * Minimal byte-array slice holder used for raw intermediate keys and values.
 *
 * <p>The ordered shuffle/merge paths only need a buffer to describe the
 * current key/value bytes. Hadoop's {@code DataInputBuffer} also carries
 * {@code InputStream}/{@code DataInput} machinery which is unused on these
 * hot paths. This class intentionally exposes the small subset of methods that
 * those callers use, while preserving {@code DataInputBuffer}'s offset/length
 * convention: {@link #getPosition()} is the slice start and
 * {@link #getLength()} is the exclusive end offset in the backing array.</p>
 */
public class TezRawDataBuffer {
  private static final byte[] EMPTY_BYTES = new byte[0];

  private byte[] data = EMPTY_BYTES;
  private int position;
  private int length;

  public TezRawDataBuffer() {
  }

  public TezRawDataBuffer(byte[] data, int length) {
    reset(data, length);
  }

  public TezRawDataBuffer(byte[] data, int position, int length) {
    reset(data, position, length);
  }

  public void reset(byte[] input, int length) {
    reset(input, 0, length);
  }

  public void reset(byte[] input, int position, int length) {
    this.data = input;
    this.position = position;
    this.length = position + length;
  }

  public byte[] getData() {
    return data;
  }

  public int getPosition() {
    return position;
  }

  public int getLength() {
    return length;
  }

  public int getRemaining() {
    return length - position;
  }
}
