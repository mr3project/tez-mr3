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
 * Minimal holder for a raw byte-array slice.
 *
 * <p>The length is the exclusive end offset, matching the raw-slice semantics used by the
 * runtime-library sort and merge paths.</p>
 */
public class RawDataBuffer {
  private static final byte[] EMPTY_BYTES = new byte[0];

  private byte[] data = EMPTY_BYTES;
  private int position;
  private int length;

  public RawDataBuffer() {
  }

  public RawDataBuffer(byte[] data, int length) {
    reset(data, length);
  }

  public RawDataBuffer(byte[] data, int position, int length) {
    reset(data, position, length);
  }

  public void reset(byte[] input, int length) {
    this.data = input;
    this.position = 0;
    this.length = length;
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
