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
package org.apache.tez.runtime.io.compress;

final class CompressionStreamUtils {
  private CompressionStreamUtils() {
  }

  static void checkRange(byte[] array, int offset, int length, String name) {
    if (offset < 0 || length < 0 || offset > array.length - length) {
      throw new IndexOutOfBoundsException(
          name + " range: offset=" + offset + ", length=" + length
              + ", arrayLength=" + array.length);
    }
  }

  static int nextBufferSize(int currentLength, int requiredLength, int maxLength) {
    assert currentLength >= 0;
    assert requiredLength >= 0;
    assert requiredLength <= maxLength;
    assert maxLength >= 8;

    int newLength = currentLength == 0 ? maxLength / 8 : currentLength;
    while (newLength < requiredLength) {
      newLength = Math.min(newLength * 2, maxLength);
    }
    return newLength;
  }
}
