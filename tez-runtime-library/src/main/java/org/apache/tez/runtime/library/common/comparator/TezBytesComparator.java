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
package org.apache.tez.runtime.library.common.comparator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.util.FastByteComparisons;

public final class TezBytesComparator {

  private TezBytesComparator() {}

  /**
   * Compare the buffers in serialized form.
   */
  // copy of FastByteComparisons.UnsafeComparer.compareTo()
  public static int compare(byte[] buffer1, int offset1, int length1,
                            byte[] buffer2, int offset2, int length2) {
    assert !(buffer1 == buffer2 && offset1 == offset2);

    final int stride = 8;
    int minLength = Math.min(length1, length2);
    int strideLimit = minLength & ~(stride - 1);
    int offset1Adj = offset1 + FastByteComparisons.BYTE_ARRAY_BASE_OFFSET;
    int offset2Adj = offset2 + FastByteComparisons.BYTE_ARRAY_BASE_OFFSET;
    int i;

    for (i = 0; i < strideLimit; i += stride) {
      long lw = FastByteComparisons.theUnsafe.getLong(buffer1, offset1Adj + (long) i);
      long rw = FastByteComparisons.theUnsafe.getLong(buffer2, offset2Adj + (long) i);

      if (lw != rw) {
        long bw = Long.reverseBytes(lw);
        long br = Long.reverseBytes(rw);
        return Long.compareUnsigned(bw, br);
      }
    }

    for (; i < minLength; i++) {
      int b1 = buffer1[offset1 + i] & 0xFF;
      int b2 = buffer2[offset2 + i] & 0xFF;
      if (b1 != b2) {
        return b1 - b2;
      }
    }
    return length1 - length2;
  }

  public static int compare(BytesWritable key1, BytesWritable key2) {
    return compare(
        key1.getBytesRaw(), key1.getOffset(), key1.getLength(),
        key2.getBytesRaw(), key2.getOffset(), key2.getLength());
  }

  public static int getProxy(BytesWritable key) {
    final int len = key.getLength();
    final byte[] content = key.getBytesRaw();
    final int offset = key.getOffset();

    switch (len) {
      default:
        return ((content[offset] & 0xff) << 24)
            | ((content[offset + 1] & 0xff) << 16)
            | ((content[offset + 2] & 0xff) << 8);
      case 2:
        return ((content[offset] & 0xff) << 24)
            | ((content[offset + 1] & 0xff) << 16);
      case 1:
        return (content[offset] & 0xff) << 24;
      case 0:
        return 0;
    }
  }
}
