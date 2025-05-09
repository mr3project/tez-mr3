package org.apache.tez.runtime.library.utils;

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

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;

import sun.misc.Unsafe;

/**
 * Same as {@link org.apache.hadoop.io.FastByteComparisons}
 *
 * Utility code to do optimized byte-array comparison.
 * This is borrowed and slightly modified from Guava's {@link com.google.common.primitives.UnsignedBytes}
 * class to be able to compare arrays that start at non-zero offsets.
 */
public final class FastByteComparisons {

  /**
   * Lexicographically compare two byte arrays.
   */
  public static int compareTo(byte[] b1, int s1, int l1, byte[] b2, int s2,
      int l2) {
    return LexicographicalComparerHolder.BEST_COMPARER.compareTo(
        b1, s1, l1, b2, s2, l2);
  }

  /* Determine if two strings are equal from two byte arrays each
   * with their own start position and length.
   * Use lexicographic unsigned byte value order.
   * This is what's used for UTF-8 sort order.
   */
  public static boolean compareEqual(byte[] arg1, final int start1, final int len1,
                                     byte[] arg2, final int start2, final int len2) {
    if (len1 != len2) {
      return false;
    }
    if (len1 == 0) {
      return true;
    }

    // do bounds check for OOB exception
    if (arg1[start1] != arg2[start2]
      || arg1[start1 + len1 - 1] != arg2[start2 + len2 - 1]) {
      return false;
    }

    if (len1 == len2) {
      // prove invariant to the compiler: len1 = len2
      // all array access between (start1, start1+len1)
      // and (start2, start2+len2) are valid
      // no more OOB exceptions are possible
      final int step = 8;
      final int remainder = len1 % step;
      final int wlen = len1 - remainder;
      // suffix first
      for (int i = wlen; i < len1; i++) {
        if (arg1[start1 + i] != arg2[start2 + i]) {
          return false;
        }
      }
      // SIMD loop
      for (int i = 0; i < wlen; i += step) {
        final int s1 = start1 + i;
        final int s2 = start2 + i;
        boolean neq = false;
        for (int j = 0; j < step; j++) {
          neq = (arg1[s1 + j] != arg2[s2 + j]) || neq;
        }
        if (neq) {
          return false;
        }
      }
    }

    return true;
  }

  private interface Comparer<T> {
    abstract public int compareTo(T buffer1, int offset1, int length1,
        T buffer2, int offset2, int length2);
  }

  private static Comparer<byte[]> lexicographicalComparerJavaImpl() {
    return LexicographicalComparerHolder.PureJavaComparer.INSTANCE;
  }


  /**
   * Provides a lexicographical comparer implementation; either a Java
   * implementation or a faster implementation based on {@link sun.misc.Unsafe}.
   *
   * <p>Uses reflection to gracefully fall back to the Java implementation if
   * {@code Unsafe} isn't available.
   */
  private static class LexicographicalComparerHolder {
    static final String UNSAFE_COMPARER_NAME =
        LexicographicalComparerHolder.class.getName() + "$UnsafeComparer";

    static final Comparer<byte[]> BEST_COMPARER = getBestComparer();
    /**
     * Returns the Unsafe-using Comparer, or falls back to the pure-Java
     * implementation if unable to do so.
     */
    static Comparer<byte[]> getBestComparer() {
      try {
        Class<?> theClass = Class.forName(UNSAFE_COMPARER_NAME);

        // yes, UnsafeComparer does implement Comparer<byte[]>
        @SuppressWarnings("unchecked")
        Comparer<byte[]> comparer =
            (Comparer<byte[]>) theClass.getEnumConstants()[0];
        return comparer;
      } catch (Throwable t) { // ensure we really catch *everything*
        return lexicographicalComparerJavaImpl();
      }
    }

    private enum PureJavaComparer implements Comparer<byte[]> {
      INSTANCE;

      @Override
      public int compareTo(byte[] buffer1, int offset1, int length1,
          byte[] buffer2, int offset2, int length2) {
        // Short circuit equal case
        if (buffer1 == buffer2 &&
            offset1 == offset2 &&
            length1 == length2) {
          return 0;
        }
        // Bring WritableComparator code local
        int end1 = offset1 + length1;
        int end2 = offset2 + length2;
        for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
          int a = (buffer1[i] & 0xff);
          int b = (buffer2[j] & 0xff);
          if (a != b) {
            return a - b;
          }
        }
        return length1 - length2;
      }
    }

    @SuppressWarnings("unused") // used via reflection
    private enum UnsafeComparer implements Comparer<byte[]> {
      INSTANCE;

      static final Unsafe theUnsafe;

      /** The offset to the first element in a byte array. */
      static final int BYTE_ARRAY_BASE_OFFSET;

      static {
        if (ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
          throw new AssertionError("ERROR: Big-endian architectures are not supported.");
        }

        theUnsafe = (Unsafe) AccessController.doPrivileged(
            new PrivilegedAction<Object>() {
              @Override
              public Object run() {
                try {
                  Field f = Unsafe.class.getDeclaredField("theUnsafe");
                  f.setAccessible(true);
                  return f.get(null);
                } catch (NoSuchFieldException e) {
                  // It doesn't matter what we throw;
                  // it's swallowed in getBestComparer().
                  throw new Error();
                } catch (IllegalAccessException e) {
                  throw new Error();
                }
              }
            });

        BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);

        // sanity check - this should never fail
        if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
          throw new AssertionError();
        }
      }

      /**
       * Returns true if x1 is less than x2, when both values are treated as
       * unsigned.
       */
      static boolean lessThanUnsigned(long x1, long x2) {
        return (x1 + Long.MIN_VALUE) < (x2 + Long.MIN_VALUE);
      }

      /**
       * Lexicographically compare two arrays.
       *
       * @param buffer1 left operand
       * @param buffer2 right operand
       * @param offset1 Where to start comparing in the left buffer
       * @param offset2 Where to start comparing in the right buffer
       * @param length1 How much to compare from the left buffer
       * @param length2 How much to compare from the right buffer
       * @return 0 if equal, < 0 if left is less than right, etc.
       */
      @Override
      public int compareTo(byte[] buffer1, int offset1, int length1,
          byte[] buffer2, int offset2, int length2) {
        if (buffer1 == buffer2 &&
            offset1 == offset2 &&
            length1 == length2) {
          return 0;
        }

        final int stride = 8;
        int minLength = Math.min(length1, length2);
        int strideLimit = minLength & ~(stride - 1);
        int offset1Adj = offset1 + BYTE_ARRAY_BASE_OFFSET;
        int offset2Adj = offset2 + BYTE_ARRAY_BASE_OFFSET;
        int i;

        for (i = 0; i < strideLimit; i += stride) {
          long lw = theUnsafe.getLong(buffer1, offset1Adj + (long) i);
          long rw = theUnsafe.getLong(buffer2, offset2Adj + (long) i);

          if (lw != rw) {
            // original approach (slower in JMH testing)
            // int n = Long.numberOfTrailingZeros(lw ^ rw) & ~0x7;
            // return ((int) ((lw >>> n) & 0xFF)) - ((int) ((rw >>> n) & 0xFF));
            long bw = Long.reverseBytes(lw);
            long br = Long.reverseBytes(rw);
            return Long.compareUnsigned(bw, br);
          }
        }

        for (; i < minLength; i++) {
          // do not use UnsignedBytes.compare() because we want to avoid Guava
          int b1 = buffer1[offset1 + i] & 0xFF;
          int b2 = buffer2[offset2 + i] & 0xFF;
          if (b1 != b2) {
            return b1 - b2;
          }
        }
        return length1 - length2;
      }
    }
  }
}
