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

import java.io.IOException;
import java.util.Arrays;

import org.xerial.snappy.Snappy;

/** Raw xerial Snappy block decompressor. */
public final class XerialSnappyDecompressor implements Decompressor {
  private byte[] scratch = new byte[0];
  private boolean closed;

  @Override
  public CompressionAlgorithm getAlgorithm() {
    return CompressionAlgorithm.SNAPPY;
  }

  @Override
  public int decompress(byte[] input, int inputOffset, int inputLength,
      byte[] output, int outputOffset, int outputCapacity) throws IOException {
    ensureOpen();
    XerialSnappyCompressor.checkRange(input, inputOffset, inputLength, "input");
    XerialSnappyCompressor.checkRange(output, outputOffset, outputCapacity, "output");
    try {
      int length = Snappy.uncompressedLength(input, inputOffset, inputLength);
      if (length < 0 || length > outputCapacity) {
        throw new IOException("Insufficient Snappy output capacity: need " + length
            + ", have " + outputCapacity);
      }
      if (scratch.length < length) {
        scratch = new byte[length];
      }
      int actual = Snappy.rawUncompress(input, inputOffset, inputLength, scratch, 0);
      if (actual != length) {
        throw new IOException("Corrupt Snappy block: expected " + length + " bytes, decoded " + actual);
      }
      System.arraycopy(scratch, 0, output, outputOffset, actual);
      return actual;
    } catch (IOException e) {
      throw e;
    } catch (Throwable e) {
      throw new IOException("Invalid or corrupt xerial Snappy block", e);
    }
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Decompressor is closed");
    }
  }

  @Override
  public void reset() {
    ensureOpen();
  }

  @Override
  public void close() {
    closed = true;
    Arrays.fill(scratch, (byte) 0);
    scratch = new byte[0];
  }
}
