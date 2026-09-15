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
import java.io.InputStream;

/** Reader for Tez's provider-private LZ4 block framing. */
final class Lz4CompressionInputStream extends BlockCompressionInputStream {

  private final Lz4JniDecompressor decompressor;

  Lz4CompressionInputStream(
      InputStream input, Lz4JniDecompressor decompressor, int bufferSize,
      int maximumCompressedLength)
      throws IOException {
    super(input, bufferSize, maximumCompressedLength, Lz4CompressionOutputStream.MAGIC, "LZ4");
    this.decompressor = decompressor;
  }

  @Override
  byte[] ensureCompressedCapacity(int length) {
    return decompressor.ensureCompressedCapacity(length);
  }

  @Override
  int decompressBuffered(int inputLength, int outputCapacity) throws IOException {
    return decompressor.decompressBuffered(inputLength, outputCapacity);
  }

  @Override
  byte[] getDecompressedBuffer() {
    return decompressor.getDecompressedBuffer();
  }

  @Override
  void resetDecompressor() {
    decompressor.reset();
  }
}
