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

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Random;

import org.junit.Test;

public class TestLz4CompressionProvider {

  private static final int BUFFER_SIZE = 128;

  @Test
  public void testFastCompressionRoundTrip() throws Exception {
    testRoundTrip(false);
  }

  @Test
  public void testHighCompressionRoundTrip() throws Exception {
    testRoundTrip(true);
  }

  private static void testRoundTrip(boolean useHighCompression) throws Exception {
    Lz4CompressionProvider provider =
        new Lz4CompressionProvider(BUFFER_SIZE, useHighCompression);
    byte[] original = new byte[BUFFER_SIZE * 2 + 17];
    new Random(12345L).nextBytes(original);

    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    Compressor compressor = provider.createCompressor();
    try (CompressionOutputStream output = provider.createOutputStream(encoded, compressor)) {
      output.write(original);
    } finally {
      compressor.close();
    }

    ByteArrayOutputStream decoded = new ByteArrayOutputStream();
    Decompressor decompressor = provider.createDecompressor();
    try (InputStream input = provider.createInputStream(
        new ByteArrayInputStream(encoded.toByteArray()), decompressor)) {
      byte[] buffer = new byte[31];
      int read;
      while ((read = input.read(buffer)) != -1) {
        decoded.write(buffer, 0, read);
      }
    } finally {
      decompressor.close();
    }

    assertArrayEquals(original, decoded.toByteArray());
  }
}
