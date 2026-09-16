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
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Field;

import org.apache.tez.runtime.api.CompressionProvider;
import org.junit.Test;

public class TestPerDagDecompressors {

  private static final int MAX_BUFFER_SIZE = 128;

  @Test
  public void testRoundTrips() throws Exception {
    CompressionProvider[] providers = {
        new Lz4CompressionProvider(MAX_BUFFER_SIZE, false, false),
        new XerialSnappyCompressionProvider(MAX_BUFFER_SIZE),
        new ZstdCompressionProvider(MAX_BUFFER_SIZE, 3)
    };
    byte[] original = new byte[MAX_BUFFER_SIZE + 1];
    for (int i = 0; i < original.length; ++i) {
      original[i] = (byte) i;
    }

    for (CompressionProvider provider : providers) {
      Compressor compressor = provider.createCompressor();
      ByteArrayOutputStream encoded = new ByteArrayOutputStream();
      CompressionOutputStream output = provider.createOutputStream(encoded, compressor);
      output.write(original);
      output.close();
      compressor.close();

      Decompressor decompressor = provider.createDecompressorPerDag();
      ByteArrayOutputStream decoded = new ByteArrayOutputStream();
      try (InputStream input = provider.createInputStream(
          new ByteArrayInputStream(encoded.toByteArray()), decompressor)) {
        byte[] buffer = new byte[31];
        int length;
        while ((length = input.read(buffer)) != -1) {
          decoded.write(buffer, 0, length);
        }
      } finally {
        decompressor.close();
      }
      assertArrayEquals(provider.getAlgorithm().name(), original, decoded.toByteArray());
    }
  }

  @Test
  public void testLz4BuffersGrowAtBoundaries() throws Exception {
    Lz4JniDecompressor decompressor = (Lz4JniDecompressor)
        new Lz4CompressionProvider(MAX_BUFFER_SIZE, false, false).createDecompressorPerDag();
    try {
      assertEmpty(decompressor);
      decompressor.ensureCapacity(1, 1);
      assertEquals(16, decompressor.getDecompressedBuffer().length);
      decompressor.ensureCapacity(1, 17);
      assertEquals(32, decompressor.getDecompressedBuffer().length);
      decompressor.ensureCapacity(1, MAX_BUFFER_SIZE);
      assertEquals(MAX_BUFFER_SIZE, decompressor.getDecompressedBuffer().length);
    } finally {
      decompressor.close();
    }
  }

  @Test
  public void testSnappyBuffersGrowAtBoundaries() throws Exception {
    XerialSnappyDecompressor decompressor = (XerialSnappyDecompressor)
        new XerialSnappyCompressionProvider(MAX_BUFFER_SIZE).createDecompressorPerDag();
    try {
      assertEmpty(decompressor);
      decompressor.ensureCapacity(1, 1);
      assertEquals(16, decompressor.getDecompressedBuffer().length);
      decompressor.ensureCapacity(1, 17);
      assertEquals(32, decompressor.getDecompressedBuffer().length);
      decompressor.ensureCapacity(1, MAX_BUFFER_SIZE);
      assertEquals(MAX_BUFFER_SIZE, decompressor.getDecompressedBuffer().length);
    } finally {
      decompressor.close();
    }
  }

  @Test
  public void testZstdBuffersGrowAtBoundaries() throws Exception {
    ZstdJniDecompressor decompressor = (ZstdJniDecompressor)
        new ZstdCompressionProvider(MAX_BUFFER_SIZE, 3).createDecompressorPerDag();
    try {
      assertEmpty(decompressor);
      decompressor.ensureCapacity(1, 1);
      assertEquals(16, decompressor.getDecompressedBuffer().length);
      decompressor.ensureCapacity(1, 17);
      assertEquals(32, decompressor.getDecompressedBuffer().length);
      decompressor.ensureCapacity(1, MAX_BUFFER_SIZE);
      assertEquals(MAX_BUFFER_SIZE, decompressor.getDecompressedBuffer().length);
    } finally {
      decompressor.close();
    }
  }

  private static void assertEmpty(Object decompressor) throws Exception {
    assertEquals(0, getBuffer(decompressor, "compressed").length);
    assertEquals(0, getBuffer(decompressor, "scratch").length);
  }

  private static byte[] getBuffer(Object decompressor, String name) throws Exception {
    Field field = decompressor.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return (byte[]) field.get(decompressor);
  }
}
