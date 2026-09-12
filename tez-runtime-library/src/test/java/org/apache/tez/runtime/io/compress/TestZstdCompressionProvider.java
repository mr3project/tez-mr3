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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.tez.runtime.api.CompressionAlgorithm;
import org.apache.tez.runtime.api.CompressionProvider;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.junit.Test;

public class TestZstdCompressionProvider {

  private static final int BUFFER_SIZE = 128;
  private static final int COMPRESSION_LEVEL = 3;
  private static final int MEBIBYTE = 1024 * 1024;

  @Test
  public void testProviderIdentity() {
    ZstdCompressionProvider provider = createProvider();
    assertEquals(CompressionAlgorithm.ZSTD, provider.getAlgorithm());
    assertEquals(BUFFER_SIZE, provider.getBufferSize());
    assertEquals(CompressionAlgorithm.ZSTD, provider.createCompressor().getAlgorithm());
    assertEquals(CompressionAlgorithm.ZSTD, provider.createDecompressor().getAlgorithm());
  }

  @Test
  public void testResolverUsesConfiguredBufferSizeAndLevel() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, true);
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC,
        "org.apache.hadoop.io.compress.ZStandardCodec");
    conf.setInt(CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_KEY, BUFFER_SIZE);
    conf.setInt(CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_LEVEL_KEY, COMPRESSION_LEVEL);

    CompressionProvider provider = CompressionResolver.createProvider(conf);
    assertTrue(provider instanceof ZstdCompressionProvider);
    assertEquals(BUFFER_SIZE, provider.getBufferSize());
    assertArrayEquals(randomBytes(BUFFER_SIZE + 1),
        roundTrip(provider, randomBytes(BUFFER_SIZE + 1)));
  }

  @Test
  public void testRoundTripsBlockBoundariesAndEmptyInput() throws Exception {
    int[] lengths = {0, 1, BUFFER_SIZE - 1, BUFFER_SIZE, BUFFER_SIZE + 1,
        2 * BUFFER_SIZE, 2 * BUFFER_SIZE + 17};
    for (int length : lengths) {
      byte[] original = randomBytes(length);
      assertArrayEquals("length=" + length, original, roundTrip(createProvider(), original));
    }
  }

  @Test
  public void testTenMiBRandomData() throws Exception {
    testLargeRandomData(10 * MEBIBYTE);
  }

  @Test
  public void testTwentyMiBRandomData() throws Exception {
    testLargeRandomData(20 * MEBIBYTE);
  }

  @Test
  public void testFortyMiBRandomData() throws Exception {
    testLargeRandomData(40 * MEBIBYTE);
  }

  @Test
  public void testOneHundredMiBRandomData() throws Exception {
    testLargeRandomData(100 * MEBIBYTE);
  }

  @Test
  public void testFlushAndResetState() throws Exception {
    ZstdCompressionProvider provider = createProvider();
    ZstdJniCompressor compressor = (ZstdJniCompressor) provider.createCompressor();
    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    CompressionOutputStream output = provider.createOutputStream(encoded, compressor);
    byte[] first = randomBytes(37);
    output.write(first);
    output.flush();
    output.finish();
    int firstEncodedLength = encoded.size();

    assertArrayEquals(first, decode(provider, encoded.toByteArray()));

    output.resetState();
    byte[] second = randomBytes(BUFFER_SIZE + 7);
    output.write(second);
    output.finish();
    byte[] both = encoded.toByteArray();
    byte[] secondStream = new byte[both.length - firstEncodedLength];
    System.arraycopy(both, firstEncodedLength, secondStream, 0, secondStream.length);
    assertArrayEquals(second, decode(provider, secondStream));
    output.close();
    compressor.close();
  }

  @Test
  public void testRejectsInvalidFraming() throws Exception {
    expectReadFailure(frame(0x12345678, 0, 0, new byte[0]), "magic");
    expectReadFailure(frame(ZstdCompressionOutputStream.MAGIC, BUFFER_SIZE + 1, 1,
        new byte[] {0}), "uncompressed chunk length");
    expectReadFailure(frame(ZstdCompressionOutputStream.MAGIC, 1, 0, new byte[0]),
        "compressed chunk length");

    byte[] truncated = encode(createProvider(), randomBytes(20));
    byte[] shortened = new byte[truncated.length - 5];
    System.arraycopy(truncated, 0, shortened, 0, shortened.length);
    expectReadFailure(shortened, "Truncated");
  }

  @Test
  public void testRejectsTrailingDataAndCorruption() throws Exception {
    byte[] encoded = encode(createProvider(), randomBytes(BUFFER_SIZE));
    byte[] trailing = new byte[encoded.length + 1];
    System.arraycopy(encoded, 0, trailing, 0, encoded.length);
    expectReadFailure(trailing, "Trailing data");

    byte[] corrupt = encoded.clone();
    corrupt[12] ^= 1;
    expectReadFailure(corrupt, "Zstd");
  }

  private static ZstdCompressionProvider createProvider() {
    return new ZstdCompressionProvider(BUFFER_SIZE, COMPRESSION_LEVEL);
  }

  private static void testLargeRandomData(int size) throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, true);
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC,
        "org.apache.hadoop.io.compress.ZStandardCodec");
    CompressionProvider provider = CompressionResolver.createProvider(conf);

    byte[] original = randomBytes(size);
    ByteArrayOutputStream compressedOut = new ByteArrayOutputStream();
    Compressor compressor = provider.createCompressor();
    try (OutputStream output = provider.createOutputStream(compressedOut, compressor)) {
      output.write(original, 0, size);
    } finally {
      compressor.close();
    }
    byte[] compressed = compressedOut.toByteArray();

    ByteArrayOutputStream decompressedOut = new ByteArrayOutputStream();
    Decompressor decompressor = provider.createDecompressor();
    try (InputStream input = provider.createInputStream(
        new ByteArrayInputStream(compressed), decompressor)) {
      byte[] buffer = new byte[1024];
      int bytesRead;
      while ((bytesRead = input.read(buffer)) != -1) {
        decompressedOut.write(buffer, 0, bytesRead);
      }
    } finally {
      decompressor.close();
    }

    assertArrayEquals("size=" + size, original, decompressedOut.toByteArray());
  }

  private static byte[] randomBytes(int length) {
    byte[] data = new byte[length];
    new Random(12345L + length).nextBytes(data);
    return data;
  }

  private static byte[] roundTrip(CompressionProvider provider, byte[] original) throws Exception {
    return decode(provider, encode(provider, original));
  }

  private static byte[] encode(CompressionProvider provider, byte[] original) throws Exception {
    Compressor compressor = provider.createCompressor();
    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    CompressionOutputStream output = provider.createOutputStream(encoded, compressor);
    output.write(original);
    output.finish();
    output.close();
    compressor.close();
    return encoded.toByteArray();
  }

  private static byte[] decode(CompressionProvider provider, byte[] encoded) throws Exception {
    Decompressor decompressor = provider.createDecompressor();
    InputStream input = provider.createInputStream(new ByteArrayInputStream(encoded), decompressor);
    ByteArrayOutputStream decoded = new ByteArrayOutputStream();
    byte[] buffer = new byte[31];
    int read;
    while ((read = input.read(buffer)) != -1) {
      decoded.write(buffer, 0, read);
    }
    input.close();
    decompressor.close();
    return decoded.toByteArray();
  }

  private static byte[] frame(int magic, int rawLength, int compressedLength, byte[] payload)
      throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream data = new DataOutputStream(bytes);
    data.writeInt(magic);
    data.writeInt(rawLength);
    data.writeInt(compressedLength);
    data.write(payload);
    return bytes.toByteArray();
  }

  private static void expectReadFailure(byte[] encoded, String message) throws Exception {
    try {
      decode(createProvider(), encoded);
      fail("Expected IOException containing: " + message);
    } catch (IOException e) {
      assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains(message));
    }
  }
}
