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
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.tez.runtime.api.CompressionAlgorithm;
import org.apache.tez.runtime.api.CompressionProvider;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.junit.Test;

public class TestLz4CompressionProvider {

  private static final int BUFFER_SIZE = 128;

  @Test
  public void testProviderIdentity() {
    Lz4CompressionProvider provider = createProvider();
    assertEquals(CompressionAlgorithm.LZ4, provider.getAlgorithm());
    assertEquals(BUFFER_SIZE, provider.getBufferSize());
    assertEquals(CompressionAlgorithm.LZ4, provider.createCompressor().getAlgorithm());
    assertEquals(CompressionAlgorithm.LZ4, provider.createDecompressor().getAlgorithm());
  }

  @Test
  public void testResolverUsesConfiguredBufferSize() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, true);
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC,
        "org.apache.hadoop.io.compress.Lz4Codec");
    conf.setInt(CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY, BUFFER_SIZE);

    CompressionProvider provider = CompressionResolver.createProvider(conf);
    assertTrue(provider instanceof Lz4CompressionProvider);
    assertEquals(BUFFER_SIZE, provider.getBufferSize());
    byte[] original = randomBytes(BUFFER_SIZE + 1);
    assertArrayEquals(original, roundTrip(provider, original));
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
  public void testFlushAndResetState() throws Exception {
    Lz4CompressionProvider provider = createProvider();
    Lz4JniCompressor compressor = (Lz4JniCompressor) provider.createCompressor();
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
    expectReadFailure(frame(Lz4CompressionOutputStream.MAGIC, BUFFER_SIZE + 1, 1,
        new byte[] {0}), "uncompressed chunk length");
    expectReadFailure(frame(Lz4CompressionOutputStream.MAGIC, 1, 0, new byte[0]),
        "compressed chunk length");

    byte[] encoded = encode(createProvider(), randomBytes(BUFFER_SIZE));
    byte[] trailing = new byte[encoded.length + 1];
    System.arraycopy(encoded, 0, trailing, 0, encoded.length);
    expectReadFailure(trailing, "Trailing data");

    byte[] truncated = new byte[encoded.length - 5];
    System.arraycopy(encoded, 0, truncated, 0, truncated.length);
    expectReadFailure(truncated, "Truncated");
  }

  private static Lz4CompressionProvider createProvider() {
    return new Lz4CompressionProvider(BUFFER_SIZE);
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
