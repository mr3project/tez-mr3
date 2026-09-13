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
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.apache.tez.runtime.api.CompressionProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestBlockCompressionStreams {

  private static final int BUFFER_SIZE = 128;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> codecs() {
    return Arrays.asList(new Object[][] {
        {"Snappy", new XerialSnappyCompressionProvider(BUFFER_SIZE),
            SnappyCompressionOutputStream.MAGIC},
        {"Zstd", new ZstdCompressionProvider(BUFFER_SIZE, 3),
            ZstdCompressionOutputStream.MAGIC}
    });
  }

  private final String codecName;
  private final CompressionProvider provider;
  private final int magic;

  public TestBlockCompressionStreams(
      String codecName, CompressionProvider provider, int magic) {
    this.codecName = codecName;
    this.provider = provider;
    this.magic = magic;
  }

  @Test
  public void testRoundTripsBlockBoundaries() throws Exception {
    int[] lengths = {0, 1, BUFFER_SIZE - 1, BUFFER_SIZE, BUFFER_SIZE + 1,
        2 * BUFFER_SIZE + 17};
    for (int length : lengths) {
      byte[] original = randomBytes(length);
      assertArrayEquals("length=" + length, original, decode(encode(original)));
    }
  }

  @Test
  public void testEmptyStreamFraming() throws Exception {
    ByteArrayOutputStream expected = new ByteArrayOutputStream();
    DataOutputStream data = new DataOutputStream(expected);
    data.writeInt(magic);
    data.writeInt(0);
    data.writeInt(0);

    assertArrayEquals(expected.toByteArray(), encode(new byte[0]));
  }

  @Test
  public void testFlushAndResetState() throws Exception {
    Compressor compressor = provider.createCompressor();
    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    CompressionOutputStream output = provider.createOutputStream(encoded, compressor);
    byte[] first = randomBytes(37);
    output.write(first);
    output.flush();
    output.finish();
    int firstLength = encoded.size();

    output.resetState();
    byte[] second = randomBytes(BUFFER_SIZE + 7);
    output.write(second);
    output.finish();
    output.close();
    compressor.close();

    byte[] both = encoded.toByteArray();
    assertArrayEquals(first, decode(Arrays.copyOf(both, firstLength)));
    assertArrayEquals(second, decode(Arrays.copyOfRange(both, firstLength, both.length)));
  }

  @Test
  public void testRejectsInvalidAndTrailingFraming() throws Exception {
    expectReadFailure(frame(0x12345678, 0, 0, new byte[0]), "magic");
    expectReadFailure(frame(magic, BUFFER_SIZE + 1, 1, new byte[] {0}),
        "uncompressed chunk length");
    expectReadFailure(frame(magic, 1, 0, new byte[0]), "compressed chunk length");

    byte[] encoded = encode(randomBytes(20));
    expectReadFailure(Arrays.copyOf(encoded, encoded.length - 5), "Truncated");
    expectReadFailure(Arrays.copyOf(encoded, encoded.length + 1), "Trailing data");
  }

  @Test
  public void testCloseValidatesUnreadFraming() throws Exception {
    byte[] valid = encode(randomBytes(20));
    byte[] encoded = Arrays.copyOf(valid, valid.length + 1);
    Decompressor decompressor = provider.createDecompressor();
    InputStream input = provider.createInputStream(new ByteArrayInputStream(encoded), decompressor);
    try {
      input.close();
      fail("Expected close to validate trailing data");
    } catch (IOException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("Trailing data"));
    } finally {
      decompressor.close();
    }
  }

  private byte[] encode(byte[] original) throws Exception {
    Compressor compressor = provider.createCompressor();
    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    try {
      CompressionOutputStream output = provider.createOutputStream(encoded, compressor);
      output.write(original);
      output.finish();
      output.close();
    } finally {
      compressor.close();
    }
    return encoded.toByteArray();
  }

  private byte[] decode(byte[] encoded) throws Exception {
    Decompressor decompressor = provider.createDecompressor();
    ByteArrayOutputStream decoded = new ByteArrayOutputStream();
    try (InputStream input = provider.createInputStream(
        new ByteArrayInputStream(encoded), decompressor)) {
      byte[] buffer = new byte[31];
      int read;
      while ((read = input.read(buffer)) != -1) {
        decoded.write(buffer, 0, read);
      }
    } finally {
      decompressor.close();
    }
    return decoded.toByteArray();
  }

  private byte[] frame(int frameMagic, int rawLength, int compressedLength, byte[] payload)
      throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream data = new DataOutputStream(bytes);
    data.writeInt(frameMagic);
    data.writeInt(rawLength);
    data.writeInt(compressedLength);
    data.write(payload);
    return bytes.toByteArray();
  }

  private void expectReadFailure(byte[] encoded, String message) throws Exception {
    try {
      decode(encoded);
      fail("Expected IOException containing: " + message);
    } catch (IOException e) {
      assertTrue("Unexpected " + codecName + " message: " + e.getMessage(),
          e.getMessage().contains(message));
    }
  }

  private static byte[] randomBytes(int length) {
    byte[] data = new byte[length];
    new Random(12345L + length).nextBytes(data);
    return data;
  }
}
