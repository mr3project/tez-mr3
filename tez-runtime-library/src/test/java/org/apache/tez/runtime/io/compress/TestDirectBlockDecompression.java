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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.apache.tez.runtime.api.CompressionProvider;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.IFileOutputStream;
import org.junit.Test;

/** Boundaries and failure contracts of direct-to-destination block decompression. */
public class TestDirectBlockDecompression {

  private static final int BLOCK_SIZE = 8192;
  private static final int TEST_MAGIC = 0x54455354; // TEST
  private static final byte CANARY = (byte) 0x5a;

  private static CompressionProvider[] providers() {
    return new CompressionProvider[] {
        new XerialSnappyCompressionProvider(BLOCK_SIZE),
        new Lz4CompressionProvider(BLOCK_SIZE, false, false),
        new ZstdCompressionProvider(BLOCK_SIZE, 3)
    };
  }

  @Test
  public void testDestinationCapacity() throws Exception {
    for (CompressionProvider provider : providers()) {
      for (int size : new int[] {2, 4095, 4096, 4097, BLOCK_SIZE}) {
        byte[] original = randomBytes(size);
        byte[] encoded = encode(provider, original);
        for (int difference : new int[] {-1, 0, 1}) {
          int capacity = size + difference;
          Decompressor decompressor = provider.createDecompressor();
          try (InputStream input = provider.createInputStream(
              new ByteArrayInputStream(encoded), decompressor)) {
            byte[] destination = new byte[size + 3];
            Arrays.fill(destination, CANARY);
            int read = input.read(destination, 1, capacity);
            assertEquals(provider.getAlgorithm() + ", size=" + size
                + ", capacity=" + capacity, Math.min(size, capacity), read);
            readFully(input, destination, 1 + read, size - read);
            assertArrayEquals(original,
                Arrays.copyOfRange(destination, 1, size + 1));
            assertCanaries(destination, 1, size);
            assertEquals(-1, input.read());
          } finally {
            decompressor.close();
          }
        }
      }
    }
  }

  @Test
  public void testNonzeroDestinationOffsets() throws Exception {
    for (CompressionProvider provider : providers()) {
      byte[] original = randomBytes(1024);
      byte[] encoded = encode(provider, original);
      for (int offset : new int[] {0, 1, 17}) {
        Decompressor decompressor = provider.createDecompressor();
        try (InputStream input = provider.createInputStream(
            new ByteArrayInputStream(encoded), decompressor)) {
          byte[] destination = new byte[offset + original.length + 19];
          Arrays.fill(destination, CANARY);
          assertEquals(original.length, input.read(destination, offset, original.length));
          assertArrayEquals(original,
              Arrays.copyOfRange(destination, offset, offset + original.length));
          assertCanaries(destination, offset, original.length);
          assertEquals(-1, input.read());
        } finally {
          decompressor.close();
        }
      }
    }
  }

  @Test
  public void testBufferedToDirectTransition() throws Exception {
    for (CompressionProvider provider : providers()) {
      byte[] first = randomBytes(3072);
      byte[] second = randomBytes(2048);
      byte[] third = randomBytes(4096);
      byte[] encoded = encodeChunks(provider, first, second, third);
      Decompressor decompressor = provider.createDecompressor();
      try (InputStream input = provider.createInputStream(
          new ByteArrayInputStream(encoded), decompressor)) {
        byte[] decodedFirst = new byte[first.length];
        assertEquals(23, input.read(decodedFirst, 0, 23));
        readFully(input, decodedFirst, 23, first.length - 23);
        assertArrayEquals(first, decodedFirst);

        byte[] decodedSecond = new byte[second.length + 21];
        Arrays.fill(decodedSecond, CANARY);
        assertEquals(second.length, input.read(decodedSecond, 7, second.length));
        assertArrayEquals(second,
            Arrays.copyOfRange(decodedSecond, 7, 7 + second.length));
        assertCanaries(decodedSecond, 7, second.length);

        byte[] decodedThird = new byte[third.length];
        readFully(input, decodedThird, 0, third.length);
        assertArrayEquals(third, decodedThird);
        assertEquals(-1, input.read());
      } finally {
        decompressor.close();
      }
    }
  }

  @Test
  public void testDirectToBufferedTransition() throws Exception {
    for (CompressionProvider provider : providers()) {
      byte[] first = randomBytes(4096);
      byte[] second = randomBytes(2053);
      byte[] third = randomBytes(8192);
      byte[] encoded = encodeChunks(provider, first, second, third);
      Decompressor decompressor = provider.createDecompressor();
      try (InputStream input = provider.createInputStream(
          new ByteArrayInputStream(encoded), decompressor)) {
        byte[] decodedFirst = new byte[first.length];
        assertEquals(first.length, input.read(decodedFirst, 0, decodedFirst.length));
        assertArrayEquals(first, decodedFirst);

        byte[] decodedSecond = new byte[second.length];
        assertEquals(second[0] & 0xff, input.read());
        decodedSecond[0] = second[0];
        for (int pos = 1; pos < decodedSecond.length;) {
          int read = input.read(decodedSecond, pos, Math.min(31, decodedSecond.length - pos));
          assertTrue(read > 0);
          pos += read;
        }
        assertArrayEquals(second, decodedSecond);

        byte[] decodedThird = new byte[third.length];
        assertEquals(third.length, input.read(decodedThird, 0, decodedThird.length));
        assertArrayEquals(third, decodedThird);
        assertEquals(-1, input.read());
      } finally {
        decompressor.close();
      }
    }
  }

  @Test
  public void testZeroLengthAndInvalidCallerRanges() throws Exception {
    for (CompressionProvider provider : providers()) {
      byte[] original = randomBytes(101);
      Decompressor decompressor = provider.createDecompressor();
      try (InputStream input = provider.createInputStream(
          new ByteArrayInputStream(encode(provider, original)), decompressor)) {
        byte[] destination = new byte[original.length];
        assertEquals(0, input.read(destination, destination.length, 0));
        expectInvalidRange(input, destination, -1, 1);
        expectInvalidRange(input, destination, 1, destination.length);
        expectInvalidRange(input, destination, 0, -1);
        assertEquals(original.length, input.read(destination));
        assertArrayEquals(original, destination);
        assertEquals(-1, input.read());
        assertEquals(0, input.read(destination, 0, 0));
      } finally {
        decompressor.close();
      }
    }
  }

  @Test
  public void testTruncatedFramesAndChunks() throws Exception {
    for (CompressionProvider provider : providers()) {
      byte[] encoded = encode(provider, randomBytes(1300));
      int compressedLength = intAt(encoded, 8);
      for (int size : new int[] {0, 1, 2, 3, 4, 5, 8, 9, 11,
          12, 12 + compressedLength - 1, encoded.length - 8,
          encoded.length - 7, encoded.length - 1}) {
        expectReadFailure(provider, Arrays.copyOf(encoded, size));
      }
    }
  }

  @Test
  public void testInvalidFrameSizes() throws Exception {
    for (CompressionProvider provider : providers()) {
      byte[] encoded = encode(provider, randomBytes(1300));
      for (int rawLength : new int[] {-1, 0, BLOCK_SIZE + 1}) {
        byte[] invalid = encoded.clone();
        putInt(invalid, 4, rawLength);
        expectReadFailure(provider, invalid);
      }
      for (int compressedLength : new int[] {-1, 0, Integer.MAX_VALUE}) {
        byte[] invalid = encoded.clone();
        putInt(invalid, 8, compressedLength);
        expectReadFailure(provider, invalid);
      }
    }
  }

  @Test
  public void testCorruptionOnDirectPath() throws Exception {
    for (CompressionProvider provider : providers()) {
      byte[] encoded = encode(provider, randomBytes(1300));

      // Mismatching the framed raw size with the decoded size must be rejected.
      byte[] wrongRawSize = encoded.clone();
      putInt(wrongRawSize, 4, 1299);
      expectReadFailure(provider, wrongRawSize);

      byte[] corrupt = encoded.clone();
      switch (provider.getAlgorithm()) {
      case ZSTD:
        Arrays.fill(corrupt, 12, 16, (byte) 0);
        break;
      case SNAPPY:
        // Corrupt Snappy's raw-length varint.
        corrupt[12] = (byte) 0xff;
        break;
      case LZ4:
        // Force a mismatch between the recorded compressed length and the
        // number of bytes consumed by the fast decompressor.
        int compressedLength = intAt(corrupt, 8);
        corrupt = new byte[encoded.length + 1];
        System.arraycopy(encoded, 0, corrupt, 0, 12 + compressedLength);
        System.arraycopy(encoded, 12 + compressedLength, corrupt,
            13 + compressedLength, encoded.length - 12 - compressedLength);
        putInt(corrupt, 8, compressedLength + 1);
        break;
      default:
        throw new AssertionError(provider.getAlgorithm());
      }
      expectReadFailure(provider, corrupt);
    }
  }

  @Test
  public void testTerminatorAndTrailingData() throws Exception {
    for (CompressionProvider provider : providers()) {
      Decompressor decompressor = provider.createDecompressor();
      try (InputStream input = provider.createInputStream(
          new ByteArrayInputStream(encode(provider, new byte[0])), decompressor)) {
        assertEquals(-1, input.read());
        assertEquals(-1, input.read(new byte[1]));
      } finally {
        decompressor.close();
      }

      byte[] encoded = encode(provider, randomBytes(128));
      expectReadFailure(provider, Arrays.copyOf(encoded, encoded.length - 8));

      byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
      trailing[trailing.length - 1] = 42;
      expectReadFailure(provider, trailing);

      byte[] invalidTerminator = encoded.clone();
      putInt(invalidTerminator, invalidTerminator.length - 4, 1);
      expectReadFailure(provider, invalidTerminator);
    }
  }

  @Test
  public void testCloseDrainAndReuse() throws Exception {
    for (CompressionProvider provider : providers()) {
      byte[] first = randomBytes(4096);
      byte[] second = randomBytes(3072);
      byte[] encoded = encodeChunks(provider, first, second);
      Decompressor decompressor = provider.createDecompressor();
      try {
        InputStream input = provider.createInputStream(
            new ByteArrayInputStream(encoded), decompressor);
        byte[] result = new byte[first.length];
        assertEquals(first.length, input.read(result));
        assertArrayEquals(first, result);
        input.close(); // Must validate and discard the second chunk and terminator.

        byte[] reusedData = randomBytes(537);
        try (InputStream reused = provider.createInputStream(
            new ByteArrayInputStream(encode(provider, reusedData)), decompressor)) {
          assertArrayEquals(reusedData, readAll(reused, 256));
        }

        byte[] withTrailing = Arrays.copyOf(encoded, encoded.length + 1);
        InputStream invalid = provider.createInputStream(
            new ByteArrayInputStream(withTrailing), decompressor);
        assertEquals(first.length, invalid.read(result));
        try {
          invalid.close();
          fail("close must detect trailing bytes");
        } catch (IOException expected) {
          // close() still resets the decompressor.
        }

        try (InputStream reused = provider.createInputStream(
            new ByteArrayInputStream(encode(provider, reusedData)), decompressor)) {
          assertArrayEquals(reusedData, readAll(reused, reusedData.length));
        }
      } finally {
        decompressor.close();
      }
    }
  }

  @Test
  public void testPerDagDecompressorReuse() throws Exception {
    for (CompressionProvider provider : providers()) {
      Decompressor decompressor = provider.createDecompressorPerDag();
      try {
        byte[] first = randomBytes(BLOCK_SIZE);
        try (InputStream input = provider.createInputStream(
            new ByteArrayInputStream(encode(provider, first)), decompressor)) {
          byte[] decoded = new byte[BLOCK_SIZE + 4];
          assertEquals(BLOCK_SIZE, input.read(decoded, 2, BLOCK_SIZE));
          assertArrayEquals(first, Arrays.copyOfRange(decoded, 2, BLOCK_SIZE + 2));
          assertEquals(-1, input.read());
        }

        byte[] second = randomBytes(63);
        try (InputStream input = provider.createInputStream(
            new ByteArrayInputStream(encode(provider, second)), decompressor)) {
          assertArrayEquals(second, readAll(input, 7));
        }
      } finally {
        decompressor.close();
      }
    }
  }

  @Test
  public void testDispatchBetweenDirectAndBufferedDecode() throws Exception {
    byte[] first = randomBytes(100);
    byte[] second = randomBytes(200);
    TrackingInputStream source = new TrackingInputStream(frame(first, second));
    TrackingBlockStream input = new TrackingBlockStream(source);
    byte[] decodedFirst = new byte[first.length];
    assertEquals(first.length - 1, input.read(decodedFirst, 0, first.length - 1));
    assertEquals(1, input.bufferedCalls);
    assertEquals(0, input.directCalls);
    assertEquals(1, input.read(decodedFirst, first.length - 1, 1));
    assertArrayEquals(first, decodedFirst);

    byte[] decodedSecond = new byte[second.length + 2];
    assertEquals(second.length, input.read(decodedSecond, 1, second.length + 1));
    assertArrayEquals(second,
        Arrays.copyOfRange(decodedSecond, 1, second.length + 1));
    assertEquals(1, input.directCalls);
    assertEquals(1, input.bufferedCalls);
    assertEquals(-1, input.read());
    input.close();
    assertEquals(1, input.resetCalls);
    assertTrue(source.closed);
  }

  @Test
  public void testSingleByteReadUsesScratch() throws Exception {
    byte[] first = randomBytes(31);
    byte[] second = randomBytes(97);
    TrackingBlockStream input = new TrackingBlockStream(
        new TrackingInputStream(frame(first, second)));
    assertEquals(first[0] & 0xff, input.read());
    assertEquals(1, input.bufferedCalls);
    assertEquals(0, input.directCalls);

    byte[] tail = new byte[first.length - 1];
    readFully(input, tail, 0, tail.length);
    assertArrayEquals(Arrays.copyOfRange(first, 1, first.length), tail);
    byte[] result = new byte[second.length];
    assertEquals(second.length, input.read(result));
    assertArrayEquals(second, result);
    assertEquals(1, input.directCalls);
    assertEquals(-1, input.read());
    input.close();
  }

  @Test
  public void testInjectedBufferedFailureKeepsCallerBufferUntouched() throws Exception {
    TrackingInputStream source = new TrackingInputStream(frame(randomBytes(100)));
    TrackingBlockStream input = new TrackingBlockStream(source);
    input.failBuffered = true;
    byte[] destination = new byte[100];
    Arrays.fill(destination, CANARY);
    try {
      input.read(destination, 1, 99); // Smaller than the raw chunk.
      fail("buffered decompression must fail");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("Injected"));
    }
    assertEquals(1, input.bufferedCalls);
    assertEquals(0, input.directCalls);
    for (byte b : destination) {
      assertEquals(CANARY, b);
    }
    input.close();
    assertTrue(source.closed);
    assertEquals(1, input.resetCalls);
  }

  @Test
  public void testWrongDecodedLengthRejected() throws Exception {
    for (int capacity : new int[] {99, 100}) {
      TrackingBlockStream input = new TrackingBlockStream(
          new TrackingInputStream(frame(randomBytes(100))));
      input.shortDecode = true;
      try {
        input.read(new byte[100], 0, capacity);
        fail("decoded length mismatch must be rejected");
      } catch (IOException expected) {
        assertTrue(expected.getMessage().contains("chunk length mismatch"));
      }
      assertEquals(capacity == 100 ? 1 : 0, input.directCalls);
      assertEquals(capacity == 99 ? 1 : 0, input.bufferedCalls);
      input.close();
    }
  }

  @Test
  public void testInjectedDecodeFailureAndCleanup() throws Exception {
    TrackingInputStream source = new TrackingInputStream(frame(randomBytes(100)));
    TrackingBlockStream input = new TrackingBlockStream(source);
    input.failDirect = true;
    byte[] destination = new byte[134];
    Arrays.fill(destination, CANARY);
    try {
      input.read(destination, 17, 100);
      fail("direct decompression must fail");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("Injected"));
    }
    assertEquals(1, input.directCalls);
    assertEquals(0, input.bufferedCalls);
    assertCanaries(destination, 17, 100);
    assertEquals(-1, input.read()); // The failing chunk was consumed.
    input.close();
    assertTrue(source.closed);
    assertEquals(1, input.resetCalls);
  }

  @Test
  public void testCloseCleansUpAfterDrainFailure() throws Exception {
    byte[] framed = frame(randomBytes(1024));
    byte[] trailing = Arrays.copyOf(framed, framed.length + 1);
    TrackingInputStream source = new TrackingInputStream(trailing);
    TrackingBlockStream input = new TrackingBlockStream(source);
    try {
      input.close();
      fail("close must reject trailing data");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("Trailing data"));
    }
    assertTrue(source.closed);
    assertEquals(1, input.resetCalls);
    input.close();
    assertEquals(1, input.resetCalls);
  }

  @Test
  public void testIFileChecksumWithDirectDecode() throws Exception {
    for (CompressionProvider provider : providers()) {
      byte[] payload = randomBytes(BLOCK_SIZE + 127);
      byte[] file = ifile(provider, payload);
      byte[] expected = new byte[IFile.HEADER.length + payload.length];
      System.arraycopy(file, 0, expected, 0, IFile.HEADER.length);
      System.arraycopy(payload, 0, expected, IFile.HEADER.length, payload.length);

      Decompressor decompressor = provider.createDecompressor();
      try {
        boolean[] returned = {false};
        TaskContext context = contextReturning(decompressor, returned);
        byte[] result = new byte[expected.length];
        IFile.Reader.readToMemory(result, new ByteArrayInputStream(file), file.length,
            provider, false, 0, context, false);
        assertArrayEquals(expected, result);
        assertTrue("decompressor not returned", returned[0]);

        byte[] badChecksum = file.clone();
        badChecksum[badChecksum.length - 1] ^= 1;
        returned[0] = false;
        try {
          IFile.Reader.readToMemory(new byte[expected.length],
              new ByteArrayInputStream(badChecksum), badChecksum.length,
              provider, false, 0, context, false);
          fail("corrupt IFile checksum must be rejected");
        } catch (IOException expectedFailure) {
          assertTrue("unexpected error: " + expectedFailure,
              expectedFailure.getMessage().contains("Checksum"));
        }
        assertTrue("decompressor not returned after failure", returned[0]);
      } finally {
        decompressor.close();
      }
    }
  }

  private static byte[] randomBytes(int length) {
    byte[] data = new byte[length];
    new Random(12345L + length).nextBytes(data);
    return data;
  }

  private static byte[] encode(CompressionProvider provider, byte[] original)
      throws IOException {
    return encodeChunks(provider, original);
  }

  private static byte[] encodeChunks(CompressionProvider provider, byte[]... chunks)
      throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    Compressor compressor = provider.createCompressor();
    try (CompressionOutputStream output = provider.createOutputStream(bytes, compressor)) {
      for (byte[] chunk : chunks) {
        output.write(chunk);
        output.flush(); // Make this a separate compression chunk.
      }
    } finally {
      compressor.close();
    }
    return bytes.toByteArray();
  }

  private static void readFully(InputStream input, byte[] dst, int offset, int length)
      throws IOException {
    new DataInputStream(input).readFully(dst, offset, length);
  }

  private static byte[] readAll(InputStream input, int bufferLength) throws IOException {
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    byte[] buffer = new byte[bufferLength];
    int n;
    while ((n = input.read(buffer)) != -1) {
      result.write(buffer, 0, n);
    }
    return result.toByteArray();
  }

  private static void assertCanaries(byte[] data, int offset, int length) {
    for (int i = 0; i < offset; i++) {
      assertEquals("prefix canary at " + i, CANARY, data[i]);
    }
    for (int i = offset + length; i < data.length; i++) {
      assertEquals("suffix canary at " + i, CANARY, data[i]);
    }
  }

  private static void expectInvalidRange(InputStream input, byte[] array, int offset,
      int length) throws IOException {
    try {
      input.read(array, offset, length);
      fail("Expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException expected) {
      // Must not consume any compressed data.
    }
  }

  private static IOException expectReadFailure(CompressionProvider provider, byte[] encoded)
      throws Exception {
    Decompressor decompressor = provider.createDecompressor();
    try {
      try (InputStream input = provider.createInputStream(
          new ByteArrayInputStream(encoded), decompressor)) {
        readAll(input, BLOCK_SIZE + 1); // Decode directly wherever possible.
      }
    } catch (IOException expected) {
      return expected;
    } finally {
      decompressor.close();
    }
    throw new AssertionError("Expected IOException for " + provider.getAlgorithm());
  }

  private static int intAt(byte[] data, int offset) {
    return ByteBuffer.wrap(data, offset, Integer.BYTES).getInt();
  }

  private static void putInt(byte[] data, int offset, int value) {
    ByteBuffer.wrap(data, offset, Integer.BYTES).putInt(value);
  }

  private static byte[] frame(byte[]... chunks) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(bytes);
    output.writeInt(TEST_MAGIC);
    for (byte[] chunk : chunks) {
      output.writeInt(chunk.length);
      output.writeInt(chunk.length);
      output.write(chunk);
    }
    output.writeInt(0);
    output.writeInt(0);
    output.flush();
    return bytes.toByteArray();
  }

  private static byte[] ifile(CompressionProvider provider, byte[] payload)
      throws IOException {
    byte[] encoded = encode(provider, payload);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    byte[] header = IFile.HEADER.clone();
    header[3] = IFile.FLAG_COMPRESSED;
    bytes.write(header);
    IFileOutputStream checksumOut = new IFileOutputStream(bytes);
    checksumOut.write(encoded);
    checksumOut.finish();
    return bytes.toByteArray();
  }

  private static TaskContext contextReturning(Decompressor decompressor,
      boolean[] returned) {
    return (TaskContext) Proxy.newProxyInstance(TaskContext.class.getClassLoader(),
        new Class<?>[] {TaskContext.class}, (proxy, method, args) -> {
          if ("getDecompressor".equals(method.getName())) {
            return decompressor;
          }
          if ("returnDecompressor".equals(method.getName())) {
            returned[0] = true;
            return null;
          }
          throw new AssertionError("Unexpected TaskContext call: " + method.getName());
        });
  }

  private static final class TrackingInputStream extends ByteArrayInputStream {
    boolean closed;

    TrackingInputStream(byte[] data) {
      super(data);
    }

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }
  }

  private static final class TrackingBlockStream extends BlockCompressionInputStream {
    private final byte[] compressed = new byte[BLOCK_SIZE];
    private final byte[] scratch = new byte[BLOCK_SIZE];
    int bufferedCalls;
    int directCalls;
    int resetCalls;
    boolean failDirect;
    boolean failBuffered;
    boolean shortDecode;

    TrackingBlockStream(InputStream input) throws IOException {
      super(input, BLOCK_SIZE, BLOCK_SIZE, TEST_MAGIC, "synthetic");
    }

    @Override
    byte[] ensureCapacity(int compressedLength, int rawLength) {
      return compressed;
    }

    @Override
    int decompressBuffered(int inputLength, int outputCapacity) throws IOException {
      bufferedCalls++;
      if (failBuffered) {
        failBuffered = false;
        scratch[0] = compressed[0];
        throw new IOException("Injected buffered decoding failure");
      }
      return decode(inputLength, scratch, 0, outputCapacity);
    }

    @Override
    int decompressInto(int inputLength, byte[] destination, int offset,
        int outputCapacity) throws IOException {
      directCalls++;
      if (failDirect) {
        failDirect = false;
        destination[offset] = compressed[0];
        throw new IOException("Injected direct decoding failure");
      }
      return decode(inputLength, destination, offset, outputCapacity);
    }

    private int decode(int inputLength, byte[] destination, int offset, int outputCapacity) {
      assertEquals(inputLength, outputCapacity);
      System.arraycopy(compressed, 0, destination, offset, inputLength);
      return shortDecode ? inputLength - 1 : inputLength;
    }

    @Override
    byte[] getDecompressedBuffer() {
      return scratch;
    }

    @Override
    void resetDecompressor() {
      resetCalls++;
    }
  }
}
