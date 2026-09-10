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
 */
package org.apache.tez.runtime.io.compress;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.ZStandardCodec;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.runtime.api.CompressorPool;
import org.apache.tez.runtime.api.DecompressorPool;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.junit.Test;

public class TestXerialSnappyCompression {
  @Test
  public void testRawBlocksOffsetsCapacityResetAndClose() throws Exception {
    XerialSnappyCompressor compressor = new XerialSnappyCompressor();
    XerialSnappyDecompressor decompressor = new XerialSnappyDecompressor();
    for (byte[] value : values()) {
      byte[] input = new byte[value.length + 9];
      System.arraycopy(value, 0, input, 4, value.length);
      byte[] compressed = new byte[compressor.maxCompressedLength(value.length) + 11];
      int compressedLength = compressor.compress(
          input, 4, value.length, compressed, 6, compressed.length - 6);
      byte[] restored = new byte[value.length + 7];
      assertEquals(value.length, decompressor.decompress(
          compressed, 6, compressedLength, restored, 3, value.length));
      assertArrayEquals(value, Arrays.copyOfRange(restored, 3, 3 + value.length));

      byte[] exact = new byte[compressedLength];
      assertEquals(compressedLength,
          compressor.compress(value, 0, value.length, exact, 0, exact.length));
      assertIOException(() -> compressor.compress(
          value, 0, value.length, new byte[Math.max(0, compressedLength - 1)], 0,
          Math.max(0, compressedLength - 1)));
      if (value.length > 0) {
        assertIOException(() -> decompressor.decompress(
            exact, 0, exact.length, new byte[value.length - 1], 0, value.length - 1));
      }
      compressor.reset();
      decompressor.reset();
    }

    assertInvalidRanges(compressor, decompressor);
    compressor.close();
    compressor.close();
    decompressor.close();
    decompressor.close();
    assertIllegalState(() -> compressor.maxCompressedLength(1));
    assertIllegalState(decompressor::reset);
  }

  @Test
  public void testStreamsArbitraryWritesChunksEmptyFinishAndReuse() throws Exception {
    byte[] value = new byte[113];
    new Random(19).nextBytes(value);
    TrackingOutputStream sink = new TrackingOutputStream();
    XerialSnappyCompressor compressor = new XerialSnappyCompressor();
    SnappyCompressionOutputStream out = new SnappyCompressionOutputStream(sink, compressor, 16);
    for (int i = 0; i < 23; ++i) {
      out.write(value[i]);
    }
    out.write(value, 23, value.length - 23);
    out.finish();
    assertFalse("finish must not close the underlying stream", sink.closed);
    byte[] first = sink.toByteArray();
    assertArrayEquals(value, decode(first, 16));

    out.resetState();
    out.finish();
    byte[] both = sink.toByteArray();
    assertTrue(both.length > first.length);
    assertArrayEquals(new byte[0], decode(Arrays.copyOfRange(both, first.length, both.length), 16));
  }

  @Test
  public void testMalformedAndTruncatedStreams() throws Exception {
    byte[] encoded = encode(new byte[100], 17);
    for (int length : new int[] {0, 1, 3, 7, encoded.length - 1}) {
      assertIOException(() -> decode(Arrays.copyOf(encoded, length), 17));
    }

    assertIOException(() -> decode(ints(0x12345678), 17));
    assertIOException(() -> decode(ints(SnappyCompressionOutputStream.MAGIC, -1, 2), 17));
    assertIOException(() -> decode(ints(SnappyCompressionOutputStream.MAGIC, 18, 2), 17));
    assertIOException(() -> decode(ints(SnappyCompressionOutputStream.MAGIC, 1, Integer.MAX_VALUE), 17));

    byte[] corrupt = encode(new byte[10], 17);
    corrupt[12] = (byte) 0xff;
    assertIOException(() -> decode(corrupt, 17));

    ByteArrayOutputStream trailing = new ByteArrayOutputStream();
    trailing.write(encode(new byte[0], 17));
    trailing.write(1);
    assertIOException(() -> decode(trailing.toByteArray(), 17));
  }

  @Test
  public void testResolverUsesOnlyCodecClassName() throws Exception {
    assertEquals(CompressionAlgorithm.SNAPPY,
        CompressionResolver.resolveAlgorithm(SnappyCodec.class.getName()));
    assertEquals(CompressionAlgorithm.SNAPPY,
        CompressionResolver.resolveAlgorithm(new SnappyCodec()));
    assertEquals(CompressionAlgorithm.ZSTD,
        CompressionResolver.resolveAlgorithm(ZStandardCodec.class.getName()));
    assertEquals(CompressionAlgorithm.SNAPPY,
        CompressionResolver.getProvider(CompressionAlgorithm.SNAPPY).getAlgorithm());
    assertIOException(() -> CompressionResolver.getProvider(CompressionAlgorithm.ZSTD));
    assertIOException(() -> CompressionResolver.resolveAlgorithm("example.UnknownCodec"));

    CompressionCodec codec = new SnappyCodec() {
      @Override
      public Class<? extends org.apache.hadoop.io.compress.Compressor> getCompressorType() {
        throw new AssertionError("must not be called");
      }
    };
    // An anonymous subclass has a different class name and therefore fails without invoking it.
    assertIOException(() -> CompressionResolver.resolveAlgorithm(codec));
  }

  @Test
  public void testCompressedIFileRoundTripAndLengthAccounting() throws Exception {
    TrackingPool pool = new TrackingPool();
    ByteArrayOutputStream physical = new ByteArrayOutputStream();
    IFile.WriterBytesWritable writer = new IFile.WriterBytesWritable(
        new FSDataOutputStream(physical, null), new SnappyCodec(), null, null,
        false, -1, -1, IFile.allocateWriteBuffer(), null, pool);
    byte[] key = new byte[] {1, 2, 3};
    byte[] value = new byte[700_000];
    new Random(23).nextBytes(value);
    writer.appendNoRle(new BytesWritable(key), new BytesWritable(value));
    writer.close();

    assertEquals(1, pool.borrowedCompressors);
    assertEquals(1, pool.returnedCompressors);
    assertEquals(physical.size(), writer.getCompressedLength());
    assertTrue(writer.getRawLength() > value.length);

    byte[] raw = new byte[(int) writer.getRawLength()];
    IFile.Reader.readToMemory(raw, new ByteArrayInputStream(physical.toByteArray()),
        physical.size(), new SnappyCodec(), false, 0, taskContext(pool), false);
    assertEquals(1, pool.borrowedDecompressors);
    assertEquals(1, pool.returnedDecompressors);
    assertArrayEquals(Arrays.copyOf(IFile.HEADER, 3), Arrays.copyOf(raw, 3));
    ByteBuffer records = ByteBuffer.wrap(raw, IFile.HEADER.length,
        raw.length - IFile.HEADER.length).order(ByteOrder.nativeOrder());
    long lengths = records.getLong();
    assertEquals(key.length, (int) lengths);
    assertEquals(value.length, (int) (lengths >>> 32));
    byte[] restoredKey = new byte[key.length];
    byte[] restoredValue = new byte[value.length];
    records.get(restoredKey);
    records.get(restoredValue);
    assertArrayEquals(key, restoredKey);
    assertArrayEquals(value, restoredValue);
    long eof = records.getLong();
    assertEquals(IFile.EOF_MARKER, (int) eof);
    assertEquals(IFile.EOF_MARKER, (int) (eof >>> 32));
  }

  private static byte[][] values() {
    byte[] random = new byte[4097];
    new Random(7).nextBytes(random);
    byte[] repeated = new byte[8192];
    Arrays.fill(repeated, (byte) 42);
    return new byte[][] {new byte[0], new byte[] {9}, repeated, random};
  }

  private static byte[] encode(byte[] value, int blockSize) throws Exception {
    ByteArrayOutputStream sink = new ByteArrayOutputStream();
    SnappyCompressionOutputStream out = new SnappyCompressionOutputStream(
        sink, new XerialSnappyCompressor(), blockSize);
    out.write(value);
    out.finish();
    return sink.toByteArray();
  }

  private static byte[] decode(byte[] encoded, int blockSize) throws Exception {
    SnappyCompressionInputStream input = new SnappyCompressionInputStream(
        new ByteArrayInputStream(encoded), new XerialSnappyDecompressor(), blockSize);
    ByteArrayOutputStream restored = new ByteArrayOutputStream();
    byte[] buffer = new byte[11];
    int count;
    while ((count = input.read(buffer)) != -1) {
      restored.write(buffer, 0, count);
    }
    return restored.toByteArray();
  }

  private static byte[] ints(int... values) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    for (int value : values) {
      out.writeInt(value);
    }
    return bytes.toByteArray();
  }

  private static void assertInvalidRanges(
      XerialSnappyCompressor compressor, XerialSnappyDecompressor decompressor) throws Exception {
    try {
      compressor.maxCompressedLength(-1);
      fail("negative length accepted");
    } catch (IllegalArgumentException expected) {
    }
    try {
      compressor.maxCompressedLength(Integer.MAX_VALUE);
      fail("overflowing length accepted");
    } catch (IllegalArgumentException expected) {
    }
    byte[] bytes = new byte[4];
    assertIndexFailure(() -> compressor.compress(bytes, -1, 1, bytes, 0, 4));
    assertIndexFailure(() -> compressor.compress(bytes, 3, 2, bytes, 0, 4));
    assertIndexFailure(() -> compressor.compress(bytes, 0, 1, bytes, 3, 2));
    assertIndexFailure(() -> decompressor.decompress(bytes, 0, -1, bytes, 0, 4));
  }

  private static final class TrackingOutputStream extends ByteArrayOutputStream {
    private boolean closed;

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }
  }

  private static final class TrackingPool implements CompressorPool, DecompressorPool {
    private int borrowedCompressors;
    private int returnedCompressors;
    private int borrowedDecompressors;
    private int returnedDecompressors;

    @Override
    public Compressor getCompressor(CompressionAlgorithm algorithm) {
      assertEquals(CompressionAlgorithm.SNAPPY, algorithm);
      borrowedCompressors++;
      return new XerialSnappyCompressor();
    }

    @Override
    public void returnCompressor(Compressor compressor) {
      assertEquals(CompressionAlgorithm.SNAPPY, compressor.getAlgorithm());
      compressor.reset();
      returnedCompressors++;
    }

    @Override
    public Decompressor getDecompressor(CompressionAlgorithm algorithm) {
      assertEquals(CompressionAlgorithm.SNAPPY, algorithm);
      borrowedDecompressors++;
      return new XerialSnappyDecompressor();
    }

    @Override
    public void returnDecompressor(Decompressor decompressor) {
      assertEquals(CompressionAlgorithm.SNAPPY, decompressor.getAlgorithm());
      decompressor.reset();
      returnedDecompressors++;
    }
  }

  private static TaskContext taskContext(TrackingPool pool) {
    return (TaskContext) Proxy.newProxyInstance(
        TaskContext.class.getClassLoader(), new Class<?>[] {TaskContext.class},
        (proxy, method, arguments) -> {
          if ("getDecompressor".equals(method.getName())) {
            return pool.getDecompressor((CompressionAlgorithm) arguments[0]);
          }
          if ("returnDecompressor".equals(method.getName())) {
            pool.returnDecompressor((Decompressor) arguments[0]);
            return null;
          }
          throw new UnsupportedOperationException(method.getName());
        });
  }

  private interface CheckedRunnable {
    void run() throws Exception;
  }

  private static void assertIOException(CheckedRunnable runnable) throws Exception {
    try {
      runnable.run();
      fail("expected IOException");
    } catch (IOException expected) {
    }
  }

  private static void assertIllegalState(CheckedRunnable runnable) throws Exception {
    try {
      runnable.run();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
  }

  private static void assertIndexFailure(CheckedRunnable runnable) throws Exception {
    try {
      runnable.run();
      fail("expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException expected) {
    }
  }
}
