/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.benchmarks;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.runtime.api.CompressionProvider;
import org.apache.tez.runtime.api.DecompressorPool;
import org.apache.tez.runtime.io.compress.Compressor;
import org.apache.tez.runtime.io.compress.Decompressor;
import org.apache.tez.runtime.io.compress.XerialSnappyCompressionProvider;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Reader.KeyState;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures one complete IFile segment, not one Snappy codec invocation.
 *
 * payloadSize is the combined length of one key and one value. The IFile
 * framing adds 8 bytes of record lengths, 8 bytes of EOF, 4 bytes of header,
 * and 4 bytes of checksum (the checksum is not included in getRawLength()).
 *
 * A shared, non-owning FSDataOutputStream and a per-thread compressor model
 * the sequential-partition writer path without disk or network I/O. The
 * compressed writer reuses the same compressor across benchmark operations.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
@Fork(3)
@Threads(1)
public class SnappyIFileBenchmark {

  private static final int KEY_SIZE = 16;
  // Larger than the maximum tested segment: all test payloads fit in one Snappy block.
  private static final int SNAPPY_BLOCK_SIZE = 64 * 1024;

  @State(Scope.Thread)
  public static class InputState {

    @Param({"64", "128", "256", "512", "1024",
        "2048", "4096", "8192", "16384"})
    public int payloadSize;

    @Param({"REPETITIVE", "RANDOM"})
    public String dataType;

    @Param({"COMPRESSED", "UNCOMPRESSED"})
    public String compression;

    private BytesWritable key;
    private BytesWritable value;
    private BytesWritable readKey;
    private BytesWritable readValue;
    private CompressionProvider codec;
    private Compressor compressor;
    private Decompressor decompressor;
    private DecompressorPool decompressorPool;
    private FSDataOutputStream sharedOutput;
    private DiscardingOutputStream outputSink;
    private byte[] uncompressedWriteBuffer;
    private byte[] encoded;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      if (payloadSize < KEY_SIZE) {
        throw new IllegalArgumentException("Payload must fit the fixed key");
      }
      if (!"COMPRESSED".equals(compression) && !"UNCOMPRESSED".equals(compression)) {
        throw new IllegalArgumentException("Unknown compression: " + compression);
      }

      byte[] keyBytes = new byte[KEY_SIZE];
      byte[] valueBytes = new byte[payloadSize - KEY_SIZE];
      if ("REPETITIVE".equals(dataType)) {
        Arrays.fill(keyBytes, (byte) 'A');
        Arrays.fill(valueBytes, (byte) 'A');
      } else if ("RANDOM".equals(dataType)) {
        Random random = new Random(0x51A99EEDL + payloadSize);
        random.nextBytes(keyBytes);
        random.nextBytes(valueBytes);
      } else {
        throw new IllegalArgumentException("Unknown data type: " + dataType);
      }
      key = new BytesWritable(keyBytes);
      value = new BytesWritable(valueBytes);
      readKey = new BytesWritable();
      readValue = new BytesWritable();

      // The reader is always supplied with the configured codec. Each IFile's
      // header flag determines whether decompression is actually performed.
      codec = new XerialSnappyCompressionProvider(SNAPPY_BLOCK_SIZE);
      compressor = codec.createCompressor();
      // Reuse the raw buffer across IFiles, just as the compressed writer
      // reuses its compressor-owned buffer. No allocation is timed.
      uncompressedWriteBuffer = new byte[SNAPPY_BLOCK_SIZE];
      decompressor = codec.createDecompressor();
      decompressorPool = new SingleDecompressorPool(decompressor);
      outputSink = new DiscardingOutputStream();
      sharedOutput = new FSDataOutputStream(outputSink, null);

      // Encode once OUTSIDE the measured method to prepare the read benchmark.
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(payloadSize + 128);
      FSDataOutputStream memoryOutput = new FSDataOutputStream(bytes, null);
      IFile.WriterBytesWritable writer = newWriter(memoryOutput);
      writer.appendNoRle(key, value);
      writer.close();
      if (writer.getRawLength() != payloadSize + 20L) {
        throw new IllegalStateException("Unexpected IFile raw length: " + writer.getRawLength());
      }
      if (writer.getCompressedLength() != bytes.size()) {
        throw new IllegalStateException("Unexpected IFile output length");
      }
      encoded = bytes.toByteArray();
      memoryOutput.close();

      // Validate the exact path (including header flags and CRC) once per trial.
      IFile.Reader reader = newReader();
      try {
        if (reader.readRawKey(readKey) != KeyState.NEW_KEY) {
          throw new IllegalStateException("Missing IFile record");
        }
        reader.nextRawValue(readValue);
        if (readKey.getLength() != key.getLength() || readValue.getLength() != value.getLength()) {
          throw new IllegalStateException("IFile payload lengths changed");
        }
        for (int i = 0; i < key.getLength(); i++) {
          if (key.getBytesRaw()[i] != readKey.getBytesRaw()[i]) {
            throw new IllegalStateException("IFile key changed");
          }
        }
        for (int i = 0; i < value.getLength(); i++) {
          if (value.getBytesRaw()[i] != readValue.getBytesRaw()[i]) {
            throw new IllegalStateException("IFile value changed");
          }
        }
        if (reader.readRawKey(readKey) != KeyState.NO_KEY) {
          throw new IllegalStateException("Extra IFile record");
        }
      } finally {
        reader.close();
      }
    }

    private IFile.WriterBytesWritable newWriter(FSDataOutputStream output) throws IOException {
      boolean useCompression = "COMPRESSED".equals(compression);
      return new IFile.WriterBytesWritable(
          output, useCompression ? codec : null,
          null, null, false, -1, -1,
          useCompression ? null : uncompressedWriteBuffer,
          useCompression ? compressor : null, null);
    }

    private IFile.Reader newReader() throws IOException {
      return new IFile.Reader(
          new ByteArrayInputStream(encoded), encoded.length,
          codec, null, null, false, 0, decompressorPool, false);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      if (sharedOutput != null) {
        sharedOutput.close();
      }
      if (compressor != null) {
        compressor.close();
      }
      if (decompressor != null) {
        decompressor.close();
      }
    }
  }

  /**
   * No output copy or device I/O. Sampling the last emitted byte (normally
   * part of the CRC trailer) makes the computed content observable to JMH,
   * preventing the JVM from eliding otherwise-unused encoding work.
   */
  private static final class DiscardingOutputStream extends OutputStream {
    private int lastByte;

    @Override
    public void write(int value) {
      lastByte = value & 0xff;
    }

    @Override
    public void write(byte[] data, int offset, int length) {
      if (length > 0) {
        lastByte = data[offset + length - 1] & 0xff;
      }
    }

    int lastByte() {
      return lastByte;
    }
  }

  /**
   * The reader returns the same per-thread decompressor after each segment.
   * This excludes pool creation/acquisition contention, but retains per-segment
   * decompressor reset and codec stream setup.
   */
  private static final class SingleDecompressorPool implements DecompressorPool {
    private final Decompressor decompressor;

    private SingleDecompressorPool(Decompressor decompressor) {
      this.decompressor = decompressor;
    }

    @Override
    public Decompressor getDecompressor(CompressionProvider codec) {
      return decompressor;
    }

    @Override
    public void returnDecompressor(Decompressor returned) {
      if (returned != decompressor) {
        throw new IllegalStateException("Unexpected decompressor");
      }
    }

    @Override
    public Decompressor getDecompressorPerDag(CompressionProvider codec) {
      return decompressor;
    }

    @Override
    public void returnDecompressorPerDag(Decompressor returned) {
      returnDecompressor(returned);
    }
  }

  /**
   * Includes new IFile.Writer, one append, close(), compression (if enabled),
   * framing and CRC32; excludes opening/closing the shared underlying stream.
   */
  @Benchmark
  public long writeIFile(InputState state) throws IOException {
    IFile.WriterBytesWritable writer = state.newWriter(state.sharedOutput);
    writer.appendNoRle(state.key, state.value);
    writer.close();
    return writer.getCompressedLength() ^ state.outputSink.lastByte();
  }

  /**
   * Includes new IFile.Reader, reading one record and EOF, checksum
   * verification, decompression (if enabled), and reader close.
   */
  @Benchmark
  public int readIFile(InputState state) throws IOException {
    IFile.Reader reader = state.newReader();
    try {
      if (reader.readRawKey(state.readKey) != KeyState.NEW_KEY) {
        throw new IOException("Missing IFile record");
      }
      reader.nextRawValue(state.readValue);
      int length = state.readKey.getLength() + state.readValue.getLength();
      if (reader.readRawKey(state.readKey) != KeyState.NO_KEY) {
        throw new IOException("Unexpected additional IFile record");
      }
      return length;
    } finally {
      reader.close();
    }
  }
}
