/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.benchmarks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Compares the three ways of writing an ASCII-only shuffle map ID into a
 * DataOutputBuffer. The current variant reproduces exactly the two operations
 * performed by ShuffleHeader.encodeMapId() and ShuffleHeader.writeMapId().
 *
 * The buffer is reused to isolate the map-ID serialization costs; constructing
 * a new DataOutputBuffer for each map output is deliberately not measured.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@Threads(1)
public class MapIdSerializationBenchmark {

  @State(Scope.Thread)
  public static class MapIdState {
    @Param({"40", "64", "128"})
    public int mapIdLength;

    String mapId;
    DataOutputBuffer output;

    @Setup
    public void setup() throws IOException {
      // MR3 map IDs contain a vertex prefix, a task-attempt ID, and a
      // numeric unique suffix. Use deterministic ASCII data of varying length.
      StringBuilder sb = new StringBuilder("vertex12/attempt_1783582797348_0599_");
      if (mapIdLength < sb.length()) {
        throw new IllegalArgumentException("Map ID length too small");
      }
      while (sb.length() < mapIdLength) {
        sb.append((char) ('0' + (sb.length() % 10)));
      }
      mapId = sb.toString();
      for (int i = 0; i < mapId.length(); i++) {
        if (mapId.charAt(i) > 0x7f) {
          throw new IllegalStateException("Non-ASCII map ID");
        }
      }

      // Check byte-for-byte equivalence, including the four-byte length prefix.
      verifyEncodings(mapId);

      // Preallocate to exclude buffer growth from the measured operation.
      output = new DataOutputBuffer(mapIdLength + 4);
    }
  }

  private static void writeCurrent(DataOutputBuffer out, String mapId) throws IOException {
    // Equivalent to ShuffleHeader.encodeMapId() + writeMapId().
    ByteBuffer mapIdBytes = Text.encode(mapId);
    out.writeInt(mapIdBytes.limit());
    out.write(mapIdBytes.array(), 0, mapIdBytes.limit());
  }

  private static void writeBytes(DataOutputBuffer out, String mapId) throws IOException {
    out.writeInt(mapId.length());
    out.writeBytes(mapId);
  }

  private static void writeAscii(DataOutputBuffer out, String mapId) throws IOException {
    byte[] bytes = mapId.getBytes(StandardCharsets.US_ASCII);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private static void verifyEncodings(String mapId) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    writeCurrent(out, mapId);
    byte[] expected = Arrays.copyOf(out.getData(), out.getLength());

    out.reset();
    writeBytes(out, mapId);
    if (!Arrays.equals(expected, Arrays.copyOf(out.getData(), out.getLength()))) {
      throw new IllegalStateException("writeBytes differs from the current encoding");
    }

    out.reset();
    writeAscii(out, mapId);
    if (!Arrays.equals(expected, Arrays.copyOf(out.getData(), out.getLength()))) {
      throw new IllegalStateException("US_ASCII differs from the current encoding");
    }
  }

  private static void consumeOutput(DataOutputBuffer out, Blackhole bh) {
    // Consume the backing array as well as its logical length so writes cannot
    // be removed merely because only the resulting size is observed.
    bh.consume(out.getData());
    bh.consume(out.getLength());
  }

  @Benchmark
  public void currentTextEncode(MapIdState state, Blackhole bh) throws IOException {
    state.output.reset();
    writeCurrent(state.output, state.mapId);
    consumeOutput(state.output, bh);
  }

  @Benchmark
  public void directWriteBytes(MapIdState state, Blackhole bh) throws IOException {
    state.output.reset();
    writeBytes(state.output, state.mapId);
    consumeOutput(state.output, bh);
  }

  @Benchmark
  public void getBytesUsAscii(MapIdState state, Blackhole bh) throws IOException {
    state.output.reset();
    writeAscii(state.output, state.mapId);
    consumeOutput(state.output, bh);
  }
}
