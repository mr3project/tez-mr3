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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.DataInputBuffer;
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

/**
 * Compares alternatives for ShuffleHeader.readMapId() on ASCII-only map IDs.
 * Each operation reads the four-byte length, allocates and fills a byte array,
 * and decodes it into a String. The DataInputBuffer and wire bytes are reused.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@Threads(1)
public class MapIdDeserializationBenchmark {

  private static final int MAX_ID_LENGTH = 1000;

  @State(Scope.Thread)
  public static class MapIdState {
    @Param({"40", "64", "128"})
    public int mapIdLength;

    byte[] encoded;
    DataInputBuffer input;

    @Setup
    public void setup() throws IOException {
      StringBuilder sb = new StringBuilder("vertex12/attempt_1783582797348_0599_");
      if (mapIdLength < sb.length()) {
        throw new IllegalArgumentException("Map ID length too small");
      }
      while (sb.length() < mapIdLength) {
        sb.append((char) ('0' + (sb.length() % 10)));
      }
      String mapId = sb.toString();
      byte[] bytes = mapId.getBytes(StandardCharsets.US_ASCII);

      DataOutputBuffer output = new DataOutputBuffer(mapIdLength + 4);
      output.writeInt(bytes.length);
      output.write(bytes);
      encoded = Arrays.copyOf(output.getData(), output.getLength());
      input = new DataInputBuffer();

      // Validate the successful wire-format round trip outside measurements.
      input.reset(encoded, encoded.length);
      if (!mapId.equals(readCurrent(input))) {
        throw new IllegalStateException("Text.decode result differs");
      }
      input.reset(encoded, encoded.length);
      if (!mapId.equals(readUtf8(input))) {
        throw new IllegalStateException("UTF-8 result differs");
      }
      input.reset(encoded, encoded.length);
      if (!mapId.equals(readAscii(input))) {
        throw new IllegalStateException("US_ASCII result differs");
      }
      input.reset(encoded, encoded.length);
      if (!mapId.equals(readCheckedAscii(input))) {
        throw new IllegalStateException("Checked US_ASCII result differs");
      }

      // US_ASCII decoding replaces invalid bytes. Checked decoding rejects them.
      byte[] malformed = Arrays.copyOf(encoded, encoded.length);
      malformed[4] = (byte) 0x80;
      input.reset(malformed, malformed.length);
      try {
        readCheckedAscii(input);
        throw new IllegalStateException("Checked US_ASCII accepted a non-ASCII byte");
      } catch (IllegalArgumentException expected) {
        // Expected: the validation branch is exercised only during setup.
      }
    }
  }

  private static byte[] readMapIdBytes(DataInputBuffer in) throws IOException {
    int length = in.readInt();
    if (length < 0 || length > MAX_ID_LENGTH) {
      throw new IllegalArgumentException("Encoded byte size for String was " + length
          + ", which is outside of 0.." + MAX_ID_LENGTH + " range.");
    }
    byte[] bytes = new byte[length];
    in.readFully(bytes, 0, length);
    return bytes;
  }

  private static String readCurrent(DataInputBuffer in) throws IOException {
    return Text.decode(readMapIdBytes(in));
  }

  private static String readUtf8(DataInputBuffer in) throws IOException {
    return new String(readMapIdBytes(in), StandardCharsets.UTF_8);
  }

  private static String readAscii(DataInputBuffer in) throws IOException {
    return new String(readMapIdBytes(in), StandardCharsets.US_ASCII);
  }

  private static String readCheckedAscii(DataInputBuffer in) throws IOException {
    byte[] bytes = readMapIdBytes(in);
    for (byte b : bytes) {
      if (b < 0) {
        throw new IllegalArgumentException("Non-ASCII shuffle map ID");
      }
    }
    return new String(bytes, StandardCharsets.US_ASCII);
  }

  @Benchmark
  public String currentTextDecode(MapIdState state) throws IOException {
    state.input.reset(state.encoded, state.encoded.length);
    return readCurrent(state.input);
  }

  @Benchmark
  public String newStringUtf8(MapIdState state) throws IOException {
    state.input.reset(state.encoded, state.encoded.length);
    return readUtf8(state.input);
  }

  @Benchmark
  public String newStringUsAscii(MapIdState state) throws IOException {
    state.input.reset(state.encoded, state.encoded.length);
    return readAscii(state.input);
  }

  @Benchmark
  public String checkedUsAscii(MapIdState state) throws IOException {
    state.input.reset(state.encoded, state.encoded.length);
    return readCheckedAscii(state.input);
  }
}
