# Shuffle map ID serialization and deserialization JMH benchmarks

This standalone benchmark compares three ways to write a composite shuffle map ID
to Hadoop's real `DataOutputBuffer`:

1. **currentTextEncode**: `Text.encode(mapId)`, followed by the same length and
   byte-array writes as `ShuffleHeader.writeMapId()`.
2. **directWriteBytes**: `writeInt(mapId.length())` followed by
   `writeBytes(mapId)`.
3. **getBytesUsAscii**: `mapId.getBytes(StandardCharsets.US_ASCII)` followed by
   a length and a bulk byte-array write.

The methods use **identical ASCII-only input** of 40, 64, or 128 characters.
A setup-time check verifies that the serialized bytes, including the four-byte
length prefix, match for all three implementations. The same preallocated
`DataOutputBuffer` is reset for each invocation to isolate the encoding/writing
costs, rather than measuring buffer construction or capacity growth.

## Build and run

Requires Java 21 and Maven. The benchmark has its own Maven project so the
normal Tez build and test dependencies are unaffected.

```bash
mvn -f benchmarks/map-id-jmh/pom.xml -DskipTests package

java -jar benchmarks/map-id-jmh/target/map-id-jmh.jar \
  'MapIdSerializationBenchmark' -prof gc

java -jar benchmarks/map-id-jmh/target/map-id-jmh.jar \
  'MapIdDeserializationBenchmark' -prof gc
```

JMH reports average time in nanoseconds per invocation (`avgt`) and the GC
profiler reports allocation bytes per operation (`gc.alloc.rate.norm`).
By default there are two forks, three 1-second warmup iterations, and five
1-second measurement iterations per benchmark and map-ID length.

To run a shorter initial experiment:

```bash
java -jar benchmarks/map-id-jmh/target/map-id-jmh.jar \
  'MapIdSerializationBenchmark' -f 1 -wi 2 -i 3 -r 1s -w 1s -prof gc
```

## Deserialization variants

`MapIdDeserializationBenchmark` measures the full
`ShuffleHeader.readMapId()`-style operation: read a 4-byte length, validate
the maximum 1000-byte ID length, allocate and read the byte array, and
construct the returned String. It compares:

1. **currentTextDecode**: `Text.decode(bytes)` (the current implementation).
2. **newStringUtf8**: `new String(bytes, StandardCharsets.UTF_8)`.
3. **newStringUsAscii**: `new String(bytes, StandardCharsets.US_ASCII)`.
4. **checkedUsAscii**: explicitly reject bytes with the high bit set, then
   decode using `US_ASCII`. This measures the cost of strict validation
   versus silently replacing non-ASCII bytes.

Each variant receives the same length-prefixed ASCII-only map ID of 40, 64,
or 128 characters. A per-thread `DataInputBuffer` is reset for each operation,
and the decoded String is returned to JMH to prevent dead-code elimination.
The byte-array read/allocation and length checks are included in all variants.
Setup checks verify identical decoded results and confirm that strict ASCII
decoding rejects a byte outside the ASCII range. Malformed input is not timed.

To run just the deserialization benchmarks:

```bash
java -jar benchmarks/map-id-jmh/target/map-id-jmh.jar \
  'MapIdDeserializationBenchmark' -prof gc
```

Results apply to composite MR3 shuffle map IDs. The non-composite Hadoop
shuffle protocol uses a different header-reading method. Also note that the
MR3 ShuffleHandler currently embeds arbitrary exception text in a special
disk-error map ID; enforcing ASCII in that path is a separate production
change and is **not** performed by this benchmark.

## Java 21+ and dnsjava service loading

Hadoop 3.4.1 transitively brings in dnsjava. Its multi-release JAR may
contribute `META-INF/services/java.net.spi.InetAddressResolverProvider` to
an uber-JAR without a loadable provider class. That would cause
`DnsjavaInetAddressResolverProvider not found` when JMH launches a fork.
The shade configuration deliberately excludes this unnecessary DNS SPI
registration from the standalone benchmark JAR.

After upgrading this benchmark, rebuild the JAR with the Maven `clean` goal
so no resources from a previous shaded build remain:

```bash
mvn -f benchmarks/map-id-jmh/pom.xml clean package -DskipTests
```

## Interpretation

The benchmark compares serialization only, not overall `sendMapOutput()`
latency. In production, `sendMapOutput()` constructs a new
`DataOutputBuffer`, serializes partition metadata, and writes shuffle data;
those costs are not included. JMH results should be measured on the same JDK
and host as the MR3 workload before choosing an implementation.
