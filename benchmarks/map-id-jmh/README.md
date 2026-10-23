# Shuffle map ID serialization JMH benchmark

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
