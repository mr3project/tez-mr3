# Snappy IFile JMH benchmark

This module measures CPU-oriented, **end-to-end IFile segment operations** with
the current Tez-MR3 implementation. It does **not** implement adaptive
compression. It is a baseline for deciding whether bypassing Snappy for small
IFile segments is worthwhile.

## Build (Java 21)

From the root of the `tez-mr3` checkout:

```bash
mvn -Pifile-jmh -pl tez-runtime-library-jmh -am -Dmaven.test.skip=true package
```

This creates:

```text
tez-runtime-library-jmh/target/benchmarks.jar
```

The `ifile-jmh` profile is optional, so an ordinary Tez build is not affected.
The normal Tez-MR3 build dependencies (including its locally provided MR3
artifacts, where applicable) must already be available.

List the benchmarks:

```bash
java -jar tez-runtime-library-jmh/target/benchmarks.jar -l
```

## Run

From the repository root, run **all 36 Snappy write configurations**:

```bash
java -jar tez-runtime-library-jmh/target/benchmarks.jar \
  'SnappyIFileBenchmark.writeIFile' \
  -rf csv -rff snappy-ifile-write.csv
```

Run the **36 separate read configurations**:

```bash
java -jar tez-runtime-library-jmh/target/benchmarks.jar \
  'SnappyIFileBenchmark.readIFile' \
  -rf csv -rff snappy-ifile-read.csv
```

Default measurement settings are 5 warmup iterations x 2 seconds,
10 measurement iterations x 2 seconds, 3 forks, 1 thread.
A complete sweep takes considerable time.

For a fast smoke run (one parameter combination, both operations):

```bash
java -jar tez-runtime-library-jmh/target/benchmarks.jar \
  'SnappyIFileBenchmark' \
  -p payloadSize=256 -p dataType=RANDOM -p compression=COMPRESSED \
  -wi 1 -i 2 -w 1s -r 1s -f 1
```

For a **quick full matrix** before a long run:

```bash
java -jar tez-runtime-library-jmh/target/benchmarks.jar \
  'SnappyIFileBenchmark.writeIFile' \
  -wi 2 -i 3 -w 1s -r 1s -f 1 \
  -rf csv -rff snappy-ifile-write-quick.csv
```

## Benchmark parameters

- `payloadSize`: 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384
- `dataType`: `REPETITIVE` (repeated A bytes), `RANDOM` (fixed-seed pseudorandom)
- `compression`: `COMPRESSED` (Snappy), `UNCOMPRESSED` (no compression)

Each IFile holds **one record** with a 16-byte key and a
`payloadSize - 16` byte value. `payloadSize` counts only key + value
bytes, NOT IFile metadata. For one ordinary non-RLE record,
`getRawLength() == payloadSize + 20`: 4-byte header,
8 bytes of key/value lengths, and 8-byte EOF. The CRC adds another
4 bytes to the physical segment size.

Snappy uses a fixed 64 KiB block buffer so all tested segments fit in
one block. The compressed writer borrows the compressor's input buffer.

## What is measured?

- **`writeIFile`**: creates a new IFile writer, appends one record,
  writes EOF, finishes Snappy if selected, writes the CRC32, closes the
  logical segment. The shared underlying `FSDataOutputStream` is NOT
  closed. It discards output bytes without copying them into a storage
  buffer, but samples the final emitted byte (the CRC trailer) so that
  the compiler cannot discard the encoding work. Output positions are
  still accounted for. A compressor is
  reused per JMH thread; its creation is outside the timed operation.
- **`readIFile`**: creates a new IFile reader over already-encoded bytes,
  reads one record and EOF, verifies CRC32, closes the reader. A
  decompressor is reused per JMH thread; encoded input preparation is
  outside the timed operation. The Snappy provider remains configured
  for the uncompressed case, so the per-segment IFile flag controls
  decompression exactly as in the proposed adaptive format.

Each benchmark uses `AverageTime` in microseconds per complete IFile
segment. There is no disk or network activity; input data generation
and read input encoding are performed in `@Setup(Level.Trial)`.
These are wall-clock timings intended to approximate isolated CPU
costs, not direct per-thread CPU-time accounting.

The compressed-versus-uncompressed difference is an **upper bound**
on the achievable improvement of an adaptive writer: the latter might
still initialize a compressor before it knows the segment's size.
Use the same benchmark infrastructure to compare a future
`ADAPTIVE` mode against the current `COMPRESSED` mode.

## Analyze

For each payload size and data type:

```text
write savings (us/segment) = compressed write time - uncompressed write time
read savings  (us/segment) = compressed read time  - uncompressed read time
combined savings          = write savings + read savings
```

Compare **absolute microseconds per segment**, not just MB/s or
percentages. JMH score errors and variance matter for small results.
These measurements omit network costs, so they do not establish whether
sending the larger uncompressed segments improves end-to-end query time.
