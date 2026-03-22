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
package org.apache.tez.runtime.library.common.sort.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.tez.runtime.api.DecompressorPool;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.api.TezTaskOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.tez.runtime.library.utils.CodecUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.tez.common.counters.TezCounter;

import javax.annotation.Nullable;

/**
 * <code>IFile</code> is the simple <key-len, value-len, key, value> format
 * for the intermediate map-outputs in Map-Reduce.
 *
 * There is a <code>Writer</code> to write out map-outputs in this format and
 * a <code>Reader</code> to read files of this format.
 */
public class IFile {
  private static final Logger LOG = LoggerFactory.getLogger(IFile.class);
  private static final boolean isDebugEnabled = LOG.isDebugEnabled();

  // HEADER default = uncompressed (0)
  public static final byte[] HEADER = new byte[] { (byte) 'T', (byte) 'I', (byte) 'F' , (byte) 0};

  private static final String INCOMPLETE_READ = "Requested to read %d got %d";
  private static final String REQ_BUFFER_SIZE_TOO_LARGE = "Size of data %d is greater than the max allowed of %d";

  private static final ThreadLocal<Decompressor> decompressorHolder = new ThreadLocal<>();

  public static final int INT_SIZE = 4;

  // Cf. see TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_MAX_SIZE
  public static final int WRITER_BUFFER_SIZE_DEFAULT = 4 * 1024;

  public static byte[] allocateWriteBuffer() {
    return new byte[WRITER_BUFFER_SIZE_DEFAULT];
  }

  public static byte[] allocateWriteBufferSingle() {
    return new byte[8 * 2];   // TODO: 8 bytes is enough
  }

  public interface WriterAppend {
    void append(DataInputBuffer key, DataInputBuffer value) throws IOException;
    void close() throws IOException;
    SectionLayout getSectionLayout();   // valid after close()
  }

  public static final int checksumSize = IFileOutputStream.getCheckSumSize();

  /**
   * For basic cache size checks: header + one section checksum trailer.
   *
   * @return size of the base cache needed
   */
  // only for values section, not for a full partition
  static int getBaseCacheSize() {
    return (HEADER.length + checksumSize);
  }

  /**
   * Base size estimate used by FileBackedInMemIFileWriter spill accounting for
   * a full partition: header + checksums for values, keys and lengths sections.
   */
  static int getBasePartitionSizeEstimate() {
    return HEADER.length + (3 * checksumSize);
  }

  // IFile layout with three sections:
  //   HEADER ++ [values, checksum] ++ [keys, checksum] ++ [lengths, checksum]
  //             ^                     ^                   ^                  ^
  //             valuesStart           keysStart           lengthsStart       totalLength
  // where valuesStart == HEADER.length
  public static final class SectionLayout {

    public final long valuesStart;
    public final long keysStart;
    public final long lengthsStart;
    public final long totalLength;
    public final long totalNumRecordsWritten;   // total number of records (or keys) in the input stream

    // derived values
    // valuesLength, keysLength, lengthsLength do NOT include checksum.
    public final long valuesLength, keysLength, lengthsLength;

    // number of bytes of each section when decompressed
    // equal to valuesLength, keysLength, lengthsLength if not compressed
    public final long valuesRawLength;
    public final long keysRawLength;
    public final long lengthsRawLength;

    // derived value
    // total number of bytes (including HEADER and three checksums) when decompressed
    // totalLength == totalRawLength if not compressed
    public final long totalRawLength;

    public SectionLayout(
        long valuesStart, long keysStart, long lengthsStart,
        long totalLength,
        long totalNumRecordsWritten,
        long valuesRawLength, long keysRawLength, long lengthsRawLength) {
      assert valuesStart == HEADER.length;

      this.valuesStart = valuesStart;
      this.keysStart = keysStart;
      this.lengthsStart = lengthsStart;
      this.totalLength = totalLength;
      this.totalNumRecordsWritten = totalNumRecordsWritten;

      // derived values
      this.valuesLength = keysStart - valuesStart - checksumSize;
      this.keysLength = lengthsStart - keysStart - checksumSize;
      this.lengthsLength = totalLength - lengthsStart - checksumSize;

      this.valuesRawLength = valuesRawLength;
      this.keysRawLength = keysRawLength;
      this.lengthsRawLength = lengthsRawLength;

      // derived value
      this.totalRawLength = HEADER.length + valuesRawLength + keysRawLength + lengthsRawLength + 3L * checksumSize;

      assert valuesLength >= 0L && keysLength >= 0L && lengthsLength >= 0L;
      assert totalLength == HEADER.length + valuesLength + keysLength + lengthsLength + 3L * checksumSize;
    }

    public long payloadLength() {
      return valuesLength + keysLength + lengthsLength;
    }

    public boolean isValidForByteArray() {
      return totalLength < Integer.MAX_VALUE;
    }
  }

  /**
   * IFileWriter which stores data in memory for specified limit, beyond
   * which it falls back to file based writer. It creates files lazily on
   * need basis and avoids any disk hit (in cases, where data fits entirely in mem).
   * <p>
   * This class should not make any changes to IFile logic and should just flip streams
   * from mem to disk on need basis.
   *
   * During write, it verifies whether the final logical partition can fit in memory.
   * The bounded in-memory stream stores only values-section bytes (header + values +
   * values checksum). Spill decision is based on an estimated final partition size
   * (header + values/keys/lengths logical bytes + section checksum overhead).
   */
  public static class FileBackedInMemIFileWriter extends Writer {

    private final FileSystem fs;
    private boolean bufferFull;

    // For lazy creation of file
    private final TezTaskOutput taskOutput;
    private int totalSize;

    private Path outputPath;
    private final CompressionCodec fileCodec;
    private final BoundedByteArrayOutputStream cacheStream;

    /**
     * * Note that we do not allow compression in in-mem stream.
     * When spilled over to file, compression gets enabled.
     * @param keySerialization
     * @param valSerialization
     * @param fs
     * @param taskOutput
     * @param keyClass
     * @param valueClass
     * @param codec
     * @param writesCounter
     * @param serializedBytesCounter
     * @param cacheSize
     * @throws IOException
     */
    public FileBackedInMemIFileWriter(Serialization<?> keySerialization,
        Serialization<?> valSerialization, FileSystem fs, TezTaskOutput taskOutput,
        Class<?> keyClass, Class<?> valueClass, CompressionCodec codec, TezCounter writesCounter,
        TezCounter serializedBytesCounter, int cacheSize, byte[] writeBuffer) throws IOException {
      super(keySerialization, valSerialization,
          new FSDataOutputStream(createBoundedBuffer(cacheSize), null),
          keyClass, valueClass, null,
          writesCounter, serializedBytesCounter, false, writeBuffer, null);
      this.fs = fs;
      this.cacheStream = (BoundedByteArrayOutputStream) this.rawOut.getWrappedStream();
      this.taskOutput = taskOutput;
      this.bufferFull = (cacheStream == null);
      this.totalSize = getBasePartitionSizeEstimate();
      this.fileCodec = codec;
    }

    boolean shouldWriteToDisk() {
      return totalSize >= cacheStream.getLimit();
    }

    /**
     * Create in-memory values buffer. If requested size is too small, adjust to
     * the minimum needed for header + values-section checksum trailer.
     *
     * @param size
     * @return in memory stream
     */
    public static BoundedByteArrayOutputStream createBoundedBuffer(int size) {
      int resize = Math.max(getBaseCacheSize(), size);
      return new BoundedByteArrayOutputStream(resize);
    }

    /**
     * Flip over from memory to file based writer.
     *
     * 1. Values buffer format at spill time: HEADER + values payload + CHECKSUM.
     * 2. Before flipping, close checksum stream, so that checksum is written
     * out.
     * 3. Create relevant file based writer.
     * 4. Write header and then real data.
     *
     * @throws IOException
     */
    private void resetToFileBasedWriter() throws IOException {
      // Close out stream, so that data checksums are written.
      // Temporary in-memory values section = HEADER + (uncompressed) values payload + CHECKSUM
      finishValuesSection();

      // Get the buffer which contains data in memory
      BoundedByteArrayOutputStream bout = (BoundedByteArrayOutputStream) this.rawOut.getWrappedStream();

      // Create new file based writer
      if (outputPath == null) {
        outputPath = taskOutput.getOutputFileForWrite();
      }
      LOG.info("Switching from mem stream to disk stream. File: " + outputPath);
      FSDataOutputStream newRawOut = fs.create(outputPath);
      this.rawOut = newRawOut;
      this.ownOutputStream = true;  // because of fs.create(outputPath)

      resetValuesSectionOutput(fileCodec);

      // Write header to file
      headerWritten = false;
      writeHeader(newRawOut);

      // write values section payload produced so far (without header and checksum)
      int sPos = HEADER.length;
      int len = (bout.size() - checksumSize - HEADER.length);
      replayValuesPayload(bout.getBuffer(), sPos, len);

      bufferFull = true;
      bout.reset();
    }

    private void replayValuesPayload(byte[] data, int offset, int length) throws IOException {
      if (length > 0) {
        bufferWriteBytes(data, offset, length);
      }
    }

    @Override
    protected void writeKVPair(byte[] keyData, int keyPos, int keyLength,
        byte[] valueData, int valPos, int valueLength) throws IOException {
      if (!bufferFull) {
        totalSize += INT_SIZE + keyLength + INT_SIZE + valueLength;

        if (shouldWriteToDisk()) {
          resetToFileBasedWriter();
        }
      }
      super.writeKVPair(keyData, keyPos, keyLength, valueData, valPos, valueLength);
    }

    /**
     * Check if data was flushed to disk.
     *
     * @return whether data is flushed to disk ot not
     */
    public boolean isDataFlushedToDisk() {
      return bufferFull;
    }

    /**
     * Get cached data if any
     *
     * @return if data is not flushed to disk, it returns in-mem contents
     */
    public ByteBuffer getData() {
      assert !isDataFlushedToDisk();
      return ByteBuffer.wrap(cacheStream.getBuffer(), 0, cacheStream.size());
    }

    public Path getOutputPath() {
      return this.outputPath;
    }
  }

  /**
   * <code>IFile.Writer</code> to write out intermediate map-outputs.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static class Writer implements WriterAppend {
    // Values section output (written during append):
    // rawOut <-- valuesChecksumOut <-- valuesCompressedOut <-- valuesOut <-- [writeBuffer]
    protected DataOutputStream valuesOut;
    private final long start;

    private CompressionOutputStream valuesCompressedOut;
    private Compressor valuesCompressor;
    private boolean compressOutput = false;

    private IFileOutputStream valuesChecksumOut;
    private final CompressionCodec codec;

    protected FSDataOutputStream rawOut;
    // true iff this Writer created and owns rawOut
    // if true, close() closes rawOut.
    protected boolean ownOutputStream = false;

    // logicalValueBytesWritten, storedValueBytesWritten = only for values, not including checksum
    // logicalKeyBytesWritten, storedKeyBytesWritten = only for keys, not including checksum
    // logicalKeyLengthWritten, storedLengthBytesWritten = only for lengths, not including checksum

    // the actual number of bytes written to disk/memory, excluding checksum
    // without compression, stored???BytesWritten = logical???BytesWritten
    private long storedValueBytesWritten = 0;
    private long storedKeyBytesWritten = 0;
    private long storedLengthBytesWritten = 0;

    private long valuesRawLength = 0;
    private long keysRawLength = 0;
    private long lengthsRawLength = 0;

    private long totalNumRecordsWritten = 0;   // TODO: with RLE, we should use numKeysWritten
    private final TezCounter writtenRecordsCounter;
    private final TezCounter serializedUncompressedBytes;

    private final boolean closeSerializers;
    private Serializer keySerializer = null;
    private Serializer valueSerializer = null;

    private final DataOutputBuffer buffer = new DataOutputBuffer();
    private final DataOutputBuffer keySectionBuffer = new DataOutputBuffer();
    private final DataOutputBuffer lengthsSectionBuffer = new DataOutputBuffer();
    protected boolean headerWritten = false;

    // We use writeBuffer[] to reduce the number of writes to 'out' and thus
    // to reduce the number of writes to 'compressedOut'.
    private final byte[] writeBuffer;
    private final int writeBufferLength;        // must be larger than 8 (# of bytes in long)
    private final ByteBuffer writeByteBuffer;
    private int writeOffset;

    private final Compressor compressorExternal;  // not to be shared with concurrent threads

    public Writer(Serialization keySerialization, Serialization valSerialization,
                  FileSystem fs, Path file,
                  Class keyClass, Class valueClass,
                  CompressionCodec codec,
                  TezCounter writesCounter,
                  TezCounter serializedBytesCounter,
                  byte[] writeBuffer) throws IOException {
      this(keySerialization, valSerialization, fs.create(file), keyClass, valueClass, codec,
           writesCounter, serializedBytesCounter, false, writeBuffer, null);
      ownOutputStream = true;   // because of fs.create(file)
    }

    public Writer(Serialization keySerialization, Serialization valSerialization,
                  FSDataOutputStream outputStream,
                  Class keyClass, Class valueClass,
                  CompressionCodec codec, TezCounter writesCounter, TezCounter serializedBytesCounter,
                  boolean rle,  // TODO: to implement for the efficiency of SpanMerger
                  byte[] writeBuffer,
                  @Nullable Compressor compressorExternal) throws IOException {
      this.rawOut = outputStream;
      this.writtenRecordsCounter = writesCounter;
      this.serializedUncompressedBytes = serializedBytesCounter;
      this.start = this.rawOut.getPos();
      this.writeBuffer = writeBuffer;
      this.writeBufferLength = writeBuffer.length;
      this.writeByteBuffer = ByteBuffer.wrap(writeBuffer).order(ByteOrder.BIG_ENDIAN);
      this.writeOffset = 0;

      this.compressorExternal = compressorExternal;
      this.codec = codec;

      setupOutputStream(codec);
      writeHeader(outputStream);

      if (keyClass != null) {
        this.closeSerializers = true;
        this.keySerializer = keySerialization.getSerializer(keyClass);
        this.keySerializer.open(buffer);
        this.valueSerializer = valSerialization.getSerializer(valueClass);
        this.valueSerializer.open(buffer);
      } else {
        this.closeSerializers = false;
      }
    }

    void setupOutputStream(CompressionCodec codec) throws IOException {
      this.valuesChecksumOut = new IFileOutputStream(this.rawOut);
      if (codec != null) {
        if (compressorExternal != null) {
          this.valuesCompressor = compressorExternal;
        } else {
          this.valuesCompressor = CodecUtils.getCompressor(codec);
        }
        if (this.valuesCompressor != null) {
          this.valuesCompressor.reset();
          this.valuesCompressedOut = CodecUtils.createOutputStream(codec, valuesChecksumOut, valuesCompressor);
          this.valuesOut = new DataOutputStream(this.valuesCompressedOut);
          this.compressOutput = true;
        } else {
          throw new IOException("Could not obtain compressor from CodecPool for values section");
        }
      } else {
        this.valuesOut = new DataOutputStream(valuesChecksumOut);
      }
    }

    protected void resetValuesSectionOutput(CompressionCodec codec) throws IOException {
      // because this is called only from resetToFileBasedWriter() temporary in-memory values section is uncompressed
      assert !this.compressOutput && this.valuesCompressor == null && this.compressorExternal == null;

      this.valuesOut = null;
      this.valuesCompressedOut = null;
      this.valuesChecksumOut = null;

      setupOutputStream(codec);
    }

    protected void writeHeader(OutputStream outputStream) throws IOException {
      if (!headerWritten) {
        outputStream.write(HEADER, 0, HEADER.length - 1);
        outputStream.write((compressOutput) ? (byte) 1 : (byte) 0);
        headerWritten = true;
      }
    }

    public void close() throws IOException {
      // When IFile writer is created by BackupStore, we do not have
      // Key and Value classes set. So, check before closing the
      // serializers
      if (closeSerializers) {
        keySerializer.close();
        valueSerializer.close();
      }

      finishValuesSection();
      storedValueBytesWritten = rawOut.getPos() - (start + HEADER.length) - checksumSize;

      // values section is complete; compressor object can now be reused
      valuesOut = null;
      valuesCompressedOut = null;
      valuesChecksumOut = null;

      storedKeyBytesWritten = writeBufferedSection(keySectionBuffer,
          compressOutput ? valuesCompressor : null);
      storedLengthBytesWritten = writeBufferedSection(lengthsSectionBuffer,
          compressOutput ? valuesCompressor : null);

      // Close the underlying stream iff we own it
      if (ownOutputStream) {
        rawOut.close();
      }

      if (compressOutput) {
        // Return back the compressor
        // if compressorExternal != null, this Writer does not own compressor, so do not return it to CodecPool
        if (compressorExternal == null) {
          // this Writer owns compressor
          CodecPool.returnCompressor(valuesCompressor);
        }
        valuesCompressor = null;
      }

      if (writtenRecordsCounter != null) {
        writtenRecordsCounter.increment(totalNumRecordsWritten);
      }
    }

    public void append(Object key, Object value) throws IOException {
      keySerializer.serialize(key);
      int keyLength = buffer.getLength();
      assert(keyLength >= 0);

      valueSerializer.serialize(value);
      int valueLength = buffer.getLength() - keyLength;
      assert(valueLength >= 0);
      writeKVPair(buffer.getData(), 0, keyLength, buffer.getData(),
          keyLength, valueLength);

      buffer.reset();
      ++totalNumRecordsWritten;
    }

    public void append(DataInputBuffer key, DataInputBuffer value) throws IOException {
      int keyLength = key.getLength() - key.getPosition();
      assert(keyLength >= 0);

      int valueLength = value.getLength() - value.getPosition();
      assert(valueLength >= 0);
      writeKVPair(key.getData(), key.getPosition(), keyLength,
          value.getData(), value.getPosition(), valueLength);
      ++totalNumRecordsWritten;
    }

    protected void writeKVPair(byte[] keyData, int keyPos, int keyLength,
        byte[] valueData, int valPos, int valueLength) throws IOException {
      if (keyLength < 0 || valueLength < 0) {
        throw new IOException("Negative key/value lengths are not allowed. keyLength=" + keyLength
            + ", valueLength=" + valueLength);
      }

      keySectionBuffer.write(keyData, keyPos, keyLength);
      lengthsSectionBuffer.writeInt(keyLength);
      lengthsSectionBuffer.writeInt(valueLength);
      bufferWriteBytes(valueData, valPos, valueLength);

      keysRawLength += keyLength;
      valuesRawLength += valueLength;
      lengthsRawLength += 2L * INT_SIZE;

      if (serializedUncompressedBytes != null) {
        serializedUncompressedBytes.increment(keyLength + valueLength);
      }
    }

    protected void bufferWriteInt(int val) throws IOException {
      final int len = 4;
      final int remaining = writeBufferLength - writeOffset;
      if (len > remaining) {
        flushWriteBuffer();
      }
      writeByteBuffer.putInt(writeOffset, val);
      writeOffset += len;
    }

    protected void bufferWriteLong(long val) throws IOException {
      final int len = 8;
      final int remaining = writeBufferLength - writeOffset;
      if (len > remaining) {
        flushWriteBuffer();
      }
      writeByteBuffer.putLong(writeOffset, val);
      writeOffset += len;
    }

    protected void bufferWriteBytes(byte[] data, int off, int len) throws IOException {
      final int remaining = writeBufferLength - writeOffset;
      if (len > remaining) {
        flushWriteBuffer();
        if (len >= writeBufferLength) {
          valuesOut.write(data, off, len);
          return;
        }
      }
      System.arraycopy(data, off, writeBuffer, writeOffset, len);
      writeOffset += len;
    }

    protected void flushWriteBuffer() throws IOException {
      if (writeOffset > 0) {
        valuesOut.write(writeBuffer, 0, writeOffset);
        writeOffset = 0;
      }
    }

    protected void finishValuesSection() throws IOException {
      flushWriteBuffer();
      if (compressOutput) {
        valuesCompressedOut.finish();
        valuesCompressedOut.resetState();
      }
      valuesChecksumOut.finish();
    }

    private long writeBufferedSection(DataOutputBuffer sectionBuffer, @Nullable Compressor compressor)
        throws IOException {
      assert (compressOutput && compressor != null) || (!compressOutput && compressor == null);

      final long sectionStart = rawOut.getPos();

      IFileOutputStream sectionChecksumOut = new IFileOutputStream(rawOut);
      DataOutputStream sectionOut = new DataOutputStream(sectionChecksumOut);
      CompressionOutputStream sectionCompressedOut = null;

      if (compressor != null) {
        compressor.reset();
        sectionCompressedOut = CodecUtils.createOutputStream(codec, sectionChecksumOut, compressor);
        sectionOut = new DataOutputStream(sectionCompressedOut);
      }

      sectionOut.write(sectionBuffer.getData(), 0, sectionBuffer.getLength());
      sectionOut.flush();
      if (sectionCompressedOut != null) {
        sectionCompressedOut.finish();
        sectionCompressedOut.resetState();
      }
      sectionChecksumOut.finish();

      return rawOut.getPos() - sectionStart - checksumSize;
    }

    // Cf. corresponds to TezIndexRecord.rawLength
    public long getRawLength() {
      return HEADER.length
        + keysRawLength + valuesRawLength + lengthsRawLength
        + 3L * checksumSize;
    }

    // Cf. corresponds to TezIndexRecord.partLength (which factors in checksums and compression)
    public long getCompressedLength() {
      return HEADER.length
        + storedValueBytesWritten + storedKeyBytesWritten + storedLengthBytesWritten
        + 3L * checksumSize;
    }

    // should be called after close()
    public SectionLayout getSectionLayout() {
      assert valuesOut == null;

      long valuesStart = HEADER.length;
      long keysStart = valuesStart + storedValueBytesWritten + checksumSize;
      long lengthsStart = keysStart + storedKeyBytesWritten + checksumSize;
      long totalLength = lengthsStart + storedLengthBytesWritten + checksumSize;

      return new SectionLayout(
          valuesStart, keysStart, lengthsStart,
          totalLength,
          totalNumRecordsWritten,
          valuesRawLength, keysRawLength, lengthsRawLength);
    }
  }

  public enum KeyState {NO_KEY, NEW_KEY}

  public interface ReaderRead {
    // for reporting progress in consuming payload
    long getPosition() throws IOException;

    // reporting the payload length, i.e., getPayloadLength()
    long getLength();

    KeyState readRawKey(DataInputBuffer key) throws IOException;
    boolean nextRawKey(DataInputBuffer key) throws IOException;
    void nextRawValue(DataInputBuffer value) throws IOException;
    void close() throws IOException;
  }

  /**
   * <code>IFile.Reader</code> to read intermediate map-outputs.
   */
  public static class Reader implements ReaderRead {

    // The maximum array size is a little less than the max integer value.
    // Trying to create a larger array will result in an OOM exception.
    // The exact value is JVM dependent so setting it to max int - 8 to be safe.
    private static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;

    private final SectionLayout layout;
    private final CompressionCodec codec;
    private final TezCounter readRecordsCounter;
    private final TezCounter bytesReadCounter;
    private final DecompressorPool taskContext;

    private final boolean isCompressed;

    private IFileInputStream valuesChecksumIn;
    private IFileInputStream keysChecksumIn;
    private IFileInputStream lengthsChecksumIn;

    private Decompressor valuesDecompressor;
    private Decompressor keysDecompressor;
    private Decompressor lengthsDecompressor;

    private final InputStream valuesIn;
    private final InputStream keysIn;
    private final InputStream lengthsIn;

    private DataInputStream valuesDataIn = null;
    private DataInputStream keysDataIn = null;
    private DataInputStream lengthsDataIn = null;

    private long valuesStartPos;
    private long keysStartPos;
    private long lengthsStartPos;

    private byte keyBytes[] = new byte[0];

    public long bytesRead = 0;      // TODO: rename to rawBytesRead; convert to an accessor method
    private long numRecordsRead = 0;
    private boolean isEof = false;  // set to true when numRecordsRead == totalNumRecordsWritten

    private int currentKeyLength;
    private int currentValueLength;

    /**
     * Construct an IFile Reader.
     *
     * @param codec codec
     * @param readsCounter Counter for records read from disk
     * @throws IOException
     */
    public Reader(InputStream headerValuesIn,
                  InputStream keysIn,
                  InputStream lengthsIn,
                  SectionLayout layout,
                  CompressionCodec codec,
                  TezCounter readsCounter, TezCounter bytesReadCounter,
                  boolean readAhead, int readAheadLength,
                  DecompressorPool taskContext) throws IOException {
      assert headerValuesIn != null && keysIn != null && lengthsIn != null;

      this.layout = layout;
      this.codec = codec;
      this.readRecordsCounter = readsCounter;
      this.bytesReadCounter = bytesReadCounter;
      this.taskContext = taskContext;

      this.isCompressed = isCompressedFlagEnabled(headerValuesIn);

      long valuesChecksumLength = layout.valuesLength + checksumSize;
      long keysChecksumLength = layout.keysLength + checksumSize;
      long lengthsChecksumLength = layout.lengthsLength + checksumSize;
      this.valuesChecksumIn = new IFileInputStream(headerValuesIn, valuesChecksumLength, readAhead, readAheadLength);
      this.keysChecksumIn = new IFileInputStream(keysIn, keysChecksumLength, readAhead, readAheadLength);
      this.lengthsChecksumIn = new IFileInputStream(lengthsIn, lengthsChecksumLength, readAhead, readAheadLength);

      if (isCompressed) {
        if (codec == null) {
          throw new IOException("IFile is compressed but no codec was provided");
        }
        assert taskContext != null;

        this.valuesDecompressor = taskContext.getDecompressor(codec);
        this.keysDecompressor = taskContext.getDecompressor(codec);
        this.lengthsDecompressor = taskContext.getDecompressor(codec);

        if (this.valuesDecompressor == null || this.keysDecompressor == null || this.lengthsDecompressor == null) {
          throw new IOException("Could not obtain decompressor from CodecPool for values section");
        }

        this.valuesIn = CodecUtils.createInputStream(codec, valuesChecksumIn, valuesDecompressor);
        this.keysIn = CodecUtils.createInputStream(codec, keysChecksumIn, keysDecompressor);
        this.lengthsIn = CodecUtils.createInputStream(codec, lengthsChecksumIn, lengthsDecompressor);
      } else {
        this.valuesIn = valuesChecksumIn;
        this.keysIn = keysChecksumIn;
        this.lengthsIn = lengthsChecksumIn;
      }

      this.valuesDataIn = new DataInputStream(this.valuesIn);
      this.keysDataIn = new DataInputStream(this.keysIn);
      this.lengthsDataIn = new DataInputStream(this.lengthsIn);

      this.valuesStartPos = valuesChecksumIn.getPosition();
      this.keysStartPos = keysChecksumIn.getPosition();
      this.lengthsStartPos = lengthsChecksumIn.getPosition();

      if (bytesReadCounter != null) {
        bytesReadCounter.increment(IFile.HEADER.length);
      }
    }

    private static boolean isCompressedFlagEnabled(InputStream in) throws IOException {
      byte[] header = new byte[HEADER.length];
      IOUtils.readFully(in, header, 0, HEADER.length);
      verifyHeaderMagic(header);
      return (header[3] == 1);
    }

    /**
     * Read the entire IFile contents to memory (byte[] array).
     * If the IFile header indicates compression, decompress all the three sections.
     */
    public static SectionLayout readToMemory(byte[] buffer, InputStream in, SectionLayout layout,
        CompressionCodec codec, boolean ifileReadAhead, int ifileReadAheadLength,
        TaskContext taskContext, boolean useThreadLocalDecompressor)
        throws IOException {
      assert buffer.length >= layout.totalRawLength;
      assert layout.isValidForByteArray();

      byte[] header = new byte[HEADER.length];
      IOUtils.readFully(in, header, 0, HEADER.length);
      verifyHeaderMagic(header);
      final boolean isCompressed = (header[3] == 1);

      final int valuesLength = (int)layout.valuesLength;
      final int keysLength = (int)layout.keysLength;
      final int lengthsLength = (int)layout.lengthsLength;

      final int valuesRawLength = (int)layout.valuesRawLength;
      final int keysRawLength = (int)layout.keysRawLength;
      final int lengthsRawLength = (int)layout.lengthsRawLength;

      if (!isCompressed) {
        System.arraycopy(header, 0, buffer, 0, HEADER.length);
        int offset = HEADER.length;
        offset = readStoredSectionToBuffer(buffer, offset, in, valuesLength + checksumSize,
            ifileReadAhead, ifileReadAheadLength);
        offset = readStoredSectionToBuffer(buffer, offset, in, keysLength + checksumSize,
            ifileReadAhead, ifileReadAheadLength);
        readStoredSectionToBuffer(buffer, offset, in, lengthsLength + checksumSize,
            ifileReadAhead, ifileReadAheadLength);
        return null;
      }

      if (codec == null) {
        throw new IOException("IFile is compressed but no codec was provided");
      }

      // Buffer stores decompressed payloads; header must indicate uncompressed data.
      System.arraycopy(HEADER, 0, buffer, 0, HEADER.length);

      int valuesOffset = HEADER.length;
      int keysOffset = valuesOffset + valuesRawLength + checksumSize;
      int lengthsOffset = keysOffset + keysRawLength + checksumSize;
      if (lengthsOffset + lengthsRawLength + checksumSize > buffer.length) {
        throw new IOException("Insufficient destination buffer for decompressed IFile");
      }

      Decompressor decompressor = getDecompressor(codec, taskContext, useThreadLocalDecompressor);

      try {
        readSectionToMemory(buffer, valuesOffset,
            in, valuesLength + checksumSize, valuesRawLength,
            true, codec, ifileReadAhead, ifileReadAheadLength,
            decompressor);
        IFileOutputStream.writeChecksumTrailer(buffer, valuesOffset, valuesRawLength,
            buffer, valuesOffset + valuesRawLength);

        readSectionToMemory(buffer, keysOffset,
            in, keysLength + checksumSize, keysRawLength,
            true, codec, ifileReadAhead, ifileReadAheadLength,
            decompressor);
        IFileOutputStream.writeChecksumTrailer(buffer, keysOffset, keysRawLength,
            buffer, keysOffset + keysRawLength);

        readSectionToMemory(buffer, lengthsOffset,
            in, lengthsLength + checksumSize, lengthsRawLength,
            true, codec, ifileReadAhead, ifileReadAheadLength,
            decompressor);
        IFileOutputStream.writeChecksumTrailer(buffer, lengthsOffset, lengthsRawLength,
            buffer, lengthsOffset + lengthsRawLength);
      } finally {
        returnDecompressor(codec, taskContext, useThreadLocalDecompressor, decompressor);
      }

      SectionLayout decompressedLayout = new SectionLayout(
          HEADER.length,
          HEADER.length + valuesRawLength + checksumSize,
          HEADER.length + valuesRawLength + checksumSize + keysRawLength + checksumSize,
          layout.totalRawLength,
          layout.totalNumRecordsWritten,
          valuesRawLength, keysRawLength, lengthsRawLength);
      return decompressedLayout;
    }

    private static void readSectionToMemory(byte[] buffer, int offset,
        InputStream in, int sectionChecksumLength,
        int sectionRawLength,
        boolean isCompressed, CompressionCodec codec, boolean ifileReadAhead, int ifileReadAheadLength,
        Decompressor decompressor)
        throws IOException {
      IFileInputStream checksumIn = new IFileInputStream(
          in, sectionChecksumLength, ifileReadAhead, ifileReadAheadLength);
      in = checksumIn;
      if (isCompressed && codec != null) {
        if (decompressor != null) {
          decompressor.reset();
          in = CodecUtils.getDecompressedInputStreamWithBufferSize(codec, checksumIn, decompressor,
              sectionChecksumLength);
        } else {
          throw new IOException("Could not obtain decompressor from CodecPool");
        }
      }
      try {
        IOUtils.readFully(in, buffer, offset, sectionRawLength);
        /*
         * We've gotten the amount of data we were expecting. Verify the
         * decompressor has nothing more to offer. This action also forces the
         * decompressor to read any trailing bytes that weren't critical for
         * decompression, which is necessary to keep the stream in sync.
         */
        if (in.read() >= 0) {
          throw new IOException("Unexpected extra bytes from input stream");
        }
      } catch (IOException ioe) {
        if(in != null) {
          try {
            in.close();
          } catch(IOException e) {
            if(isDebugEnabled) {
              LOG.debug("Exception in closing " + in, e);
            }
          }
        }
        throw ioe;
      }
    }

    private static Decompressor getDecompressor(CompressionCodec codec, TaskContext taskContext,
        boolean useThreadLocalDecompressor) throws IOException {
      Decompressor decompressor;
      if (useThreadLocalDecompressor) {
        decompressor = decompressorHolder.get();
        if (decompressor == null) {
          assert taskContext != null;
          decompressor = taskContext.getDecompressor(codec);
          decompressorHolder.set(decompressor);
        }
      } else {
        assert taskContext != null;
        decompressor = taskContext.getDecompressor(codec);
      }
      if (decompressor == null) {
        throw new IOException("Could not obtain decompressor from CodecPool");
      }
      return decompressor;
    }

    private static void returnDecompressor(CompressionCodec codec, TaskContext taskContext,
        boolean useThreadLocalDecompressor, Decompressor decompressor) {
      if (decompressor == null) {
        return;
      }
      decompressor.reset();
      // if useThreadLocalDecompressor == true, never return decompressor which will be garbage-collected
      if (!useThreadLocalDecompressor) {
        if (taskContext != null) {
          taskContext.returnDecompressor(codec.getCompressorType(), decompressor);
        } else {
          CodecPool.returnDecompressor(decompressor);
        }
      }
    }

    private static int readStoredSectionToBuffer(byte[] buffer, int offset, InputStream in,
        int sectionChecksumLength, boolean ifileReadAhead, int ifileReadAheadLength) throws IOException {
      IFileInputStream checksumIn = new IFileInputStream(in, sectionChecksumLength,
          ifileReadAhead, ifileReadAheadLength);
      int bytesRead = 0;
      while (bytesRead < sectionChecksumLength) {
        int n = checksumIn.readWithChecksum(buffer, offset + bytesRead,
            sectionChecksumLength - bytesRead);
        if (n < 0) {
          throw new IOException("read past end of stream");
        }
        bytesRead += n;
      }
      return offset + sectionChecksumLength;
    }

    /**
     * Read the entire IFile contents to disk.
     * Do not decompress even if the IFile header indicates compression, i.e., preserve on-wire representation.
     * Hence, the input SectionLayout can be used for the data written to disk as well.
     */
    public static long readToDisk(OutputStream out,
        InputStream in, SectionLayout layout,
        boolean ifileReadAhead, int ifileReadAheadLength)
        throws IOException {
      final long length = layout.totalLength;
      final int BYTES_TO_READ = 64 * 1024;
      byte[] buf = new byte[BYTES_TO_READ];

      byte[] header = new byte[HEADER.length];
      IOUtils.readFully(in, header, 0, HEADER.length);
      verifyHeaderMagic(header);
      out.write(header, 0, HEADER.length);

      long bytesWritten = HEADER.length;
      bytesWritten += copyStoredSectionToDisk(out, in, buf, layout.valuesLength + checksumSize,
          ifileReadAhead, ifileReadAheadLength);
      bytesWritten += copyStoredSectionToDisk(out, in, buf, layout.keysLength + checksumSize,
          ifileReadAhead, ifileReadAheadLength);
      bytesWritten += copyStoredSectionToDisk(out, in, buf, layout.lengthsLength + checksumSize,
          ifileReadAhead, ifileReadAheadLength);

      if (bytesWritten != length) {
        throw new IOException("IFile layout length mismatch while reading to disk. expected="
            + length + ", actual=" + bytesWritten);
      }
      return bytesWritten;
    }

    private static long copyStoredSectionToDisk(OutputStream out, InputStream in, byte[] buffer,
        long sectionChecksumLength, boolean ifileReadAhead, int ifileReadAheadLength)
        throws IOException {
      @SuppressWarnings("resource")
      IFileInputStream checksumIn = new IFileInputStream(in, sectionChecksumLength,
          ifileReadAhead, ifileReadAheadLength);
      long bytesLeft = sectionChecksumLength;
      while (bytesLeft > 0) {
        int n = checksumIn.readWithChecksum(buffer, 0, (int) Math.min(bytesLeft, buffer.length));
        if (n < 0) {
          throw new IOException("read past end of stream");
        }
        out.write(buffer, 0, n);
        bytesLeft -= n;
      }
      return sectionChecksumLength;
    }

    public long getLength() {
      if (valuesChecksumIn == null) {   // close() already called
        return 0;
      }
      return layout.payloadLength();
    }

    // for reporting progress
    public long getPosition() throws IOException {
      if (valuesChecksumIn == null) {   // close() already called
        return 0;
      }
      return (valuesChecksumIn.getPosition() - valuesStartPos)
          + (keysChecksumIn.getPosition() - keysStartPos)
          + (lengthsChecksumIn.getPosition() - lengthsStartPos);
    }

    /**
     * Read up to len bytes into buf starting at offset off.
     *
     * @param buf buffer
     * @param len length of buffer
     * @return the no. of bytes read
     * @throws IOException
     */
    private int readData(InputStream sectionIn, byte[] buf, int len) throws IOException {
      int bytesRead = 0;
      while (bytesRead < len) {
        int n = IOUtils.wrappedReadForCompressedData(sectionIn, buf, bytesRead, len - bytesRead);
        if (n < 0) {
          return bytesRead;
        }
        bytesRead += n;
      }
      return len;
    }

    private void readKeyValueLength() throws IOException {
      currentKeyLength = lengthsDataIn.readInt();
      currentValueLength = lengthsDataIn.readInt();
      bytesRead += INT_SIZE + INT_SIZE;
    }

    /**
     * Reset key length and value length for next record in the file
     *
     * @return true if key length and value length were set to the next
     *         false if end of file (EOF) marker was reached
     * @throws IOException
     */
    private boolean positionToNextRecord() throws IOException {
      if (numRecordsRead >= layout.totalNumRecordsWritten) {
        isEof = true;
        return false;
      }

      readKeyValueLength();

      if (currentKeyLength < 0) {
        throw new IOException("Negative key-length: " + currentKeyLength);
      }
      if (currentValueLength < 0) {
        throw new IOException("Negative value-length: " + currentValueLength);
      }
      return true;
    }

    public final boolean nextRawKey(DataInputBuffer key) throws IOException {
      return readRawKey(key) != KeyState.NO_KEY;
    }

    private static byte[] createLargerArray(int currentLength) {
      if (currentLength > MAX_BUFFER_SIZE) {
        throw new IllegalArgumentException(String.format(
            REQ_BUFFER_SIZE_TOO_LARGE, currentLength, MAX_BUFFER_SIZE));
      }
      int newLength;
      if (currentLength > (MAX_BUFFER_SIZE - currentLength)) {
        // possible overflow: if (2 * currentLength > MAX_BUFFER_SIZE)
        newLength = currentLength;
      } else {
        newLength = currentLength << 1;
      }
      return new byte[newLength];
    }

    public KeyState readRawKey(DataInputBuffer key) throws IOException {
      if (!positionToNextRecord()) {
        if (isDebugEnabled) {
          LOG.debug("currentKeyLength=" + currentKeyLength +
              ", currentValueLength=" + currentValueLength +
              ", bytesRead=" + bytesRead +
              ", totalLength=" + layout.totalLength);
        }
        return KeyState.NO_KEY;
      }
      if (keyBytes.length < currentKeyLength) {
        keyBytes = createLargerArray(currentKeyLength);
      }
      int i = readData(keysIn, keyBytes, currentKeyLength);
      if (i != currentKeyLength) {
        throw new IOException(String.format(INCOMPLETE_READ, currentKeyLength, i));
      }
      key.reset(keyBytes, currentKeyLength);
      bytesRead += currentKeyLength;
      return KeyState.NEW_KEY;
    }

    public void nextRawValue(DataInputBuffer value) throws IOException {
      assert value.getData() != keyBytes;   // if true, we should not use value.getData()

      final byte[] valBytes;
      if (value.getData().length < currentValueLength) {
        valBytes = createLargerArray(currentValueLength);
      } else {
        valBytes = value.getData();
      }

      int i = readData(valuesIn, valBytes, currentValueLength);
      if (i != currentValueLength) {
        throw new IOException(String.format(INCOMPLETE_READ, currentValueLength, i));
      }
      value.reset(valBytes, currentValueLength);

      bytesRead += currentValueLength;
      ++numRecordsRead;

      if (numRecordsRead == layout.totalNumRecordsWritten) {
        isEof = true;
      }
    }

    private static void verifyHeaderMagic(byte[] header) throws IOException {
      if (!(header[0] == 'T' && header[1] == 'I' && header[2] == 'F')) {
        throw new IOException("Not a valid ifile header");
      }
    }

    public void close() throws IOException {
      IOException thrown = null;

      if (readRecordsCounter != null) {
        readRecordsCounter.increment(numRecordsRead);
      }

      if (bytesReadCounter != null && valuesChecksumIn != null) {
        bytesReadCounter.increment(getPosition() + (3L * checksumSize));
      }

      // Release section readers
      valuesDataIn = null;
      keysDataIn = null;
      lengthsDataIn = null;

      try { closeSection(valuesIn); } catch (IOException e) { thrown = e; }
      try { closeSection(keysIn); } catch (IOException e) { thrown = e; }
      try { closeSection(lengthsIn); } catch (IOException e) { thrown = e; }

      // Return decompressors (all-or-none when compressed)
      if (isCompressed) {
        assert keysDecompressor != null && valuesDecompressor != null && lengthsDecompressor != null;
        assert taskContext != null;

        valuesDecompressor.reset();
        keysDecompressor.reset();
        lengthsDecompressor.reset();
        taskContext.returnDecompressor(codec.getCompressorType(), valuesDecompressor);
        taskContext.returnDecompressor(codec.getCompressorType(), keysDecompressor);
        taskContext.returnDecompressor(codec.getCompressorType(), lengthsDecompressor);

        valuesDecompressor = null;
        keysDecompressor = null;
        lengthsDecompressor = null;
      }

      valuesChecksumIn = null;
      keysChecksumIn = null;
      lengthsChecksumIn = null;

      if (thrown != null) {
        throw thrown;
      }
    }

    private void closeSection(InputStream sectionIn) throws IOException {
      if (sectionIn != null) {
        sectionIn.close();
      }
    }
  }
}
