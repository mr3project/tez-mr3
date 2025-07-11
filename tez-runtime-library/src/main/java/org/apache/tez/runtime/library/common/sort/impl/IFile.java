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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.apache.tez.runtime.library.utils.BufferUtils;
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

  public static final int EOF_MARKER = -1; // End of File Marker
  public static final int RLE_MARKER = -2; // Repeat same key marker
  public static final int V_END_MARKER = -3; // End of values marker

  public static final DataInputBuffer REPEAT_KEY = new DataInputBuffer();
  static final byte[] HEADER = new byte[] { (byte) 'T', (byte) 'I',
    (byte) 'F' , (byte) 0};

  private static final String INCOMPLETE_READ = "Requested to read %d got %d";
  private static final String REQ_BUFFER_SIZE_TOO_LARGE = "Size of data %d is greater than the max allowed of %d";

  private static final ThreadLocal<Decompressor> decompressorHolder = new ThreadLocal<>();

  private static final int INT_SIZE = 4;

  // Cf. see TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_MAX_SIZE
  public static final int WRITER_BUFFER_SIZE_DEFAULT = 4 * 1024;

  public static byte[] allocateWriteBuffer() {
    return new byte[WRITER_BUFFER_SIZE_DEFAULT];
  }

  public static byte[] allocateWriteBufferSingle() {
    return new byte[8 * 2];   // TODO: 8 bytes is enough
  }

  public interface WriterAppend {
    public void append(DataInputBuffer key, DataInputBuffer value) throws IOException;
    public void close() throws IOException;
  }

  private static final int checksumSize = IFileOutputStream.getCheckSumSize();

  /**
   * For basic cache size checks: header + checksum + EOF marker
   *
   * @return size of the base cache needed
   */
  static int getBaseCacheSize() {
    return (HEADER.length + checksumSize + (2 * INT_SIZE));
  }

  /**
   * IFileWriter which stores data in memory for specified limit, beyond
   * which it falls back to file based writer. It creates files lazily on
   * need basis and avoids any disk hit (in cases, where data fits entirely in mem).
   * <p>
   * This class should not make any changes to IFile logic and should just flip streams
   * from mem to disk on need basis.
   *
   * During write, it verifies whether uncompressed payload can fit in memory. If so, it would
   * store in buffer. Otherwise, it falls back to file based writer. Note that data stored
   * internally would be in compressed format (if codec is provided). However, for easier
   * comparison and spill over, uncompressed payload check is done. This is
   * done intentionally, as it is not possible to know compressed data length
   * upfront.
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
     * Note that we do not allow compression in in-mem stream.
     * When spilled over to file, compression gets enabled.
     *
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
      this.totalSize = getBaseCacheSize();
      this.fileCodec = codec;
    }

    boolean shouldWriteToDisk() {
      return totalSize >= cacheStream.getLimit();
    }

    /**
     * Create in mem stream. In it is too small, adjust it's size
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
     * 1. Content format: HEADER + real data + CHECKSUM. Checksum is for real
     * data.
     * 2. Before flipping, close checksum stream, so that checksum is written
     * out.
     * 3. Create relevant file based writer.
     * 4. Write header and then real data.
     *
     * @throws IOException
     */
    private void resetToFileBasedWriter() throws IOException {
      // Close out stream, so that data checksums are written.
      // Buf contents = HEADER + (uncompressed) real data + CHECKSUM
      flushWriteBuffer();
      this.out.close();

      // Get the buffer which contains data in memory
      BoundedByteArrayOutputStream bout =
          (BoundedByteArrayOutputStream) this.rawOut.getWrappedStream();

      // Create new file based writer
      if (outputPath == null) {
        outputPath = taskOutput.getOutputFileForWrite();
      }
      LOG.info("Switching from mem stream to disk stream. File: " + outputPath);
      FSDataOutputStream newRawOut = fs.create(outputPath);
      this.rawOut = newRawOut;
      this.ownOutputStream = true;  // because of fs.create(outputPath)

      setupOutputStream(fileCodec);

      // Write header to file
      headerWritten = false;
      writeHeader(newRawOut);

      // write real data
      int sPos = HEADER.length;
      int len = (bout.size() - checksumSize - HEADER.length);
      bufferWriteBytes(bout.getBuffer(), sPos, len);

      bufferFull = true;
      bout.reset();
    }

    @Override
    protected void writeKVPair(byte[] keyData, int keyPos, int keyLength,
        byte[] valueData, int valPos, int valueLength) throws IOException {
      if (!bufferFull) {
        // Compute actual payload size: write RLE marker, length info and then entire data.
        totalSize += ((prevKey == REPEAT_KEY) ? V_END_MARKER_SIZE : 0)
            + INT_SIZE + keyLength
            + INT_SIZE + valueLength;

        if (shouldWriteToDisk()) {
          resetToFileBasedWriter();
        }
      }
      super.writeKVPair(keyData, keyPos, keyLength, valueData, valPos, valueLength);
    }

    @Override
    protected void writeValue(byte[] data, int offset, int length) throws IOException {
      if (!bufferFull) {
        totalSize += ((prevKey != REPEAT_KEY) ? RLE_MARKER_SIZE : 0) + INT_SIZE + length;

        if (shouldWriteToDisk()) {
          resetToFileBasedWriter();
        }
      }
      super.writeValue(data, offset, length);
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
      if (!isDataFlushedToDisk()) {
        return ByteBuffer.wrap(cacheStream.getBuffer(), 0, cacheStream.size());
      }
      return null;
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
    // DataOutput: rawOut <-- checksumOut <-- compressedOut <-- out <-- [writeBuffer]

    protected DataOutputStream out;
    private long start = 0;

    private CompressionOutputStream compressedOut;
    private Compressor compressor;
    private boolean compressOutput = false;

    private IFileOutputStream checksumOut;

    protected FSDataOutputStream rawOut;
    // true iff this Writer created and owns rawOut
    // if true, close() closes rawOut.
    protected boolean ownOutputStream = false;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private long decompressedBytesWritten = 0;
    private long compressedBytesWritten = 0;

    // Count records written to disk
    private long numRecordsWritten = 0;
    private long rleWritten = 0; //number of RLE markers written
    private long totalKeySaving = 0; //number of keys saved due to multi KV writes + RLE
    private final TezCounter writtenRecordsCounter;
    private final TezCounter serializedUncompressedBytes;

    private boolean closeSerializers = false;
    private Serializer keySerializer = null;
    private Serializer valueSerializer = null;

    private final DataOutputBuffer buffer = new DataOutputBuffer();
    private final DataOutputBuffer previous = new DataOutputBuffer();
    protected Object prevKey = null;
    protected boolean headerWritten = false;

    final int RLE_MARKER_SIZE = INT_SIZE;
    final int V_END_MARKER_SIZE = INT_SIZE;

    // de-dup keys or not
    protected final boolean rle;

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
                  boolean rle,
                  byte[] writeBuffer,
                  @Nullable Compressor compressorExternal) throws IOException {
      this.rawOut = outputStream;
      this.writtenRecordsCounter = writesCounter;
      this.serializedUncompressedBytes = serializedBytesCounter;
      this.start = this.rawOut.getPos();
      this.rle = rle;

      this.writeBuffer = writeBuffer;
      this.writeBufferLength = writeBuffer.length;
      this.writeByteBuffer = ByteBuffer.wrap(writeBuffer).order(ByteOrder.BIG_ENDIAN);
      this.writeOffset = 0;

      this.compressorExternal = compressorExternal;

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
      this.checksumOut = new IFileOutputStream(this.rawOut);
      if (codec != null) {
        if (compressorExternal != null) {
          this.compressor = compressorExternal;
        } else {
          this.compressor = CodecUtils.getCompressor(codec);
        }
        if (this.compressor != null) {
          this.compressor.reset();
          this.compressedOut = CodecUtils.createOutputStream(codec, checksumOut, compressor);
          this.out = new DataOutputStream(this.compressedOut);
          this.compressOutput = true;
        } else {
          LOG.warn("Could not obtain compressor from CodecPool");
          this.out = new DataOutputStream(checksumOut);
        }
      } else {
        this.out = new DataOutputStream(checksumOut);
      }
    }

    protected void writeHeader(OutputStream outputStream) throws IOException {
      if (!headerWritten) {
        outputStream.write(HEADER, 0, HEADER.length - 1);
        outputStream.write((compressOutput) ? (byte) 1 : (byte) 0);
        headerWritten = true;
      }
    }

    public void close() throws IOException {
      if (closed.getAndSet(true)) {
        throw new IOException("Writer was already closed earlier");
      }

      // When IFile writer is created by BackupStore, we do not have
      // Key and Value classes set. So, check before closing the
      // serializers
      if (closeSerializers) {
        keySerializer.close();
        valueSerializer.close();
      }

      // write V_END_MARKER as needed
      writeValueMarker();

      // Write EOF_MARKER for key/value length
      // bufferWriteInt(EOF_MARKER);
      // bufferWriteInt(EOF_MARKER);
      long combined = ((long) EOF_MARKER << 32) | (EOF_MARKER & 0xFFFFFFFFL);
      bufferWriteLong(combined);

      decompressedBytesWritten += 2 * INT_SIZE;
      //account for header bytes
      decompressedBytesWritten += HEADER.length;

      flushWriteBuffer();   // Ensure all buffered data is written to 'out'

      // Close the underlying stream iff we own it
      if (ownOutputStream) {
        out.close();
      } else {
        if (compressOutput) {
          // Flush
          compressedOut.finish();
          compressedOut.resetState();
        }
        // Write the checksum and flush the buffer
        checksumOut.finish();
      }
      //header bytes are already included in rawOut
      compressedBytesWritten = rawOut.getPos() - start;

      if (compressOutput) {
        // Return back the compressor
        // if compressorExternal != null, this Writer does not own compressor, so do not return it to CodecPool
        if (compressorExternal == null) {
          // this Writer owns compressor
          CodecPool.returnCompressor(compressor);
        }
        compressor = null;
      }

      out = null;
      if (writtenRecordsCounter != null) {
        writtenRecordsCounter.increment(numRecordsWritten);
      }
      if (isDebugEnabled) {
        LOG.debug("Total keys written=" + numRecordsWritten + "; rleEnabled=" + rle + "; Savings" +
            "(due to multi-kv/rle)=" + totalKeySaving + "; number of RLEs written=" +
            rleWritten + "; compressedLen=" + compressedBytesWritten + "; rawLen="
            + decompressedBytesWritten);
      }
    }

    /**
     * Send key/value to be appended to IFile. To represent same key as previous
     * one, send IFile.REPEAT_KEY as key parameter.  Should not call this method with
     * IFile.REPEAT_KEY as the first key. It is caller's responsibility to ensure that correct
     * key/value type checks and key/value length (non-negative) checks are done properly.
     *
     * @param key
     * @param value
     * @throws IOException
     */
    public void append(Object key, Object value) throws IOException {
      int keyLength = 0;
      boolean sameKey = (key == REPEAT_KEY);
      if (!sameKey) {
        keySerializer.serialize(key);
        keyLength = buffer.getLength();
        assert(keyLength >= 0);
        if (rle && (keyLength == previous.getLength())) {
          sameKey = BufferUtils.compareEqual(previous, buffer);
        }
      }

      // Append the 'value'
      valueSerializer.serialize(value);
      int valueLength = buffer.getLength() - keyLength;
      assert(valueLength >= 0);
      if (!sameKey) {
        //dump entire key value pair
        writeKVPair(buffer.getData(), 0, keyLength, buffer.getData(),
            keyLength, buffer.getLength() - keyLength);
        if (rle) {
          previous.reset();
          previous.write(buffer.getData(), 0, keyLength); //store the key
        }
      } else {
        writeValue(buffer.getData(), keyLength, valueLength);
      }

      prevKey = sameKey ? REPEAT_KEY : key;
      // Reset
      buffer.reset();
      ++numRecordsWritten;
    }

    /**
     * Send key/value to be appended to IFile. To represent same key as previous
     * one, send IFile.REPEAT_KEY as key parameter.  Should not call this method with
     * IFile.REPEAT_KEY as the first key. It is caller's responsibility to pass non-negative
     * key/value lengths. Otherwise,IndexOutOfBoundsException could be thrown at runtime.
     *
     *
     * @param key
     * @param value
     * @throws IOException
     */
    public void append(DataInputBuffer key, DataInputBuffer value) throws IOException {
      int keyLength = key.getLength() - key.getPosition();
      assert(key == REPEAT_KEY || keyLength >=0);

      int valueLength = value.getLength() - value.getPosition();
      assert(valueLength >= 0);

      boolean sameKey = (key == REPEAT_KEY);
      if (!sameKey && rle) {
        sameKey = (keyLength != 0) && BufferUtils.compareEqual(previous, key);
      }

      if (!sameKey) {
        writeKVPair(key.getData(), key.getPosition(), keyLength,
            value.getData(), value.getPosition(), valueLength);
        if (rle) {
          BufferUtils.copy(key, previous);
        }
      } else {
        writeValue(value.getData(), value.getPosition(), valueLength);
      }
      prevKey = sameKey ? REPEAT_KEY : key;
      ++numRecordsWritten;
    }

    protected void writeValue(byte[] data, int offset, int length) throws IOException {
      writeRLE();
      bufferWriteInt(length); // value length
      bufferWriteBytes(data, offset, length);
      // Update bytes written
      decompressedBytesWritten += length + INT_SIZE;
      if (serializedUncompressedBytes != null) {
        serializedUncompressedBytes.increment(length);
      }
      totalKeySaving++;
    }

    protected void writeKVPair(byte[] keyData, int keyPos, int keyLength,
        byte[] valueData, int valPos, int valueLength) throws IOException {
      writeValueMarker();

      // bufferWriteInt(keyLength);
      // bufferWriteLong(valueLength);
      long combined = ((long) keyLength << 32) | (valueLength & 0xFFFFFFFFL);
      bufferWriteLong(combined);

      bufferWriteBytes(keyData, keyPos, keyLength);
      bufferWriteBytes(valueData, valPos, valueLength);

      // Update bytes written
      decompressedBytesWritten += keyLength + valueLength + INT_SIZE + INT_SIZE;
      if (serializedUncompressedBytes != null) {
        serializedUncompressedBytes.increment(keyLength + valueLength);
      }
    }

    private void writeRLE() throws IOException {
      /**
       * To strike a balance between 2 use cases (lots of unique KV in stream
       * vs lots of identical KV in stream), we start off by writing KV pair.
       * If subsequent KV is identical, we write RLE marker along with V_END_MARKER
       * {KL1, VL1, K1, V1}
       * {RLE, VL2, V2, VL3, V3, ...V_END_MARKER}
       */
      if (prevKey != REPEAT_KEY) {
        bufferWriteInt(RLE_MARKER);
        decompressedBytesWritten += RLE_MARKER_SIZE;
        rleWritten++;
      }
    }

    protected void writeValueMarker() throws IOException {
      /**
       * Write V_END_MARKER only in RLE scenario. This will
       * save space in conditions where lots of unique KV pairs are found in the
       * stream.
       */
      if (prevKey == REPEAT_KEY) {
        bufferWriteInt(V_END_MARKER);
        decompressedBytesWritten += V_END_MARKER_SIZE;
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
          out.write(data, off, len);
          return;
        }
      }
      System.arraycopy(data, off, writeBuffer, writeOffset, len);
      writeOffset += len;
    }

    protected void flushWriteBuffer() throws IOException {
      if (writeOffset > 0) {
        out.write(writeBuffer, 0, writeOffset);
        writeOffset = 0;
      }
    }

    public long getRawLength() {
      return decompressedBytesWritten;
    }

    public long getCompressedLength() {
      return compressedBytesWritten;
    }
  }

  /**
   * <code>IFile.Reader</code> to read intermediate map-outputs.
   */
  public static class Reader {

    public enum KeyState {NO_KEY, NEW_KEY, SAME_KEY}

    private static final int MAX_BUFFER_SIZE
            = Integer.MAX_VALUE - 8;  // The maximum array size is a little less than the
                                      // max integer value. Trying to create a larger array
                                      // will result in an OOM exception. The exact value
                                      // is JVM dependent so setting it to max int - 8 to be safe.

    // Count records read from disk
    private long numRecordsRead = 0;
    private final TezCounter readRecordsCounter;
    private final TezCounter bytesReadCounter;

    final InputStream in;        // Possibly decompressed stream that we read
    Decompressor decompressor;
    public long bytesRead = 0;
    final long fileLength;
    protected boolean eof = false;
    IFileInputStream checksumIn;

    protected DataInputStream dataIn = null;

    protected int recNo = 1;
    protected int originalKeyLength;
    protected int prevKeyLength;
    byte keyBytes[] = new byte[0];

    protected int currentKeyLength;
    protected int currentValueLength;
    long startPos;

    private CompressionCodec codec;
    private DecompressorPool taskContext;

    /**
     * Construct an IFile Reader.
     *
     * @param in   The input stream
     * @param length Length of the data in the stream, including the checksum
     *               bytes.
     * @param codec codec
     * @param readsCounter Counter for records read from disk
     * @throws IOException
     */
    public Reader(InputStream in, long length,
        CompressionCodec codec,
        TezCounter readsCounter, TezCounter bytesReadCounter,
        boolean readAhead, int readAheadLength,
        DecompressorPool taskContext) throws IOException {
      this(in, ((in != null) ? (length - HEADER.length) : length), codec,
          readsCounter, bytesReadCounter, readAhead, readAheadLength,
          taskContext, ((in != null) ? isCompressedFlagEnabled(in) : false));
      if (in != null && bytesReadCounter != null) {
        bytesReadCounter.increment(IFile.HEADER.length);
      }
    }

    /**
     * Construct an IFile Reader.
     *
     * @param in   The input stream
     * @param length Length of the data in the stream, including the checksum
     *               bytes.
     * @param codec codec
     * @param readsCounter Counter for records read from disk
     * @throws IOException
     */
    private Reader(InputStream in, long length,
                  CompressionCodec codec,
                  TezCounter readsCounter, TezCounter bytesReadCounter,
                  boolean readAhead, int readAheadLength,
                  DecompressorPool taskContext, boolean isCompressed) throws IOException {
      if (in != null) {
        checksumIn = new IFileInputStream(in, length, readAhead,
            readAheadLength/* , isCompressed */);
        if (isCompressed && codec != null) {
          assert taskContext != null;
          this.codec = codec;
          this.taskContext = taskContext;
          decompressor = taskContext.getDecompressor(codec);
          if (decompressor != null) {
            this.in = CodecUtils.createInputStream(codec, checksumIn, decompressor);
          } else {
            LOG.warn("Could not obtain decompressor from CodecPool");
            this.in = checksumIn;
          }
        } else {
          this.in = checksumIn;
        }
        startPos = checksumIn.getPosition();
      } else {
        this.in = null;
      }

      if (in != null) {
        this.dataIn = new DataInputStream(this.in);
      }
      this.readRecordsCounter = readsCounter;
      this.bytesReadCounter = bytesReadCounter;
      this.fileLength = length;
    }

    /**
     * Read entire ifile content to memory.
     *
     * @param buffer
     * @param in
     * @param compressedLength
     * @param codec
     * @param ifileReadAhead
     * @param ifileReadAheadLength
     * @throws IOException
     */
    public static void readToMemory(byte[] buffer, InputStream in, int compressedLength,
        CompressionCodec codec, boolean ifileReadAhead, int ifileReadAheadLength,
        TaskContext taskContext, boolean useThreadLocalDecompressor)
        throws IOException {
      boolean isCompressed = IFile.Reader.isCompressedFlagEnabled(in);
      IFileInputStream checksumIn = new IFileInputStream(in,
          compressedLength - IFile.HEADER.length, ifileReadAhead,
          ifileReadAheadLength);
      in = checksumIn;
      Decompressor decompressor = null;
      if (isCompressed && codec != null) {
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
        if (decompressor != null) {
          decompressor.reset();
          in = CodecUtils.getDecompressedInputStreamWithBufferSize(codec, checksumIn, decompressor,
              compressedLength);
        } else {
          LOG.warn("Could not obtain decompressor from CodecPool");
          in = checksumIn;
        }
      }
      try {
        IOUtils.readFully(in, buffer, 0, buffer.length - IFile.HEADER.length);
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
      } finally {
        if (decompressor != null) {
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
      }
    }

    /**
     * Read entire IFile content to disk.
     *
     * @param out the output stream that will receive the data
     * @param in the input stream containing the IFile data
     * @param length the amount of data to read from the input
     * @return the number of bytes copied
     * @throws IOException
     */
    public static long readToDisk(OutputStream out, InputStream in, long length,
        boolean ifileReadAhead, int ifileReadAheadLength)
        throws IOException {
      final int BYTES_TO_READ = 64 * 1024;
      byte[] buf = new byte[BYTES_TO_READ];

      // copy the IFile header
      if (length < HEADER.length) {
        throw new IOException("Missing IFile header");
      }
      IOUtils.readFully(in, buf, 0, HEADER.length);
      verifyHeaderMagic(buf);
      out.write(buf, 0, HEADER.length);
      long bytesLeft = length - HEADER.length;
      @SuppressWarnings("resource")
      IFileInputStream ifInput = new IFileInputStream(in, bytesLeft,
          ifileReadAhead, ifileReadAheadLength);
      while (bytesLeft > 0) {
        int n = ifInput.readWithChecksum(buf, 0, (int) Math.min(bytesLeft, BYTES_TO_READ));
        if (n < 0) {
          throw new IOException("read past end of stream");
        }
        out.write(buf, 0, n);
        bytesLeft -= n;
      }
      return length - bytesLeft;
    }

    public long getLength() {
      return fileLength - checksumIn.getSize();
    }

    public long getPosition() throws IOException {
      return checksumIn.getPosition();
    }

    /**
     * Read up to len bytes into buf starting at offset off.
     *
     * @param buf buffer
     * @param len length of buffer
     * @return the no. of bytes read
     * @throws IOException
     */
    private int readData(byte[] buf, int len) throws IOException {
      int bytesRead = 0;
      while (bytesRead < len) {
        int n = IOUtils.wrappedReadForCompressedData(in, buf, bytesRead, len - bytesRead);
        if (n < 0) {
          return bytesRead;
        }
        bytesRead += n;
      }
      return len;
    }

    protected void readValueLength(DataInput dIn) throws IOException {
      currentValueLength = dIn.readInt();
      bytesRead += INT_SIZE;
      if (currentValueLength == V_END_MARKER) {
        readKeyValueLength(dIn);
      }
    }

    protected void readKeyValueLength(DataInput dIn) throws IOException {
      currentKeyLength = dIn.readInt();
      currentValueLength = dIn.readInt();
      // long combined = dIn.readLong();
      // currentKeyLength = (int) (combined >> 32);
      // currentValueLength = (int) combined;

      if (currentKeyLength != RLE_MARKER) {
        // original key length
        originalKeyLength = currentKeyLength;
      }
      bytesRead += INT_SIZE + INT_SIZE;
    }

    /**
     * Reset key length and value length for next record in the file
     *
     * @param dIn
     * @return true if key length and value length were set to the next
     *         false if end of file (EOF) marker was reached
     * @throws IOException
     */
    protected boolean positionToNextRecord(DataInput dIn) throws IOException {
      // Sanity check
      if (eof) {
        throw new IOException(String.format("Reached EOF. Completed reading %d", bytesRead));
      }
      prevKeyLength = currentKeyLength;

      if (prevKeyLength == RLE_MARKER) {
        // Same key as previous one. Just read value length alone
        readValueLength(dIn);
      } else {
        readKeyValueLength(dIn);
      }

      // Check for EOF
      if (currentKeyLength == EOF_MARKER && currentValueLength == EOF_MARKER) {
        eof = true;
        return false;
      }

      // Sanity check
      if (currentKeyLength != RLE_MARKER && currentKeyLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative key-length: " +
                              currentKeyLength + " PreviousKeyLen: " + prevKeyLength);
      }
      if (currentValueLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative value-length: " +
                              currentValueLength);
      }
      return true;
    }

    public final boolean nextRawKey(DataInputBuffer key) throws IOException {
      return readRawKey(key) != KeyState.NO_KEY;
    }

    private static byte[] createLargerArray(int currentLength) {
      if (currentLength > MAX_BUFFER_SIZE) {
        throw new IllegalArgumentException(
                String.format(REQ_BUFFER_SIZE_TOO_LARGE, currentLength, MAX_BUFFER_SIZE));
      }
      int newLength;
      if (currentLength > (MAX_BUFFER_SIZE - currentLength)) {
        // possible overflow: if (2*currentLength > MAX_BUFFER_SIZE)
        newLength = currentLength;
      } else {
        newLength = currentLength << 1;
      }
      return new byte[newLength];
    }

    public KeyState readRawKey(DataInputBuffer key) throws IOException {
      if (!positionToNextRecord(dataIn)) {
        if (isDebugEnabled) {
          LOG.debug("currentKeyLength=" + currentKeyLength +
              ", currentValueLength=" + currentValueLength +
              ", bytesRead=" + bytesRead +
              ", length=" + fileLength);
        }
        return KeyState.NO_KEY;
      }
      if(currentKeyLength == RLE_MARKER) {
        // get key length from original key
        key.reset(keyBytes, originalKeyLength);
        return KeyState.SAME_KEY;
      }
      if (keyBytes.length < currentKeyLength) {
        keyBytes = createLargerArray(currentKeyLength);
      }
      int i = readData(keyBytes, currentKeyLength);
      if (i != currentKeyLength) {
        throw new IOException(String.format(INCOMPLETE_READ, currentKeyLength, i));
      }
      key.reset(keyBytes, currentKeyLength);
      bytesRead += currentKeyLength;
      return KeyState.NEW_KEY;
    }

    public void nextRawValue(DataInputBuffer value) throws IOException {
      final byte[] valBytes;
      if ((value.getData().length < currentValueLength) || (value.getData() == keyBytes)) {
        valBytes = createLargerArray(currentValueLength);
      } else {
        valBytes = value.getData();
      }

      int i = readData(valBytes, currentValueLength);
      if (i != currentValueLength) {
        throw new IOException(String.format(INCOMPLETE_READ, currentValueLength, i));
      }
      value.reset(valBytes, currentValueLength);

      // Record the bytes read
      bytesRead += currentValueLength;

      ++recNo;
      ++numRecordsRead;
    }

    private static void verifyHeaderMagic(byte[] header) throws IOException {
      if (!(header[0] == 'T' && header[1] == 'I'
          && header[2] == 'F')) {
        throw new IOException("Not a valid ifile header");
      }
    }

    public static boolean isCompressedFlagEnabled(InputStream in) throws IOException {
      byte[] header = new byte[HEADER.length];
      IOUtils.readFully(in, header, 0, HEADER.length);
      verifyHeaderMagic(header);
      return (header[3] == 1);
    }

    public void close() throws IOException {
      // Close the underlying stream
      in.close();

      // Release the buffer
      dataIn = null;
      if (readRecordsCounter != null) {
        readRecordsCounter.increment(numRecordsRead);
      }

      if (bytesReadCounter != null) {
        bytesReadCounter.increment(checksumIn.getPosition() - startPos + checksumIn.getSize());
      }

      // Return the decompressor
      if (decompressor != null) {
        decompressor.reset();
        if (taskContext != null) {
          taskContext.returnDecompressor(codec.getCompressorType(), decompressor);
        } else {
          CodecPool.returnDecompressor(decompressor);
        }
        decompressor = null;
      }
    }

    public void reset(int offset) {
      return;
    }
  }

}
