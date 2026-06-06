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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.runtime.api.CompressorPool;
import org.apache.tez.runtime.api.DecompressorPool;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.api.TezOffsetRecord;
import org.apache.tez.runtime.api.TezTaskOutput;
import org.apache.tez.runtime.library.api.KeyValueReaderEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.tez.runtime.library.utils.CodecUtils;
import org.apache.tez.util.FastByteComparisons;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
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
  public static final byte FLAG_COMPRESSED = 0x01;
  public static final byte FLAG_RLE_ENABLED = 0x02;

  // REPEAT_KEY is primarily an ordered-path optimization, and never used for unordered output.
  public static final TezRawDataBuffer REPEAT_KEY = new TezRawDataBuffer();
  public static final byte[] HEADER = new byte[] { (byte) 'T', (byte) 'I', (byte) 'F', (byte) 0};

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

  public static int getHeaderLength() {
    return HEADER.length;
  }

  public static int getEOFMarkerLength() {
    return 2 * INT_SIZE;
  }

  public interface WriterAppendDataInputBuffer {
    boolean isRleEnabled();

    // call when isRleEnabled is not statically known
    void appendNoRle(TezRawDataBuffer key, TezRawDataBuffer value) throws IOException;
    void appendNoRleTez(TezRawDataBuffer key, TezRawDataBuffer value) throws IOException;

    // if key != IFile.REPEAT_KEY, perform key comparison to check whether 'key' is a new key or not
    void appendRle(TezRawDataBuffer key, TezRawDataBuffer value) throws IOException;

    void close() throws IOException;
  }

  public interface WriterAppendBytesWritable {
    boolean isRleEnabled();

    void appendNoRle(BytesWritable key, BytesWritable value) throws IOException;
    void appendNoRleTez(BytesWritable key, BytesWritable value) throws IOException;

    // if key != IFile.REPEAT_KEY, perform key comparison to check whether 'key' is a new key or not
    void appendRle(BytesWritable key, BytesWritable value) throws IOException;

    void close() throws IOException;
  }

  private static final int checksumSize = IFileOutputStream.CHECKSUM_SIZE;

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
  public static class FileBackedInMemIFileWriter extends WriterBytesWritable {

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
     * @param fs
     * @param taskOutput
     * @param codec
     * @param writesCounter
     * @param serializedBytesCounter
     * @param cacheSize
     * @throws IOException
     */
    public FileBackedInMemIFileWriter(FileSystem fs, TezTaskOutput taskOutput,
        CompressionCodec codec, TezCounter writesCounter,
        TezCounter serializedBytesCounter, int cacheSize,
        byte[] writeBuffer, CompressorPool taskContext) throws IOException {
      super(new FSDataOutputStream(createBoundedBuffer(cacheSize), null), null,
          writesCounter, serializedBytesCounter, false, false, -1, -1,
          writeBuffer, null, taskContext);
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
        totalSize += INT_SIZE + keyLength + INT_SIZE + valueLength;

        if (shouldWriteToDisk()) {
          resetToFileBasedWriter();
        }
      }
      super.writeKVPair(keyData, keyPos, keyLength, valueData, valPos, valueLength);
    }

    @Override
    protected void writeValue(byte[] data, int offset, int length) throws IOException {
      if (!bufferFull) {
        totalSize += INT_SIZE + length;

        if (shouldWriteToDisk()) {
          resetToFileBasedWriter();
        }
      }
      super.writeValue(data, offset, length);
    }

    @Override
    public void appendNoRleTez(BytesWritable key, BytesWritable value) throws IOException {
      assert false;
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
  public abstract static class Writer {
    // DataOutput: rawOut <-- checksumOut <-- compressedOut <-- out <-- [writeBuffer]

    protected FSDataOutputStream rawOut;
    private final TezCounter writtenRecordsCounter;
    private final TezCounter serializedUncompressedBytes;
    private final long start;

    protected final boolean useMaxKeyValLen;
    protected final boolean isRleEnabled;

    // We use writeBuffer[] to reduce the number of writes to 'out' and thus
    // to reduce the number of writes to 'compressedOut'.
    private final byte[] writeBuffer;
    private final int writeBufferLength;        // must be larger than 8 (# of bytes in long)
    private int writeOffset;

    private final Compressor compressorExternal;  // not to be shared with concurrent threads
    private final CompressorPool taskContext;
    private final CompressionCodec codec;

    private IFileOutputStream checksumOut;
    private CompressionOutputStream compressedOut;
    private Compressor compressor;
    private boolean compressOutput = false;
    protected DataOutputStream out;

    // true iff this Writer created and owns rawOut
    // if true, close() closes rawOut.
    protected boolean ownOutputStream = false;

    protected boolean headerWritten = false;

    // passed to TezIndexRecord
    private long decompressedBytesWritten = 0;
    private long compressedBytesWritten = 0;

    // Count records written to disk
    protected long numRecordsWritten = 0;
    // Count serialized key/value bytes for counter updates.
    protected long numSerializedBytesWritten = 0;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    protected int maxKeyLen;
    protected int maxValLen;
    protected boolean keyLenTransitioned = false;
    protected boolean valLenTransitioned = false;
    protected int firstKeyOffset = -1;
    protected int firstValOffset = -1;
    protected int eofPos = -1;

    protected Writer(FSDataOutputStream outputStream,
                     CompressionCodec codec,
                     TezCounter writesCounter, TezCounter serializedBytesCounter,
                     boolean useMaxKeyValLen,
                     boolean isRleEnabled,
                     int maxKeyLen, int maxValLen,
                     byte[] writeBuffer,
                     @Nullable Compressor compressorExternal,
                     CompressorPool taskContext) throws IOException {
      this.rawOut = outputStream;
      this.writtenRecordsCounter = writesCounter;
      this.serializedUncompressedBytes = serializedBytesCounter;
      this.start = this.rawOut.getPos();

      this.useMaxKeyValLen = useMaxKeyValLen;
      this.isRleEnabled = isRleEnabled;
      this.maxKeyLen = maxKeyLen;
      this.maxValLen = maxValLen;
      assert !(useMaxKeyValLen && isRleEnabled);
      assert !(!useMaxKeyValLen) || (maxKeyLen == -1 && maxValLen == -1);

      this.writeBuffer = writeBuffer;
      this.writeBufferLength = writeBuffer.length;
      this.writeOffset = 0;

      this.compressorExternal = compressorExternal;
      this.taskContext = taskContext;
      this.codec = codec;

      setupOutputStream(codec);
      writeHeader(outputStream);
    }

    void setupOutputStream(CompressionCodec codec) throws IOException {
      this.checksumOut = new IFileOutputStream(this.rawOut);
      if (codec != null) {
        if (compressorExternal != null) {
          this.compressor = compressorExternal;
        } else {
          this.compressor = taskContext.getCompressor(codec);
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
        byte flag = 0;
        if (compressOutput) {
          flag |= FLAG_COMPRESSED;
        }
        if (isRleEnabled) {
          flag |= FLAG_RLE_ENABLED;
        }
        outputStream.write(flag);
        headerWritten = true;
      }
    }

    public void close() throws IOException {
      if (closed.getAndSet(true)) {
        throw new IOException("Writer was already closed earlier");
      }

      onClose();

      // Write EOF_MARKER for key/value length
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
      // header bytes are already included in rawOut
      compressedBytesWritten = rawOut.getPos() - start;

      if (compressOutput) {
        // Return back the compressor
        // if compressorExternal != null, this Writer does not own compressor, so do not return it to CodecPool
        if (compressorExternal == null) {
          // this Writer owns compressor
          taskContext.returnCompressor(codec.getCompressorType(), compressor);
        }
        compressor = null;
      }

      out = null;
      if (writtenRecordsCounter != null) {
        writtenRecordsCounter.increment(numRecordsWritten);
      }
      if (serializedUncompressedBytes != null) {
        serializedUncompressedBytes.increment(numSerializedBytesWritten);
      }
    }

    protected void writeValue(byte[] data, int offset, int length) throws IOException {
      bufferWriteInt(length); // value length
      bufferWriteBytes(data, offset, length);
      // Update bytes written
      decompressedBytesWritten += length + INT_SIZE;
      numSerializedBytesWritten += length;
    }

    protected void writeKVPair(byte[] keyData, int keyPos, int keyLength,
        byte[] valueData, int valPos, int valueLength) throws IOException {
      long combined = ((long) valueLength << 32) | (keyLength & 0xFFFFFFFFL);
      bufferWriteLong(combined);

      bufferWriteBytes(keyData, keyPos, keyLength);
      bufferWriteBytes(valueData, valPos, valueLength);

      // Update bytes written
      decompressedBytesWritten += keyLength + valueLength + INT_SIZE + INT_SIZE;
      numSerializedBytesWritten += keyLength + valueLength;
    }

    protected void onClose() throws IOException {
      if (useMaxKeyValLen) {
        if (numRecordsWritten == 0) {
          maxKeyLen = 0;
          maxValLen = 0;
        }
        eofPos = (int) decompressedBytesWritten;
        if (firstKeyOffset < 0) {
          firstKeyOffset = eofPos;
        }
        if (firstValOffset < 0) {
          firstValOffset = eofPos;
        }
      }
    }

    protected void incrementDecompressedBytesWritten(long length) {
      decompressedBytesWritten += length;
    }

    protected long getDecompressedBytesWritten() {
      return decompressedBytesWritten;
    }

    protected void bufferWriteInt(int val) throws IOException {
      final int len = 4;
      final int remaining = writeBufferLength - writeOffset;
      if (len > remaining) {
        flushWriteBuffer();
      }
      FastByteComparisons.theUnsafe.putInt(writeBuffer,
          FastByteComparisons.BYTE_ARRAY_BASE_OFFSET + (long) writeOffset, val);
      writeOffset += len;
    }

    protected void bufferWriteLong(long val) throws IOException {
      final int len = 8;
      final int remaining = writeBufferLength - writeOffset;
      if (len > remaining) {
        flushWriteBuffer();
      }
      FastByteComparisons.theUnsafe.putLong(writeBuffer,
          FastByteComparisons.BYTE_ARRAY_BASE_OFFSET + (long) writeOffset, val);
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

    @Nullable
    public TezOffsetRecord getTezOffsetRecord() {
      if (!useMaxKeyValLen) {
        return null;
      } else {
        assert eofPos >= 0;   // must be called after close()
        return new TezOffsetRecord(maxKeyLen, maxValLen, firstKeyOffset, firstValOffset, eofPos);
      }
    }
  }

  public static class WriterDataInputBuffer extends Writer implements WriterAppendDataInputBuffer {

    private final DataOutputBuffer previous = new DataOutputBuffer();
    private boolean previousSameKey = false;

    private long rleWritten = 0;      //number of RLE markers written
    private long totalKeySaving = 0;  //number of keys saved due to multi KV writes + RLE

    private static final int RLE_MARKER_SIZE = INT_SIZE;
    private static final int V_END_MARKER_SIZE = INT_SIZE;

    public WriterDataInputBuffer(FileSystem fs, Path file,
                                 CompressionCodec codec,
                                 TezCounter writesCounter,
                                 TezCounter serializedBytesCounter,
                                 boolean isRleEnabled,
                                 byte[] writeBuffer,
                                 CompressorPool taskContext) throws IOException {
      this(fs.create(file), codec, writesCounter, serializedBytesCounter,
          false, isRleEnabled, -1, -1,
          writeBuffer, null, taskContext);
      this.ownOutputStream = true;
    }

    public WriterDataInputBuffer(FSDataOutputStream outputStream,
                                 CompressionCodec codec,
                                 TezCounter writesCounter,
                                 TezCounter serializedBytesCounter,
                                 boolean useMaxKeyValLen,
                                 boolean isRleEnabled,
                                 int maxKeyLen, int maxValLen,
                                 byte[] writeBuffer,
                                 @Nullable Compressor compressorExternal,
                                 CompressorPool taskContext)
        throws IOException {
      super(outputStream, codec, writesCounter, serializedBytesCounter, useMaxKeyValLen, isRleEnabled,
          maxKeyLen, maxValLen,
          writeBuffer, compressorExternal, taskContext);
    }

    public boolean isRleEnabled() {
      return isRleEnabled;
    }

    public void appendNoRle(TezRawDataBuffer key, TezRawDataBuffer value) throws IOException {
      assert !isRleEnabled && !useMaxKeyValLen;
      int keyLength = key.getLength() - key.getPosition();
      int valueLength = value.getLength() - value.getPosition();

      super.writeKVPair(key.getData(), key.getPosition(), keyLength,
          value.getData(), value.getPosition(), valueLength);
      ++numRecordsWritten;
    }

    public void appendNoRle(byte[] keyData, int keyOffset, int keyLength,
                            byte[] valueData, int valueOffset, int valueLength) throws IOException {
      assert !isRleEnabled && !useMaxKeyValLen;
      super.writeKVPair(keyData, keyOffset, keyLength, valueData, valueOffset, valueLength);
      ++numRecordsWritten;
    }

    public void appendNoRleTez(TezRawDataBuffer key, TezRawDataBuffer value) throws IOException {
      assert !isRleEnabled && useMaxKeyValLen;
      int keyLength = key.getLength() - key.getPosition();
      int valueLength = value.getLength() - value.getPosition();

      if (maxKeyLen < 0 || maxValLen < 0) {
        maxKeyLen = keyLength;
        maxValLen = valueLength;
      }

      int lengthBytes = 0;
      if (!keyLenTransitioned
          && (keyLength != maxKeyLen || (numRecordsWritten == 0 && keyLength == 0))) {
        keyLenTransitioned = true;
        firstKeyOffset = (int) getDecompressedBytesWritten();
      }
      if (!valLenTransitioned
          && (valueLength != maxValLen || (numRecordsWritten == 0 && valueLength == 0))) {
        valLenTransitioned = true;
        firstValOffset = (int) getDecompressedBytesWritten();
      }
      if (keyLenTransitioned) {
        bufferWriteInt(keyLength);
        lengthBytes += INT_SIZE;
      }
      if (valLenTransitioned) {
        bufferWriteInt(valueLength);
        lengthBytes += INT_SIZE;
      }
      bufferWriteBytes(key.getData(), key.getPosition(), keyLength);
      bufferWriteBytes(value.getData(), value.getPosition(), valueLength);

      incrementDecompressedBytesWritten(lengthBytes + keyLength + valueLength);
      numSerializedBytesWritten += keyLength + valueLength;
      ++numRecordsWritten;
    }

    public void appendRle(TezRawDataBuffer key, TezRawDataBuffer value) throws IOException {
      assert isRleEnabled && !useMaxKeyValLen;
      int keyLength = key.getLength() - key.getPosition();
      int valueLength = value.getLength() - value.getPosition();

      if (key == REPEAT_KEY) {
        appendRepeatValue(value.getData(), value.getPosition(), valueLength);
        ++numRecordsWritten;
        return;
      }

      appendRle(key.getData(), key.getPosition(), keyLength,
          value.getData(), value.getPosition(), valueLength);
    }

    public void appendRle(byte[] keyData, int keyOffset, int keyLength,
                          byte[] valueData, int valueOffset, int valueLength) throws IOException {
      assert isRleEnabled && !useMaxKeyValLen;
      boolean sameKey = keyLength != 0 && FastByteComparisons.compareEqual(
          previous.getData(), 0, previous.getLength(), keyData, keyOffset, keyLength);
      if (sameKey) {
        appendRepeatValue(valueData, valueOffset, valueLength);
      } else {
        writeValueMarker();
        super.writeKVPair(keyData, keyOffset, keyLength, valueData, valueOffset, valueLength);
        previous.reset();
        previous.write(keyData, keyOffset, keyLength);
        previousSameKey = false;
      }
      ++numRecordsWritten;
    }

    private void appendRepeatValue(byte[] valueData, int valueOffset, int valueLength) throws IOException {
      if (!previousSameKey) {
        bufferWriteInt(RLE_MARKER);
        incrementDecompressedBytesWritten(RLE_MARKER_SIZE);
        rleWritten++;
      }
      super.writeValue(valueData, valueOffset, valueLength);
      totalKeySaving++;
      previousSameKey = true;
    }

    private void writeValueMarker() throws IOException {
      if (previousSameKey) {
        bufferWriteInt(V_END_MARKER);
        incrementDecompressedBytesWritten(V_END_MARKER_SIZE);
      }
    }

    @Override
    protected void onClose() throws IOException {
      super.onClose();
      if (isRleEnabled) {
        writeValueMarker();
      }
      if (isDebugEnabled) {
        LOG.debug("WriterInputBuffer rleEnabled=" + isRleEnabled + "; Savings(due to multi-kv/rle)="
            + totalKeySaving + "; number of RLEs written=" + rleWritten);
      }
    }

    /**
     * Append raw uncompressed record bytes to this IFile writer.
     * This API is intended for merging existing uncompressed IFile segments
     * without materializing every key/value record.
     *
     * @param in input stream positioned at the start of raw record bytes
     * @param rawDataLength number of raw record bytes to copy
     */
    public void append(IFileInputStream in, long rawDataLength) throws IOException {
      final int chunkSize = 64 * 1024;
      byte[] buffer = new byte[chunkSize];
      long remaining = rawDataLength;
      while (remaining > 0) {
        int toRead = (int) Math.min(remaining, chunkSize);
        int read = in.read(buffer, 0, toRead);
        if (read < 0) {
          throw new IOException("Unexpected EOF while copying raw IFile data");
        }
        bufferWriteBytes(buffer, 0, read);
        incrementDecompressedBytesWritten(read);
        remaining -= read;
      }
    }
  }

  public static class WriterBytesWritable extends Writer implements WriterAppendBytesWritable {
    private final DataOutputBuffer previous = new DataOutputBuffer();
    private boolean prevSameKey = false;

    private long rleWritten = 0;      //number of RLE markers written
    private long totalKeySaving = 0;  //number of keys saved due to multi KV writes + RLE

    private static final int RLE_MARKER_SIZE = INT_SIZE;
    private static final int V_END_MARKER_SIZE = INT_SIZE;

    public WriterBytesWritable(FileSystem fs, Path file,
        CompressionCodec codec,
        TezCounter writesCounter,
        TezCounter serializedBytesCounter,
        boolean useMaxKeyValLen,
        boolean isRleEnabled,
        int maxKeyLen, int maxValLen,
        byte[] writeBuffer,
        CompressorPool taskContext) throws IOException {
      this(fs.create(file), codec, writesCounter, serializedBytesCounter, useMaxKeyValLen, isRleEnabled,
          maxKeyLen, maxValLen,
          writeBuffer, null, taskContext);
      ownOutputStream = true;
    }

    public WriterBytesWritable(FSDataOutputStream outputStream,
        CompressionCodec codec, TezCounter writesCounter, TezCounter serializedBytesCounter,
        boolean useMaxKeyValLen,
        boolean isRleEnabled,
        int maxKeyLen, int maxValLen,
        byte[] writeBuffer, @Nullable Compressor compressorExternal,
        CompressorPool taskContext)
        throws IOException {
      super(outputStream, codec, writesCounter, serializedBytesCounter, useMaxKeyValLen, isRleEnabled,
          maxKeyLen, maxValLen,
          writeBuffer, compressorExternal, taskContext);
    }

    public boolean isRleEnabled() {
      return isRleEnabled;
    }

    public void appendNoRle(BytesWritable key, BytesWritable value) throws IOException {
      assert !isRleEnabled;
      assert !useMaxKeyValLen;
      int keyLength = key.getLength();
      int valueLength = value.getLength();

      writeKVPair(key.getBytesRaw(), key.getOffset(), keyLength,
          value.getBytesRaw(), value.getOffset(), valueLength);
      ++numRecordsWritten;
    }

    public void appendNoRleTez(BytesWritable key, BytesWritable value) throws IOException {
      assert !isRleEnabled;
      assert useMaxKeyValLen;
      int recordStartOffset = (int) getDecompressedBytesWritten();
      int keyLength = key.getLength();
      int valueLength = value.getLength();

      if (maxKeyLen < 0 || maxValLen < 0) {
        maxKeyLen = keyLength;
        maxValLen = valueLength;
      }

      if (!keyLenTransitioned
          && (keyLength != maxKeyLen || (numRecordsWritten == 0 && keyLength == 0))) {
        keyLenTransitioned = true;
        firstKeyOffset = recordStartOffset;
      }
      if (!valLenTransitioned
          && (valueLength != maxValLen || (numRecordsWritten == 0 && valueLength == 0))) {
        valLenTransitioned = true;
        firstValOffset = recordStartOffset;
      }

      int lengthBytes = 0;
      if (keyLenTransitioned) {
        bufferWriteInt(keyLength);
        lengthBytes += INT_SIZE;
      }
      if (valLenTransitioned) {
        bufferWriteInt(valueLength);
        lengthBytes += INT_SIZE;
      }
      bufferWriteBytes(key.getBytesRaw(), key.getOffset(), keyLength);
      bufferWriteBytes(value.getBytesRaw(), value.getOffset(), valueLength);
      incrementDecompressedBytesWritten(lengthBytes + keyLength + valueLength);
      numSerializedBytesWritten += keyLength + valueLength;
      ++numRecordsWritten;
    }

    public void appendRle(BytesWritable key, BytesWritable value) throws IOException {
      assert isRleEnabled;
      assert !useMaxKeyValLen;
      int keyLength = key.getLength();
      int valueLength = value.getLength();

      boolean sameKey = keyLength != 0 && compareEqual(previous, key);
      if (!sameKey) {
        writeValueMarker();
        super.writeKVPair(key.getBytesRaw(), key.getOffset(), keyLength,
            value.getBytesRaw(), value.getOffset(), valueLength);
        copyKeyToPrevious(key);
        prevSameKey = false;
      } else {
        if (!prevSameKey) {
          bufferWriteInt(RLE_MARKER);
          incrementDecompressedBytesWritten(RLE_MARKER_SIZE);
          rleWritten++;
        }
        super.writeValue(value.getBytesRaw(), value.getOffset(), valueLength);
        totalKeySaving++;
        prevSameKey = true;
      }
      ++numRecordsWritten;
    }

    private void copyKeyToPrevious(BytesWritable key) throws IOException {
      previous.reset();
      previous.write(key.getBytesRaw(), key.getOffset(), key.getLength());
    }

    private static boolean compareEqual(DataOutputBuffer previous, BytesWritable key) {
      int keyLength = key.getLength();
      if (previous.getLength() != keyLength) {
        return false;
      }
      byte[] previousBytes = previous.getData();
      byte[] keyBytes = key.getBytesRaw();
      int keyOffset = key.getOffset();
      for (int i = 0; i < keyLength; i++) {
        if (previousBytes[i] != keyBytes[keyOffset + i]) {
          return false;
        }
      }
      return true;
    }

    private void writeValueMarker() throws IOException {
      if (prevSameKey) {
        bufferWriteInt(V_END_MARKER);
        incrementDecompressedBytesWritten(V_END_MARKER_SIZE);
      }
    }

    @Override
    protected void onClose() throws IOException {
      super.onClose();
      if (isRleEnabled) {
        writeValueMarker();
      }
      if (isDebugEnabled) {
        LOG.debug("WriterBytesWritable rleEnabled=" + isRleEnabled
            + "; Savings(due to multi-kv/rle)=" + totalKeySaving
            + "; number of RLEs written=" + rleWritten);
      }
    }
  }

  public interface KeyValueReaderBase {
    long getLength();
    void close() throws IOException;
  }

  public interface KeyValueReaderDataInputBuffer extends KeyValueReaderBase {
    Reader.KeyState readRawKey(TezRawDataBuffer key) throws IOException;
    void nextRawValue(TezRawDataBuffer value) throws IOException;

    /**
     * Reports whether the most recently loaded current record returned through
     * readRawKey(TezRawDataBuffer) and nextRawValue(TezRawDataBuffer)
     * has stable backing byte arrays.
     *
     * Stable means the byte[] slices exposed through TezRawDataBuffer may be
     * retained by the caller without being overwritten or reused by this reader.
     * The result must be safe for both the key and the value of the current
     * record, based on the invariant that current merge-based records are not
     * assembled from different segments. If a reader cannot prove backing-array
     * stability, it must return false.
     *
     * @return true if both key and value backing arrays for the current record
     *         are stable; false otherwise.
     */
    boolean isCurrentRecordStable();
  }

  public interface KeyValueReaderBytesWritable extends KeyValueReaderBase {
    // Contract: readRawKey()/nextRawValue() and consumeAll() are mutually exclusive and must not be mixed.
    // Invariant: key already contains the previous key read from this stream.
    // On the first call, key can be any BytesWritable instance.
    // After readRawKey() returns, the backing byte[] array is immutable, so the consumer may keep pointers to it.
    Reader.KeyState readRawKey(BytesWritable key) throws IOException;
    // After readRawValue() returns, the backing byte[] array is immutable, so the consumer may keep pointers to it.
    void nextRawValue(BytesWritable value) throws IOException;

    // Retrieves all key/value pairs, where both BytesWritable arguments are backed by immutable byte[] arrays.
    // consumeAll() must not be mixed with readRawKey()/nextRawValue().
    long consumeAll(KeyValueReaderEdge.ThrowingBiConsumer<BytesWritable, BytesWritable> consumer) throws Exception;
  }

  public interface KeyValueReader extends KeyValueReaderDataInputBuffer, KeyValueReaderBytesWritable {
  }

  /**
   * <code>IFile.Reader</code> to read intermediate map-outputs.
   */
  public static class Reader implements KeyValueReader {

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

    private CompressionCodec codec;
    private DecompressorPool taskContext;
    private Decompressor decompressor;
    private final IFileInputStream checksumIn;
    private final InputStream in;   // Possibly decompressed stream that we read
    private final long startPos;

    private static final int READ_BUFFER_SIZE = 8 * 1024;   // the usual general-purpose java.io default buffer size

    private final byte[] readBuffer = new byte[READ_BUFFER_SIZE];
    private int readBufferPos = 0;
    private int readBufferLimit = 0;
    private final long fileLength;
    private final boolean isRleEnabled;

    private final TezOffsetRecord tezOffsetRecord;

    private int currentKeyLength;
    private int currentValueLength;
    private boolean eof = false;
    private int originalKeyLength;
    private byte[] keyBytes = new byte[0];  // backing array for TezRawDataBuffer in readRawKey()

    // for reporting errors
    private long bytesRead = 0;
    private int recNo = 1;

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
      this(in, length, codec, readsCounter, bytesReadCounter,
          readAhead, readAheadLength, taskContext, null);
    }

    public Reader(InputStream in, long length,
                  CompressionCodec codec,
                  TezCounter readsCounter, TezCounter bytesReadCounter,
                  boolean readAhead, int readAheadLength,
                  DecompressorPool taskContext,
                  TezOffsetRecord tezOffsetRecord) throws IOException {
      this(in, length - HEADER.length, codec,
          readsCounter, bytesReadCounter, readAhead, readAheadLength,
          taskContext, readHeaderFlag(in), tezOffsetRecord);
      assert in != null;
      if (bytesReadCounter != null) {
        bytesReadCounter.increment(IFile.HEADER.length);
      }
    }

    /**
     * Construct an IFile Reader.
     *
     * @param in   The input stream
     * @param length Length of the data in the stream, including the checksum bytes.
     * @param codec codec
     * @param readsCounter Counter for records read from disk
     * @throws IOException
     */
    private Reader(InputStream in, long length,
                  CompressionCodec codec,
                  TezCounter readsCounter, TezCounter bytesReadCounter,
                  boolean readAhead, int readAheadLength,
                  DecompressorPool taskContext, byte headerFlag,
                  TezOffsetRecord tezOffsetRecord) throws IOException {
      assert in != null;
      boolean isCompressed = (headerFlag & FLAG_COMPRESSED) != 0;
      boolean isRleEnabled = (headerFlag & FLAG_RLE_ENABLED) != 0;

      this.readRecordsCounter = readsCounter;
      this.bytesReadCounter = bytesReadCounter;

      checksumIn = new IFileInputStream(in, length, readAhead, readAheadLength/* , isCompressed */);
      if (isCompressed && codec != null) {
        assert taskContext != null;
        this.codec = codec;
        this.taskContext = taskContext;
        decompressor = taskContext.getDecompressor(codec);
        if (decompressor != null) {
          this.in = CodecUtils.getDecompressedInputStreamWithBufferSize(
              codec, checksumIn, decompressor, (int)Math.min(length, Integer.MAX_VALUE));
        } else {
          LOG.warn("Could not obtain decompressor from CodecPool");
          this.in = checksumIn;
        }
      } else {
        this.in = checksumIn;
      }
      startPos = checksumIn.getPosition();

      this.fileLength = length;
      this.isRleEnabled = isRleEnabled;

      assert !(tezOffsetRecord != null) || !isRleEnabled;
      this.tezOffsetRecord = tezOffsetRecord;
    }

    /**
     * Read entire ifile content to memory.
     *
     * @param buffer
     * @param sourceIn
     * @param compressedLength
     * @param codec
     * @param ifileReadAhead
     * @param ifileReadAheadLength
     * @throws IOException
     */
    public static void readToMemory(byte[] buffer, InputStream sourceIn, int compressedLength,
        CompressionCodec codec, boolean ifileReadAhead, int ifileReadAheadLength,
        TaskContext taskContext, boolean useThreadLocalDecompressor)
        throws IOException {
      byte[] header = new byte[HEADER.length];
      byte headerFlag = readHeader(sourceIn, header);
      boolean isCompressed = (headerFlag & FLAG_COMPRESSED) != 0;
      System.arraycopy(header, 0, buffer, 0, HEADER.length);
      int checksumInLength = compressedLength - IFile.HEADER.length;
      IFileInputStream checksumIn = new IFileInputStream(
          sourceIn, checksumInLength, ifileReadAhead, ifileReadAheadLength);
      InputStream in = checksumIn;
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
          in = CodecUtils.getDecompressedInputStreamWithBufferSize(
              codec, checksumIn, decompressor, checksumInLength);
        } else {
          LOG.warn("Could not obtain decompressor from CodecPool");
          in = checksumIn;
        }
      }
      try {
        IOUtils.readFully(in, buffer, IFile.HEADER.length,
            buffer.length - IFile.HEADER.length);
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
        if (in != null) {
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
      final int checksumSize = IFileOutputStream.CHECKSUM_SIZE;
      byte[] buf = new byte[BYTES_TO_READ + checksumSize];

      // copy the IFile header
      if (length < HEADER.length + checksumSize) {
        throw new IOException("Missing IFile header/checksum");
      }
      byte[] header = new byte[HEADER.length];
      readHeader(in, header);
      System.arraycopy(header, 0, buf, 0, HEADER.length);
      out.write(buf, 0, HEADER.length);
      long bytesLeft = length - HEADER.length;
      IFileInputStream ifInput = new IFileInputStream(
          in, bytesLeft, ifileReadAhead, ifileReadAheadLength);
      while (bytesLeft > 0) {
        long dataBytesLeft = bytesLeft - checksumSize;
        int bytesToRead = dataBytesLeft <= BYTES_TO_READ ? (int) bytesLeft : BYTES_TO_READ;
        int n = ifInput.readWithChecksum(buf, 0, bytesToRead);
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

    /**
     * Read up to len bytes into buf starting at offset off.
     *
     * @param buf buffer
     * @param len length of buffer
     * @return the no. of bytes read
     * @throws IOException
     */
    private int readData(byte[] buf, int len) throws IOException {
      int bytesRead = copyFromReadBuffer(buf, 0, len);
      while (bytesRead < len) {
        int n = readFromInput(buf, bytesRead, len - bytesRead);
        if (n < 0) {
          return bytesRead;
        }
        bytesRead += n;
      }
      return len;
    }

    private int readFromInput(byte[] buf, int offset, int len) throws IOException {
      return IOUtils.wrappedReadForCompressedData(in, buf, offset, len);
    }

    private int copyFromReadBuffer(byte[] buf, int offset, int len) {
      int bytesToCopy = Math.min(len, readBufferLimit - readBufferPos);
      if (bytesToCopy > 0) {
        System.arraycopy(readBuffer, readBufferPos, buf, offset, bytesToCopy);
        readBufferPos += bytesToCopy;
      }
      return bytesToCopy;
    }

    private void ensureReadBuffer(int len) throws IOException {
      final int availableInReadBuffer = readBufferLimit - readBufferPos;
      if (availableInReadBuffer >= len) {
        return;
      }
      if (availableInReadBuffer > 0) {
        System.arraycopy(readBuffer, readBufferPos, readBuffer, 0, availableInReadBuffer);
      }
      readBufferPos = 0;
      readBufferLimit = availableInReadBuffer;
      while (readBufferLimit < len) {
        int n = readFromInput(readBuffer, readBufferLimit, readBuffer.length - readBufferLimit);
        if (n < 0) {
          throw new IOException(String.format(INCOMPLETE_READ, len, readBufferLimit));
        }
        readBufferLimit += n;
      }
    }

    private int readInt() throws IOException {
      ensureReadBuffer(Integer.BYTES);
      int value = FastByteComparisons.theUnsafe.getInt(readBuffer,
          FastByteComparisons.BYTE_ARRAY_BASE_OFFSET + (long) readBufferPos);
      readBufferPos += Integer.BYTES;
      return value;
    }

    private long readLong() throws IOException {
      ensureReadBuffer(Long.BYTES);
      long value = FastByteComparisons.theUnsafe.getLong(readBuffer,
          FastByteComparisons.BYTE_ARRAY_BASE_OFFSET + (long) readBufferPos);
      readBufferPos += Long.BYTES;
      return value;
    }

    private void readValueLengthRle() throws IOException {
      currentValueLength = readInt();
      bytesRead += INT_SIZE;
      if (currentValueLength == V_END_MARKER) {
        readKeyValueLengthRle();
      }
    }

    private void readKeyValueLengthNoRle() throws IOException {
      if (tezOffsetRecord != null) {
        readKeyValueLengthNoRleWithTezOffsetRecord();
      } else {
        long combined = readLong();
        currentKeyLength = (int) combined;
        currentValueLength = (int) (combined >>> 32);
        bytesRead += INT_SIZE + INT_SIZE;
      }
      originalKeyLength = currentKeyLength;
    }

    private void readKeyValueLengthNoRleWithTezOffsetRecord() throws IOException {
      int recordOffset = (int) bytesRead;

      if (recordOffset == tezOffsetRecord.getEofPos()) {
        long combined = readLong();
        currentKeyLength = (int) combined;
        currentValueLength = (int) (combined >>> 32);
        bytesRead += INT_SIZE + INT_SIZE;
        return;
      }

      int firstKeyOffset = tezOffsetRecord.getFirstKeyOffset();
      int firstValOffset = tezOffsetRecord.getFirstValOffset();
      int maxKeyLen = tezOffsetRecord.getMaxKeyLen();
      int maxValLen = tezOffsetRecord.getMaxValLen();

      boolean readKeyLength = recordOffset >= firstKeyOffset;
      boolean readValueLength = recordOffset >= firstValOffset;
      if (readKeyLength && readValueLength) {
        long combined = readLong();
        currentKeyLength = (int) combined;
        currentValueLength = (int) (combined >>> 32);
        bytesRead += INT_SIZE + INT_SIZE;
      } else {
        if (readKeyLength) {
          currentKeyLength = readInt();
          bytesRead += INT_SIZE;
        } else {
          currentKeyLength = maxKeyLen;
        }
        if (readValueLength) {
          currentValueLength = readInt();
          bytesRead += INT_SIZE;
        } else {
          currentValueLength = maxValLen;
        }
      }
    }

    private void readKeyValueLengthRle() throws IOException {
      long combined = readLong();
      currentKeyLength = (int) combined;
      currentValueLength = (int) (combined >>> 32);

      if (currentKeyLength != RLE_MARKER) {
        // original key length
        originalKeyLength = currentKeyLength;
      }
      bytesRead += INT_SIZE + INT_SIZE;
    }

    private boolean positionToNextRecordNoRle() throws IOException {
      // Sanity check
      if (eof) {
        throw new IOException(String.format("Reached EOF. Completed reading %d", bytesRead));
      }
      int prevKeyLength = currentKeyLength;

      readKeyValueLengthNoRle();

      // Check for EOF
      if (currentKeyLength == EOF_MARKER && currentValueLength == EOF_MARKER) {
        eof = true;
        return false;
      }

      // Sanity check
      if (currentKeyLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative key-length: " +
                              currentKeyLength + " PreviousKeyLen: " + prevKeyLength);
      }
      if (currentValueLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative value-length: " +
                              currentValueLength);
      }
      return true;
    }

    private boolean positionToNextRecordRle() throws IOException {
      // Sanity check
      if (eof) {
        throw new IOException(String.format("Reached EOF. Completed reading %d", bytesRead));
      }
      int prevKeyLength = currentKeyLength;

      if (prevKeyLength == RLE_MARKER) {
        // Same key as previous one. Just read value length alone
        readValueLengthRle();
      } else {
        readKeyValueLengthRle();
      }

      // Check for EOF
      if (currentKeyLength == EOF_MARKER && currentValueLength == EOF_MARKER) {
        eof = true;
        return false;
      }

      // Sanity check
      boolean isAllowedNegativeKeyLength = currentKeyLength == RLE_MARKER;
      if (!isAllowedNegativeKeyLength && currentKeyLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative key-length: " +
                              currentKeyLength + " PreviousKeyLen: " + prevKeyLength);
      }
      if (currentValueLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative value-length: " +
                              currentValueLength);
      }
      return true;
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


    @Override
    public boolean isCurrentRecordStable() {
      return false;
    }

    public KeyState readRawKey(TezRawDataBuffer key) throws IOException {
      if (isRleEnabled) {
        return readRawKeyRle(key);
      } else {
        return readRawKeyNoRle(key);
      }
    }

    private KeyState readRawKeyNoRle(TezRawDataBuffer key) throws IOException {
      if (!positionToNextRecordNoRle()) {
        return KeyState.NO_KEY;
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

    private KeyState readRawKeyRle(TezRawDataBuffer key) throws IOException {
      if (!positionToNextRecordRle()) {
        return KeyState.NO_KEY;
      }
      if (currentKeyLength == RLE_MARKER) {
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

    public KeyState readRawKey(BytesWritable key) throws IOException {
      if (isRleEnabled) {
        return readRawKeyRle(key);
      } else {
        return readRawKeyNoRle(key);
      }
    }

    private KeyState readRawKeyNoRle(BytesWritable key) throws IOException {
      if (!positionToNextRecordNoRle()) {
        return KeyState.NO_KEY;
      }
      byte[] bytes = key.reinitialize(currentKeyLength);
      int i = readData(bytes, currentKeyLength);

      if (i != currentKeyLength) {
        throw new IOException(String.format(INCOMPLETE_READ, currentKeyLength, i));
      }
      bytesRead += currentKeyLength;
      return KeyState.NEW_KEY;
    }

    private KeyState readRawKeyRle(BytesWritable key) throws IOException {
      if (!positionToNextRecordRle()) {
        return KeyState.NO_KEY;
      }
      if (currentKeyLength == RLE_MARKER) {
        // BytesWritable readers reuse the same key object across records, so on RLE paths
        // the previous key is already present in "key".
        return KeyState.SAME_KEY;
      }
      byte[] bytes = key.reinitialize(currentKeyLength);
      int i = readData(bytes, currentKeyLength);

      if (i != currentKeyLength) {
        throw new IOException(String.format(INCOMPLETE_READ, currentKeyLength, i));
      }
      bytesRead += currentKeyLength;
      return KeyState.NEW_KEY;
    }

    public void nextRawValue(TezRawDataBuffer value) throws IOException {
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

    public void nextRawValue(BytesWritable value) throws IOException {
      byte[] bytes = value.reinitialize(currentValueLength);
      int i = readData(bytes, currentValueLength);

      if (i != currentValueLength) {
        throw new IOException(String.format(INCOMPLETE_READ, currentValueLength, i));
      }
      // Record the bytes read
      bytesRead += currentValueLength;

      ++recNo;
      ++numRecordsRead;
    }

    @Override
    public long consumeAll(KeyValueReaderEdge.ThrowingBiConsumer<BytesWritable, BytesWritable> consumer) throws Exception {
      assert numRecordsRead == 0;   // must not be mixed with nextRawValue()
      BytesWritable key = new BytesWritable();
      BytesWritable value = new BytesWritable();
      if (isRleEnabled) {
        while (readRawKeyRle(key) != KeyState.NO_KEY) {
          nextRawValue(value);
          consumer.accept(key, value);
          numRecordsRead++;
        }
      } else {
        while (readRawKeyNoRle(key) != KeyState.NO_KEY) {
          nextRawValue(value);
          consumer.accept(key, value);
          numRecordsRead++;
        }
      }
      return numRecordsRead;
    }

    private static void verifyHeaderMagic(byte[] header) throws IOException {
      if (!(header[0] == 'T' && header[1] == 'I'
          && header[2] == 'F')) {
        throw new IOException("Not a valid ifile header");
      }
    }

    private static byte readHeader(InputStream in, byte[] header) throws IOException {
      IOUtils.readFully(in, header, 0, HEADER.length);
      verifyHeaderMagic(header);
      return header[3];
    }

    private static byte readHeaderFlag(InputStream in) throws IOException {
      byte[] header = new byte[HEADER.length];
      return readHeader(in, header);
    }

    /**
     * Open IFile data stream after validating header and consuming it.
     * The returned stream reads the payload+EOF marker and validates checksum on close.
     *
     * @param in stream positioned at IFile segment start
     * @param length IFile segment length including header/checksum bytes
     */
    public static IFileInputStream openIFileInputStream(InputStream in, long length,
        boolean readAhead, int readAheadLength) throws IOException {
      byte[] header = new byte[HEADER.length];
      readHeader(in, header);
      return new IFileInputStream(in, length - HEADER.length, readAhead, readAheadLength);
    }

    public void close() throws IOException {
      // Close the underlying stream
      in.close();

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
  }
}
