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
   * For basic cache size checks: header + one section checksum trailer.
   *
   * @return size of the base cache needed
   */
  static int getBaseCacheSize() {
    return (HEADER.length + checksumSize);
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
      this.totalSize = HEADER.length + (3 * checksumSize);
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
      finishValuesSectionForSpill();

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

      resetValuesSectionOutput(fileCodec);

      // Write header to file
      headerWritten = false;
      writeHeader(newRawOut);

      // write values section payload produced so far (without header and checksum)
      int sPos = HEADER.length;
      int len = (bout.size() - checksumSize - HEADER.length);
      if (len > 0) {
        bufferWriteBytes(bout.getBuffer(), sPos, len);
      }

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
    // Values section output (written during append):
    // rawOut <-- valuesChecksumOut <-- valuesCompressedOut <-- valuesOut <-- [writeBuffer]
    protected DataOutputStream valuesOut;
    private long start = 0;

    private CompressionOutputStream valuesCompressedOut;
    private Compressor valuesCompressor;
    private boolean compressOutput = false;

    private IFileOutputStream valuesChecksumOut;
    private final CompressionCodec codec;

    protected FSDataOutputStream rawOut;
    // true iff this Writer created and owns rawOut
    // if true, close() closes rawOut.
    protected boolean ownOutputStream = false;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private long decompressedBytesWritten = HEADER.length;
    private long compressedBytesWritten = 0;
    private long logicalValueBytesWritten = 0;
    private long logicalKeyBytesWritten = 0;
    private long logicalLengthBytesWritten = 0;

    // Count records written to disk
    private long numRecordsWritten = 0;
    private final TezCounter writtenRecordsCounter;
    private final TezCounter serializedUncompressedBytes;

    private boolean closeSerializers = false;
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
      if (compressOutput && compressorExternal == null && valuesCompressor != null) {
        CodecPool.returnCompressor(valuesCompressor);
      }
      this.valuesOut = null;
      this.valuesCompressedOut = null;
      this.valuesChecksumOut = null;
      this.valuesCompressor = null;
      this.compressOutput = false;
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

      finishValuesSection();
      writeBufferedSection(keySectionBuffer);
      writeBufferedSection(lengthsSectionBuffer);

      // Close the underlying stream iff we own it
      if (ownOutputStream) {
        rawOut.close();
      }
      //header bytes are already included in rawOut
      compressedBytesWritten = rawOut.getPos() - start;

      if (compressOutput) {
        // Return back the compressor
        // if compressorExternal != null, this Writer does not own compressor, so do not return it to CodecPool
        if (compressorExternal == null) {
          // this Writer owns compressor
          CodecPool.returnCompressor(valuesCompressor);
        }
        valuesCompressor = null;
      }

      valuesOut = null;
      if (writtenRecordsCounter != null) {
        writtenRecordsCounter.increment(numRecordsWritten);
      }
      if (isDebugEnabled) {
        LOG.debug("Total records written=" + numRecordsWritten + "; compressedLen=" +
            compressedBytesWritten + "; rawLen=" + decompressedBytesWritten);
      }
    }

    public void append(Object key, Object value) throws IOException {
      keySerializer.serialize(key);
      int keyLength = buffer.getLength();
      assert(keyLength >= 0);

      // Append the 'value'
      valueSerializer.serialize(value);
      int valueLength = buffer.getLength() - keyLength;
      assert(valueLength >= 0);
      writeKVPair(buffer.getData(), 0, keyLength, buffer.getData(),
          keyLength, valueLength);

      // Reset
      buffer.reset();
      ++numRecordsWritten;
    }

    public void append(DataInputBuffer key, DataInputBuffer value) throws IOException {
      int keyLength = key.getLength() - key.getPosition();
      assert(keyLength >= 0);

      int valueLength = value.getLength() - value.getPosition();
      assert(valueLength >= 0);
      writeKVPair(key.getData(), key.getPosition(), keyLength,
          value.getData(), value.getPosition(), valueLength);
      ++numRecordsWritten;
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

      // Update bytes written
      logicalKeyBytesWritten += keyLength;
      logicalValueBytesWritten += valueLength;
      logicalLengthBytesWritten += (2L * INT_SIZE);
      decompressedBytesWritten += keyLength + valueLength + (2L * INT_SIZE);
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

    private void finishValuesSection() throws IOException {
      flushWriteBuffer();
      if (compressOutput) {
        valuesCompressedOut.finish();
        valuesCompressedOut.resetState();
      }
      valuesChecksumOut.finish();
    }

    private void writeBufferedSection(DataOutputBuffer sectionBuffer) throws IOException {
      IFileOutputStream sectionChecksumOut = new IFileOutputStream(rawOut);
      DataOutputStream sectionOut = new DataOutputStream(sectionChecksumOut);
      CompressionOutputStream sectionCompressedOut = null;
      Compressor sectionCompressor = null;
      if (compressOutput) {
        sectionCompressor = CodecUtils.getCompressor(codec);
        if (sectionCompressor == null) {
          throw new IOException("Could not obtain compressor from CodecPool for section write");
        }
        sectionCompressor.reset();
        sectionCompressedOut = CodecUtils.createOutputStream(codec, sectionChecksumOut, sectionCompressor);
        sectionOut = new DataOutputStream(sectionCompressedOut);
      }
      try {
        sectionOut.write(sectionBuffer.getData(), 0, sectionBuffer.getLength());
        sectionOut.flush();
        if (sectionCompressedOut != null) {
          sectionCompressedOut.finish();
          sectionCompressedOut.resetState();
        }
        sectionChecksumOut.finish();
      } finally {
        if (sectionCompressor != null) {
          CodecPool.returnCompressor(sectionCompressor);
        }
      }
    }

    protected long getLogicalValueBytesWritten() {
      return logicalValueBytesWritten;
    }

    protected long getLogicalKeyBytesWritten() {
      return logicalKeyBytesWritten;
    }

    protected long getLogicalLengthBytesWritten() {
      return logicalLengthBytesWritten;
    }

    protected void finishValuesSectionForSpill() throws IOException {
      finishValuesSection();
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

    public enum KeyState {NO_KEY, NEW_KEY}

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

    protected void readKeyValueLength(DataInput dIn) throws IOException {
      currentKeyLength = dIn.readInt();
      currentValueLength = dIn.readInt();
      // long combined = dIn.readLong();
      // currentKeyLength = (int) (combined >> 32);
      // currentValueLength = (int) combined;
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
      readKeyValueLength(dIn);

      // Check for EOF
      if (currentKeyLength == EOF_MARKER && currentValueLength == EOF_MARKER) {
        eof = true;
        return false;
      }

      // Sanity check
      if (currentKeyLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative key-length: " +
                              currentKeyLength);
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
