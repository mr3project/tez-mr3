package org.apache.tez.runtime.api;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.ssl.SslHandler;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ReadaheadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * An OutputStream that grows in fixed-size chunks up to a limit, after which
 * it spills subsequent writes to a local file. Can exceed 2GB without integer overflow.
 */
public class MultiByteArrayOutputStream extends OutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(MultiByteArrayOutputStream.class);

  // buffer size = 0KB -> 16KB -> 128KB -> 1MB -> 4MB -> 4MB -> ...
  // Cf. gla2025.5.26.pptx
  private static final int MIN_CACHE_SIZE_WRITER = 16 * 1024;   // set to 128 * 1024 to start at 128KB
  private static final int MAX_CACHE_SIZE_WRITER = 4 * 1024 * 1024;
  private static final int CACHE_SIZE_MULTIPLE = 8;
  private static final int MAX_NUM_BUFFERS = 64;

  public static boolean canUseFreeMemoryBuffers(long freeMemoryThreshold) {
    long currentFreeMemory = Runtime.getRuntime().freeMemory();
    return currentFreeMemory > freeMemoryThreshold;
  }

  private int cacheSize;
  private byte[] currentBuffer;
  private int posInBuf;

  private List<byte[]> buffers = new ArrayList<>();

  private long totalBytes = 0;
  private long bufferBytes = 0;

  // spill fields
  private final FileSystem fs;
  private final Path outputPath;
  private FSDataOutputStream fileOut;   // set when creating a spill file

  public MultiByteArrayOutputStream(
      FileSystem fs,
      Path outputPath) {
    // start with an empty byte[] buffer because no data might be written
    this.cacheSize = 0;   // set to the size of currentBuffer
    this.currentBuffer = null;
    this.posInBuf = 0;
    // posInBuf == cacheSize if currentBuffer is full, so currentBuffer is initially considered full

    this.fs = fs;
    this.outputPath = outputPath;
    this.fileOut = null;
  }

  @Override
  public void write(int b) throws IOException {
    if (fileOut != null) {
      // spilled: write straight to file
      fileOut.write(b);
    } else if (posInBuf < cacheSize) {
      // in-memory buffer has space
      currentBuffer[posInBuf++] = (byte) b;
      bufferBytes++;
    } else if (buffers.size() < MAX_NUM_BUFFERS) {
      allocateNewBuffer();
      currentBuffer[posInBuf++] = (byte) b;
      bufferBytes++;
    } else {
      // hit buffer limit: spill future writes
      spillToFile();
      fileOut.write(b);
    }
    totalBytes++;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }
    int remaining = len;
    int inputPos = off;
    while (remaining > 0) {
      if (fileOut != null) {
        // after spill: write all remaining to file
        fileOut.write(b, inputPos, remaining);
        totalBytes += remaining;
        break;
      }
      int space = cacheSize - posInBuf;
      if (space > 0) {
        int toCopy = Math.min(space, remaining);
        System.arraycopy(b, inputPos, currentBuffer, posInBuf, toCopy);
        posInBuf += toCopy;
        totalBytes += toCopy;
        bufferBytes += toCopy;
        inputPos += toCopy;
        remaining -= toCopy;
        if (remaining == 0) {
          break;
        }
      }
      // remaining > 0;
      if (buffers.size() < MAX_NUM_BUFFERS) {
        allocateNewBuffer();
      } else {
        // spill future writes
        spillToFile();
      }
    }
  }

  // allocate a new buffer
  private void allocateNewBuffer() {
    assert posInBuf == cacheSize;
    if (cacheSize == 0) {
      cacheSize = MIN_CACHE_SIZE_WRITER;
    } else {
      cacheSize = Math.min(cacheSize * CACHE_SIZE_MULTIPLE, MAX_CACHE_SIZE_WRITER);
    }
    currentBuffer = new byte[cacheSize];
    buffers.add(currentBuffer);
    posInBuf = 0;
  }

  /**
   * Create the spill file (if needed) and switch future writes to it.
   */
  private void spillToFile() throws IOException {
    assert posInBuf == cacheSize;
    assert fileOut == null;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating fileOut: {}", outputPath);
    }
    fileOut = fs.create(outputPath);
    // bufferBytes is never updated again
  }

  @Override
  public void flush() throws IOException {
    if (fileOut != null) {
      fileOut.flush();
    }
  }

  // 1. called by LogicalOutput threads after filling the contents of this buffer
  // after calling close(), no more writes should be made
  @Override
  synchronized public void close() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Closing: totalBytes={}, bufferBytes={}, outputPath={}", totalBytes, bufferBytes, outputPath);
    }
    if (fileOut != null) {
      fileOut.close();
    }
  }

  // write data to Channel ch atomically
  // no data is written if outputPath cannot be accessed
  // return null if the write operation fails
  // 2. called from ShuffleHandler threads
  // do not use synchronized to allow concurrent access
  public ChannelFuture writeData(
      long rangeOffset,
      long rangePartLength,
      Channel ch,
      boolean manageOsCache,
      int readaheadLength,
      int shuffleBufferSize,
      boolean shuffleTransferToAllowed,
      int sslFileBufferSize,
      ReadaheadPool readaheadPool) throws IOException {

    if (rangePartLength <= 0) {
      return ch.write(Unpooled.EMPTY_BUFFER);
    }

    // global window: [start, end)
    final long start = rangeOffset;
    final long end   = rangeOffset + rangePartLength;

    List<byte[]> buffersFinal;
    int posInBufFinal;
    long bufferBytesFinal;
    synchronized (this) {
      buffersFinal = buffers;
      posInBufFinal = posInBuf;
      bufferBytesFinal = bufferBytes;
    }

    // buffers[] can be null in rare cases where speculative fetchers are created, e.g.,
    //   1. A fetcher gets stuck while writing the header, while holding a reference to MultiByteArrayOutputStream.
    //   2. A speculative fetcher is created and the task is completed, and later clean() is called.
    //   3. Later the original fetcher resumes and find 'buffers == null'.
    if (buffersFinal == null) {
      LOG.error("Cleaned while writing: outputPath={}", outputPath);
      return null;
    }

    // 1) In-memory buffers
    ChannelFuture writeFuture = null;
    long bufBase = 0;
    for (int i = 0; i < buffersFinal.size(); i++) {
      byte[] bufferElement = buffersFinal.get(i);
      int bufLen = (i < buffersFinal.size() - 1) ? bufferElement.length : posInBufFinal;
      long bufStart = bufBase;
      long bufEnd   = bufBase + bufLen;
      // this buffer occupies: [bufStart, bufEnd)

      // once this buffer starts at or beyond 'end', no further overlap is possible
      if (bufStart >= end) {
        break;
      }

      // now we know bufStart < end, so checking start < bufEnd is enough
      if (start < bufEnd) {
        long overlapStart = Math.max(start, bufStart);
        long overlapEnd   = Math.min(end,   bufEnd);
        int offsetInBuf   = (int)(overlapStart - bufStart);
        int lengthToWrite = (int)(overlapEnd   - overlapStart);
        assert lengthToWrite > 0;

        writeFuture = ch.write(Unpooled.wrappedBuffer(bufferElement, offsetInBuf, lengthToWrite));
      }

      bufBase += bufLen;
    }

    // 2) Spill file, if any bytes remain in [start,end)
    if (end > bufBase) {
      assert bufBase == bufferBytesFinal;

      File spillFile = null;
      RandomAccessFile raf = null;
      try {
        spillFile = new File(outputPath.toUri().getPath());
        raf = new RandomAccessFile(spillFile, "r");

        long fileOffset  = Math.max(0, start - bufBase);
        long filePartLen = end - Math.max(start, bufBase);

        if (ch.pipeline().get(SslHandler.class) == null) {
          FadvisedFileRegion region = new FadvisedFileRegion(
              raf, fileOffset, filePartLen,
              manageOsCache, readaheadLength, readaheadPool,
              spillFile.getAbsolutePath(),
              shuffleBufferSize, shuffleTransferToAllowed);
          writeFuture = ch.write(region);
        } else {
          FadvisedChunkedFile chunk = new FadvisedChunkedFile(
              raf, fileOffset, filePartLen, sslFileBufferSize,
              manageOsCache, readaheadLength, readaheadPool,
              spillFile.getAbsolutePath());
          writeFuture = ch.write(chunk);
        }

        final RandomAccessFile rafFinal = raf;
        writeFuture.addListener(future -> {
          try {
            rafFinal.close();
          } catch (IOException ignored) { }
        });
        raf = null;   // the listener is responsible for closing raf.
        return writeFuture;
      } finally {
        // if accessing raf fails, we close it here
        if (raf != null) {
          try {
            raf.close();
          } catch (IOException ignored) { }
        }
      }
    }

    // 3) Nothing left on disk
    if (writeFuture != null) {
      return writeFuture;
    }

    return ch.write(Unpooled.EMPTY_BUFFER);
  }

  // Invariant:
  //   1. called only after close() is called
  //   2. on InputStream returned, close() is eventually called.
  //      In the current implementation, Segment.close() eventually calls InputStream.close() in TezMerger.
  // The returned stream reads bytes in [offset, offset + length).
  public InputStream createInputStreamFrom(long offset, long length) throws IOException {
    List<byte[]> buffersFinal;
    int posInBufFinal;
    long bufferBytesFinal;
    long totalBytesFinal;

    // TODO: synchronized() is unnecessary because createInputStreamFrom() is called in the same thread that calls close()
    synchronized (this) {
      buffersFinal = buffers;
      posInBufFinal = posInBuf;
      bufferBytesFinal = bufferBytes;
      totalBytesFinal = totalBytes;
    }

    assert buffersFinal != null;  // because createInputStreamFrom() is called before clean()

    if (offset < 0 || length < 0 || offset > totalBytesFinal || length > totalBytesFinal - offset) {
      throw new IndexOutOfBoundsException(String.format(
          "Invalid range: offset=%d, length=%d, totalBytes=%d", offset, length, totalBytesFinal));
    }

    List<byte[]> memoryBuffers = new ArrayList<>(buffersFinal.size());
    int numNonEmptyBuffers = 0;
    for (int i = 0; i < buffersFinal.size(); i++) {
      byte[] bufferElement = buffersFinal.get(i);
      int bufferLength = (i < buffersFinal.size() - 1) ? bufferElement.length : posInBufFinal;
      if (bufferLength > 0) {
        numNonEmptyBuffers++;
      }
    }

    int[] memoryBufferLengths = new int[numNonEmptyBuffers];
    int outIndex = 0;
    for (int i = 0; i < buffersFinal.size(); i++) {
      byte[] bufferElement = buffersFinal.get(i);
      int bufferLength = (i < buffersFinal.size() - 1) ? bufferElement.length : posInBufFinal;
      if (bufferLength > 0) {
        memoryBuffers.add(bufferElement);
        memoryBufferLengths[outIndex++] = bufferLength;
      }
    }

    return new MultiBufferRangeInputStream(
        memoryBuffers,
        memoryBufferLengths,
        bufferBytesFinal,
        totalBytesFinal,
        offset,
        length);
  }

  private final class MultiBufferRangeInputStream extends InputStream {
    private final List<byte[]> memoryBuffers;
    private final int[] memoryBufferLengths;
    private final long memoryBytes;
    private final long endPos;

    private long globalPos;
    private int memoryIndex;
    private int offsetInMemoryBuffer;
    private FSDataInputStream spillIn;
    private boolean closed;

    private MultiBufferRangeInputStream(
        List<byte[]> memoryBuffers,
        int[] memoryBufferLengths,
        long memoryBytes,
        long totalBytes,
        long offset,
        long length) {
      this.memoryBuffers = memoryBuffers;
      this.memoryBufferLengths = memoryBufferLengths;
      this.memoryBytes = memoryBytes;
      this.globalPos = offset;
      this.endPos = offset + length;

      this.memoryIndex = 0;
      this.offsetInMemoryBuffer = 0;

      long memoryStartPos = Math.min(offset, memoryBytes);
      long scanned = 0;
      while (memoryIndex < memoryBufferLengths.length) {
        int currentLen = memoryBufferLengths[memoryIndex];
        if (scanned + currentLen > memoryStartPos) {
          offsetInMemoryBuffer = (int) (memoryStartPos - scanned);
          break;
        }
        scanned += currentLen;
        memoryIndex++;
      }
      if (memoryIndex >= memoryBufferLengths.length) {
        offsetInMemoryBuffer = 0;
      }

      assert endPos <= totalBytes;
    }

    @Override
    public int read() throws IOException {
      if (closed) {
        throw new IOException("Stream closed");
      }
      if (globalPos >= endPos) {
        return -1;
      }

      if (globalPos < memoryBytes) {
        while (memoryIndex < memoryBuffers.size()) {
          int curLen = memoryBufferLengths[memoryIndex];
          if (offsetInMemoryBuffer < curLen) {
            int result = memoryBuffers.get(memoryIndex)[offsetInMemoryBuffer] & 0xFF;
            offsetInMemoryBuffer++;
            globalPos++;
            return result;
          }
          memoryIndex++;
          offsetInMemoryBuffer = 0;
        }
      }

      ensureSpillOpen();
      int result = spillIn.read();
      if (result < 0) {
        throw new EOFException(String.format(
            "Unexpected EOF while reading spill file: outputPath=%s, position=%d", outputPath, globalPos));
      }
      globalPos++;
      return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (closed) {
        throw new IOException("Stream closed");
      }
      if (b == null) {
        throw new NullPointerException("b");
      }
      if (off < 0 || len < 0 || len > b.length - off) {
        throw new IndexOutOfBoundsException();
      }
      if (len == 0) {
        return 0;
      }
      if (globalPos >= endPos) {
        return -1;
      }

      int copied = 0;
      while (len > 0 && globalPos < endPos) {
        if (globalPos < memoryBytes) {
          if (memoryIndex >= memoryBuffers.size()) {
            break;
          }
          byte[] cur = memoryBuffers.get(memoryIndex);
          int curLen = memoryBufferLengths[memoryIndex];
          int availableInCur = curLen - offsetInMemoryBuffer;
          if (availableInCur <= 0) {
            memoryIndex++;
            offsetInMemoryBuffer = 0;
            continue;
          }

          int toCopy = (int) Math.min(Math.min((long) len, endPos - globalPos), availableInCur);
          System.arraycopy(cur, offsetInMemoryBuffer, b, off, toCopy);

          off += toCopy;
          len -= toCopy;
          copied += toCopy;
          globalPos += toCopy;
          offsetInMemoryBuffer += toCopy;
        } else {
          ensureSpillOpen();
          int toRead = (int) Math.min((long) len, endPos - globalPos);
          int n = spillIn.read(b, off, toRead);
          if (n < 0) {
            throw new EOFException(String.format(
                "Unexpected EOF while reading spill file: outputPath=%s, position=%d", outputPath, globalPos));
          }
          off += n;
          len -= n;
          copied += n;
          globalPos += n;
        }
      }

      return copied == 0 ? -1 : copied;
    }

    @Override
    public long skip(long n) throws IOException {
      if (n <= 0) {
        return 0;
      }
      long toSkip = Math.min(n, endPos - globalPos);
      if (toSkip <= 0) {
        return 0;
      }

      long skipped = 0;
      if (globalPos < memoryBytes) {
        while (toSkip > 0 && globalPos < Math.min(memoryBytes, endPos)) {
          if (memoryIndex >= memoryBufferLengths.length) {
            break;
          }
          int curLen = memoryBufferLengths[memoryIndex];
          int availableInCur = curLen - offsetInMemoryBuffer;
          if (availableInCur <= 0) {
            memoryIndex++;
            offsetInMemoryBuffer = 0;
            continue;
          }
          int jump = (int) Math.min((long) availableInCur, toSkip);
          offsetInMemoryBuffer += jump;
          globalPos += jump;
          toSkip -= jump;
          skipped += jump;
        }
      }

      if (toSkip > 0 && globalPos < endPos) {
        ensureSpillOpen();
        long targetPosInSpill = (globalPos - memoryBytes) + toSkip;
        spillIn.seek(targetPosInSpill);
        globalPos += toSkip;
        skipped += toSkip;
      }

      return skipped;
    }

    @Override
    public int available() {
      long remaining = endPos - globalPos;
      return (int) Math.min(Integer.MAX_VALUE, Math.max(remaining, 0));
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      closed = true;
      if (spillIn != null) {
        spillIn.close();
      }
    }

    private void ensureSpillOpen() throws IOException {
      if (spillIn == null) {
        spillIn = fs.open(outputPath);
        spillIn.seek(Math.max(0, globalPos - memoryBytes));
      }
    }
  }

  // 3. called from ShuffleHandlerDaemonProcessor thread
  synchronized public void clean() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cleaning: outputPath={}", outputPath);
    }
    buffers = null;
    currentBuffer = null;
    // do not delete fileOut because it will be deleted after the source DAG or Vertex is finished
  }

  synchronized public long getTotalBytes() {
    return totalBytes;
  }
}
