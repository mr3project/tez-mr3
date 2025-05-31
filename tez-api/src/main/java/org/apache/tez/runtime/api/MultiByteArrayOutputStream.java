package org.apache.tez.runtime.api;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.ssl.SslHandler;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ReadaheadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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

  // buffer size = 0KB -> 1KB -> 10KB -> 100KB -> 1MB -> 4MB -> 4MB
  private static final int MIN_CACHE_SIZE_WRITER = 1 * 1024;
  private static final int MAX_CACHE_SIZE_WRITER = 4 * 1024 * 1024;
  private static final int CACHE_SIZE_MULTIPLE = 10;
  private static final int MAX_NUM_BUFFERS = 256;

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
    LOG.info("Creating fileOut: {}", outputPath);
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
    LOG.info("Closing: totalBytes={}, bufferBytes={}, outputPath={}", totalBytes, bufferBytes, outputPath);
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
      return ch.writeAndFlush(Unpooled.EMPTY_BUFFER);
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

    // 1) In-memory buffers
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

        ch.write(Unpooled.wrappedBuffer(bufferElement, offsetInBuf, lengthToWrite));
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

        ChannelFuture writeFuture;
        if (ch.pipeline().get(SslHandler.class) == null) {
          FadvisedFileRegion region = new FadvisedFileRegion(
              raf, fileOffset, filePartLen,
              manageOsCache, readaheadLength, readaheadPool,
              spillFile.getAbsolutePath(),
              shuffleBufferSize, shuffleTransferToAllowed);
          writeFuture = ch.writeAndFlush(region);
        } else {
          FadvisedChunkedFile chunk = new FadvisedChunkedFile(
              raf, fileOffset, filePartLen, sslFileBufferSize,
              manageOsCache, readaheadLength, readaheadPool,
              spillFile.getAbsolutePath());
          writeFuture = ch.writeAndFlush(chunk);
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
    return ch.writeAndFlush(Unpooled.EMPTY_BUFFER);
  }

  // 3. called from ShuffleHandlerDaemonProcessor thread
  synchronized public void clean() {
    buffers = null;
    currentBuffer = null;
    // do not delete fileOut because it will be deleted after the source DAG or Vertex is finished
  }

  synchronized public long getTotalBytes() {
    return totalBytes;
  }
}
