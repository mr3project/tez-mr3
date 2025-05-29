package org.apache.tez.runtime.library.common.sort.impl;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.ssl.SslHandler;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * An OutputStream that grows in fixed-size chunks up to a limit, after which
 * it spills subsequent writes to a local file. Can exceed 2GB without integer overflow.
 */
public class MultiByteArrayOutputStream extends OutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(MultiByteArrayOutputStream.class);

  private final int cacheSize;
  private final int maxNumBuffers;

  private final List<byte[]> buffers = new ArrayList<>();
  private byte[] currentBuffer;
  private int posInBuf = 0;
  private long totalBytes = 0;
  private long bufferBytes = 0;

  // spill fields
  private final FileSystem fs;
  private final TezTaskOutput taskOutput;
  private Path outputPath;
  private FSDataOutputStream fileOut;

  /**
   * @param cacheSize size of each internal byte[] buffer; must be > 0
   * @param maxNumBuffers max number of in-memory buffers before spilling (0 => always spill)
   */
  public MultiByteArrayOutputStream(
      int cacheSize,
      int maxNumBuffers,
      FileSystem fs,
      TezTaskOutput taskOutput) throws IOException {
    if (cacheSize <= 0) {
      throw new IllegalArgumentException("cacheSize must be positive");
    }
    this.cacheSize = cacheSize;
    this.maxNumBuffers = maxNumBuffers;
    this.fs = fs;
    this.taskOutput = taskOutput;

    // if no buffers allowed, immediately create an empty spill file
    if (this.maxNumBuffers == 0) {
      this.outputPath = taskOutput.getOutputFileForWrite();
      this.fileOut = fs.create(outputPath);
    } else {
      // prepare first in-memory buffer
      this.currentBuffer = new byte[cacheSize];
      buffers.add(currentBuffer);
      this.outputPath = null;
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (outputPath != null) {
      // spilled: write straight to file
      fileOut.write(b);
    } else if (posInBuf < cacheSize) {
      // in-memory buffer has space
      currentBuffer[posInBuf++] = (byte) b;
      bufferBytes++;
    } else if (buffers.size() < maxNumBuffers) {
      // allocate a new buffer and write there
      currentBuffer = new byte[cacheSize];
      buffers.add(currentBuffer);
      posInBuf = 0;
      currentBuffer[posInBuf++] = (byte) b;
      bufferBytes++;
    } else {
      // hit buffer limit: spill future writes
      spillIfNeeded();
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
      if (outputPath != null) {
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
      if (buffers.size() < maxNumBuffers) {
        // allocate new in-memory buffer
        currentBuffer = new byte[cacheSize];
        buffers.add(currentBuffer);
        posInBuf = 0;
      } else {
        // spill future writes
        spillIfNeeded();
      }
    }
  }

  /**
   * @return total number of bytes written (may exceed Integer.MAX_VALUE)
   */
  public long size() {
    return totalBytes;
  }

  @Override
  public void flush() throws IOException {
    if (fileOut != null) {
      fileOut.flush();
    }
  }

  @Override
  public void close() throws IOException {
    if (fileOut != null) {
      fileOut.close();
    }
  }

  /**
   * Create the spill file (if needed) and switch future writes to it.
   */
  private void spillIfNeeded() throws IOException {
    assert maxNumBuffers > 0;
    assert posInBuf == cacheSize;
    assert outputPath == null;
    outputPath = taskOutput.getOutputFileForWrite();
    fileOut = fs.create(outputPath);
  }

  // write data to Channel ch atomically
  // no data is written if outputPath cannot be accessed
  // return null if the write operation fails
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

    File spillFile = null;
    RandomAccessFile raf = null;

    // open outputPath here for writing atomically
    if (end > bufferBytes) {
      assert outputPath != null;  // the caller must ensure this invariant
      try {
        spillFile = new File(outputPath.toUri().getPath());
        raf = new RandomAccessFile(spillFile, "r");
      } catch (FileNotFoundException e) {
        LOG.error(outputPath + " not found");
        return null;
      }
    }

    // 1) In-memory buffers
    long bufBase = 0;
    for (int i = 0; i < buffers.size(); i++) {
      int bufLen = (i < buffers.size() - 1) ? cacheSize : posInBuf;
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

        ch.write(ByteBuffer.wrap(buffers.get(i), offsetInBuf, lengthToWrite));
      }

      bufBase += bufLen;
    }

    // 2) Spill file, if any bytes remain in [start,end)
    if (end > bufBase) {
      assert bufBase == bufferBytes;
      assert spillFile != null;
      assert raf != null;

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
      return writeFuture;
    }

    // 3) Nothing left on disk
    return ch.writeAndFlush(Unpooled.EMPTY_BUFFER);
  }
}
