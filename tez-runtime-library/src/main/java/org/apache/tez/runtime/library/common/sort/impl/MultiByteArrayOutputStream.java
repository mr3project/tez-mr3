package org.apache.tez.runtime.library.common.sort.impl;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutput;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * An OutputStream that grows in fixed-size chunks up to a limit, after which
 * it spills subsequent writes to a local file. Can exceed 2GB without integer overflow.
 */
public class MultiByteArrayOutputStream extends OutputStream {
  private final int cacheSize;
  private final int maxNumBuffers;

  private final List<byte[]> buffers = new ArrayList<>();
  private byte[] currentBuffer;
  private int posInBuf = 0;
  private long totalBytes = 0;

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
    } else if (buffers.size() < maxNumBuffers) {
      // allocate a new buffer and write there
      currentBuffer = new byte[cacheSize];
      buffers.add(currentBuffer);
      posInBuf = 0;
      currentBuffer[posInBuf++] = (byte) b;
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
        inputPos += toCopy;
        remaining -= toCopy;
        if (remaining == 0) {
          break;
        }
      }
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
    if (outputPath == null) {
      outputPath = taskOutput.getOutputFileForWrite();
      fileOut = fs.create(outputPath);
    }
  }

  /**
   * Write all collected data (in-memory then spilled file) to the Netty channel.
   * @return a ChannelFuture for the flush
   */
  public ChannelFuture writeData(Channel ch) throws IOException {
    for (int i = 0; i < buffers.size(); i++) {
      int length = (i < buffers.size() - 1) ? cacheSize : posInBuf;
      ch.write(ByteBuffer.wrap(buffers.get(i), 0, length));
    }
    if (outputPath != null) {
      try (FSDataInputStream in = fs.open(outputPath)) {
        byte[] buf = new byte[cacheSize];
        int read;
        while ((read = in.read(buf)) > 0) {
          ch.write(ByteBuffer.wrap(buf, 0, read));
        }
      }
    }
    return ch.writeAndFlush(Unpooled.EMPTY_BUFFER);
  }
}
