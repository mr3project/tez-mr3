/*
 * Copyright (c) 2020, DataMonad.
 * All rights reserved.
 */

package org.apache.tez.runtime.api;

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest;
import org.apache.hadoop.io.nativeio.NativeIO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;

import io.netty.channel.DefaultFileRegion;

public class FadvisedFileRegion extends DefaultFileRegion {

  private static final Logger LOG = LoggerFactory.getLogger(FadvisedFileRegion.class);

  private final boolean manageOsCache;
  private final int readaheadLength;
  private final ReadaheadPool readaheadPool;
  private final FileDescriptor fd;
  private final String identifier;
  private final long count;
  private final long position;
  private final int shuffleBufferSize;
  private final boolean shuffleTransferToAllowed;
  private final FileChannel fileChannel;

  private ReadaheadRequest readaheadRequest;
  private boolean transferred = false;

  public FadvisedFileRegion(RandomAccessFile file, long position, long count,
                            boolean manageOsCache, int readaheadLength, ReadaheadPool readaheadPool,
                            String identifier, int shuffleBufferSize,
                            boolean shuffleTransferToAllowed) throws IOException {
    super(file.getChannel(), position, count);
    this.manageOsCache = manageOsCache;
    this.readaheadLength = readaheadLength;
    this.readaheadPool = readaheadPool;
    this.fd = file.getFD();
    this.identifier = identifier;
    this.fileChannel = file.getChannel();
    this.count = count;
    this.position = position;
    this.shuffleBufferSize = shuffleBufferSize;
    this.shuffleTransferToAllowed = shuffleTransferToAllowed;
  }

  @Override
  public long transferTo(WritableByteChannel target, long position)
      throws IOException {
    if (readaheadPool != null && readaheadLength > 0) {
      readaheadRequest = readaheadPool.readaheadStream(identifier, fd,
          position() + position, readaheadLength,
          position() + count(), readaheadRequest);
    }

    long written = 0;
    if(this.shuffleTransferToAllowed) {
      written = super.transferTo(target, position);
    } else {
      written = customShuffleTransfer(target, position);
    }
    /*
     * At this point, we can assume that the transfer was successful.
     */
    transferred = true;
    return written;
  }

  /**
   * Since Netty4, deallocate() is called automatically during cleanup, but before the
   * ChannelFutureListeners. Deallocate calls FileChannel.close() and makes the file descriptor
   * invalid, so every OS cache operation (e.g. posix_fadvice) with the original file descriptor
   * will fail after this operation, so we need to take care of cleanup operations here (before
   * deallocating) instead of listeners outside.
   */
  @Override
  protected void deallocate() {
    if (readaheadRequest != null) {
      readaheadRequest.cancel();
    }

    if (transferred) {
      transferSuccessful();
    }
    super.deallocate();
  }

  /**
   * This method transfers data using local buffer. It transfers data from
   * a disk to a local buffer in memory, and then it transfers data from the
   * buffer to the target. This is used only if transferTo is disallowed in
   * the configuration file. super.TransferTo does not perform well on Windows
   * due to a small IO request generated. customShuffleTransfer can control
   * the size of the IO requests by changing the size of the intermediate
   * buffer.
   */
  long customShuffleTransfer(WritableByteChannel target, long position)
      throws IOException {
    long actualCount = this.count - position;
    if (actualCount < 0 || position < 0) {
      throw new IllegalArgumentException(
          "position out of range: " + position +
              " (expected: 0 - " + (this.count - 1) + ')');
    }
    if (actualCount == 0) {
      return 0L;
    }

    long trans = actualCount;
    int readSize;
    ByteBuffer byteBuffer = ByteBuffer.allocate(this.shuffleBufferSize);

    while(trans > 0L &&
        (readSize = fileChannel.read(byteBuffer, this.position+position)) > 0) {
      //adjust counters and buffer limit
      if(readSize < trans) {
        trans -= readSize;
        position += readSize;
        byteBuffer.flip();
      } else {
        //We can read more than we need if the actualCount is not multiple
        //of the byteBuffer size and file is big enough. In that case we cannot
        //use flip method but we need to set buffer limit manually to trans.
        byteBuffer.limit((int)trans);
        byteBuffer.position(0);
        position += trans;
        trans = 0;
      }

      //write data to the target
      while(byteBuffer.hasRemaining()) {
        target.write(byteBuffer);
      }

      byteBuffer.clear();
    }

    return actualCount - trans;
  }

  /**
   * Call when the transfer completes successfully so we can advise the OS that
   * we don't need the region to be cached anymore.
   */
  public void transferSuccessful() {
    if (manageOsCache && count() > 0) {
      try {
        if (fd.valid()) {
          NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(identifier, fd, position(),
              count(), POSIX_FADV_DONTNEED);
        } else {
          LOG.debug("File descriptor is not valid anymore, skipping posix_fadvise: " + identifier);
        }
      } catch (Throwable t) {
        LOG.warn("Failed to manage OS cache for " + identifier, t);
      }
    }
  }
}
