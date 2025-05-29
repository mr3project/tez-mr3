/*
 * Copyright (c) 2020, DataMonad.
 * All rights reserved.
 */

package org.apache.tez.runtime.library.common.sort.impl;

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest;
import org.apache.hadoop.io.nativeio.NativeIO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedFile;

public class FadvisedChunkedFile extends ChunkedFile {

  private static final Logger LOG = LoggerFactory.getLogger(FadvisedChunkedFile.class);

  private final boolean manageOsCache;
  private final int readaheadLength;
  private final ReadaheadPool readaheadPool;
  private final FileDescriptor fd;
  private final String identifier;

  private ReadaheadRequest readaheadRequest;

  public FadvisedChunkedFile(RandomAccessFile file, long position, long count,
                             int chunkSize, boolean manageOsCache, int readaheadLength,
                             ReadaheadPool readaheadPool, String identifier) throws IOException {
    super(file, position, count, chunkSize);
    this.manageOsCache = manageOsCache;
    this.readaheadLength = readaheadLength;
    this.readaheadPool = readaheadPool;
    this.fd = file.getFD();
    this.identifier = identifier;
  }

  @Override
  public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
    if (manageOsCache && readaheadPool != null) {
      readaheadRequest = readaheadPool
          .readaheadStream(identifier, fd, currentOffset(), readaheadLength,
              endOffset(), readaheadRequest);
    }
    return super.readChunk(ctx);
  }

  @Override
  public void close() throws Exception {
    if (readaheadRequest != null) {
      readaheadRequest.cancel();
    }
    if (manageOsCache && endOffset() - startOffset() > 0) {
      try {
        NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(identifier,
            fd,
            startOffset(), endOffset() - startOffset(),
            POSIX_FADV_DONTNEED);
      } catch (Throwable t) {
        LOG.warn("Failed to manage OS cache for " + identifier, t);
      }
    }
    super.close();
  }
}
