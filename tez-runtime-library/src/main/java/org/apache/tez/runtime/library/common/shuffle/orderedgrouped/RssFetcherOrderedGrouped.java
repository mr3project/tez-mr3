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
package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier.SPILL_INFO;
import org.apache.tez.runtime.library.common.shuffle.RssShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

class RssFetcherOrderedGrouped implements FetcherOrderedGroupedBase {

  private static final Logger LOG = LoggerFactory.getLogger(RssFetcherOrderedGrouped.class);
  private static final AtomicInteger nextId = new AtomicInteger(0);

  private final FetchedInputAllocatorOrderedGrouped allocator;
  private final ShuffleScheduler shuffleScheduler;
  private final ExceptionReporter exceptionReporter;
  private final MapHost mapHost;
  private final ShuffleClient rssShuffleClient;
  private final int shuffleId;

  private final int fetcherId;

  private final Object lock = new Object();

  // guarded by lock
  private boolean isShutdown = false;
  private boolean rssShuffleInputStreamClosed = false;
  private InputStream rssShuffleInputStream = null;

  private final int partitionId;
  private final CompositeInputAttemptIdentifier srcAttemptId;
  private final long blockLength;
  private final int mapIndexStart, mapIndexEnd;
  private final boolean readPartitionAllOnce;

  // For decompress/decode InputStream
  private final CompressionCodec codec;
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;
  private final InputContext inputContext;
  private final boolean verifyDiskChecksum;

  public RssFetcherOrderedGrouped(
      FetchedInputAllocatorOrderedGrouped allocator,
      ShuffleScheduler shuffleScheduler,
      ExceptionReporter exceptionReporter,
      MapHost mapHost,
      ShuffleClient rssShuffleClient,
      int shuffleId,
      int partitionId,
      CompositeInputAttemptIdentifier srcAttemptId,
      long blockLength,
      int mapIndexStart,
      int mapIndexEnd,
      boolean readPartitionAllOnce,
      CompressionCodec codec,
      boolean ifileReadAhead,
      int ifileReadAheadLength,
      InputContext inputContext,
      boolean verifyDiskChecksum) {
    this.allocator = allocator;
    this.shuffleScheduler = shuffleScheduler;
    this.exceptionReporter = exceptionReporter;
    this.mapHost = mapHost;
    this.rssShuffleClient = rssShuffleClient;
    this.shuffleId = shuffleId;

    this.partitionId = partitionId;
    this.srcAttemptId = srcAttemptId;
    this.blockLength = blockLength;
    this.mapIndexStart = mapIndexStart;
    this.mapIndexEnd = mapIndexEnd;
    this.readPartitionAllOnce = readPartitionAllOnce;

    this.codec = codec;
    this.ifileReadAhead = ifileReadAhead;
    this.ifileReadAheadLength = ifileReadAheadLength;
    this.inputContext = inputContext;
    this.verifyDiskChecksum = verifyDiskChecksum;

    assert blockLength > 0;
    assert !readPartitionAllOnce || srcAttemptId.getInputIdentifiersForReadPartitionAllOnce() != null;
    assert !readPartitionAllOnce ||
        srcAttemptId.getInputIdentifiersForReadPartitionAllOnce().size() == mapIndexEnd - mapIndexStart;

    this.fetcherId = nextId.getAndIncrement();
  }

  @Override
  public Void call() throws Exception {
    try {
      try {
        if (readPartitionAllOnce) {
          fetchMultipleBlocks();
        } else {
          fetchSingleBlock();
        }
      } finally {
        // if readPartitionAllOnce == true, multiple RssFetcherOrderedGrouped's can be created from mapHost
        if (!readPartitionAllOnce) {
          shuffleScheduler.freeHost(mapHost);
        }
      }

      // We do not need a catch-clause dedicated to InterruptedException,
      // which is not declared inside doFetch().
      // In original FetcherOrderedGrouped, only setupLocalDiskFetch() declares it.
    } catch (Throwable t) {
      exceptionReporter.reportException(t);
    }
    return null;
  }

  @Override
  public void shutDown() {
    synchronized (lock) {
      if (!isShutdown) {
        isShutdown = true;
        closeRssShuffleInputStream();
      }
    }
  }

  private void fetchSingleBlock() throws IOException {
    LOG.info("Ordered - RssFetcher starts fetching from RSS, map index = {}, block length={}",
        srcAttemptId.getTaskIndex(), srcAttemptId.getPartitionSize(partitionId));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Ordered - fetchSingleBlock : Ordered_shuffleId_taskIndex_attemptNumber={}_{}_{}_{}, dataLength={}",
          shuffleId,
          srcAttemptId.getTaskIndex(), srcAttemptId.getAttemptNumber(), partitionId,
          srcAttemptId.getPartitionSize(partitionId));
    }

    doFetch(srcAttemptId);
  }

  private void fetchMultipleBlocks() throws IOException {
    int numBlocks = mapIndexEnd - mapIndexStart;
    LOG.info("Ordered - RssFetcher starts fetching {} concatenated blocks from RSS: {} to {}, total block length={}",
        numBlocks, mapIndexStart, mapIndexEnd, blockLength);

    CompositeInputAttemptIdentifier firstInputAttemptIdentifier =
        srcAttemptId.getInputIdentifiersForReadPartitionAllOnce().get(0);
    doFetch(firstInputAttemptIdentifier);

    // doFetch() reads the entire set of blocks, but marks only firstInputAttemptIdentifier as finished.
    // hence we should mark all the remaining InputAttemptIdentifiers as finished as well
    // i.e., we have already called copySucceeded() for i = 0.
    for (int i = 1; i < numBlocks; i++) {
      InputAttemptIdentifier inputAttemptId =
          srcAttemptId.getInputIdentifiersForReadPartitionAllOnce().get(i);
      shuffleScheduler.copySucceeded(inputAttemptId, null, 0L, 0L, 0, null, false);
    }
  }

  private void doFetch(InputAttemptIdentifier baseInputAttemptIdentifier) throws IOException {
    setupRssShuffleInputStream(mapIndexStart, mapIndexEnd, srcAttemptId.getAttemptNumber(), partitionId);
    DataInputStream dis = new DataInputStream(rssShuffleInputStream);

    long totalReceivedBytes = 0L;
    int numFetchedBlocks = 0;
    try {
      while (totalReceivedBytes < blockLength) {
        int index = numFetchedBlocks;
        numFetchedBlocks++;
        long startTime = System.currentTimeMillis();

        // We assume that the number of bytes read by DataInputStream.readLong() is equal to Long.BYTES.
        long compressedSize = dis.readLong();
        long decompressedSize = dis.readLong();
        totalReceivedBytes += RssShuffleUtils.RSS_SHUFFLE_HEADER_SIZE;

        boolean isLastBlock = totalReceivedBytes + compressedSize >= blockLength;
        InputAttemptIdentifier identifierForCurrentBlock = new InputAttemptIdentifier(
            baseInputAttemptIdentifier.getInputIdentifier(),
            baseInputAttemptIdentifier.getAttemptNumber(),
            baseInputAttemptIdentifier.getPathComponent(),
            false,
            isLastBlock ? SPILL_INFO.FINAL_UPDATE : SPILL_INFO.INCREMENTAL_UPDATE,
            index);

        MapOutput mapOutput = allocator.reserve(identifierForCurrentBlock, decompressedSize, compressedSize, fetcherId, true);

        if (mapOutput.getType() == MapOutput.Type.MEMORY) {
          ShuffleUtils.shuffleToMemory(mapOutput.getMemory(), rssShuffleInputStream, (int) decompressedSize,
              (int) compressedSize, codec, ifileReadAhead, ifileReadAheadLength, LOG, identifierForCurrentBlock, inputContext);
        } else if (mapOutput.getType() == MapOutput.Type.DISK) {
          ShuffleUtils.shuffleToDisk(mapOutput.getDisk(), "RSS", rssShuffleInputStream, compressedSize,
              decompressedSize, LOG, identifierForCurrentBlock, ifileReadAhead, ifileReadAheadLength, verifyDiskChecksum);
        } else {
          throw new TezUncheckedException("Unexpected MapOutput.Type: " + mapOutput);
        }
        totalReceivedBytes += compressedSize;

        // this can be checked before performing I/O, but okay because this is an error case
        if (totalReceivedBytes > blockLength) {
          String message = String.format("Ordered - RssFetcher received %d bytes. Expected size: %d",
              totalReceivedBytes, blockLength);
          throw new IOException(message);
        }

        // copySucceeded() should be called in order to call mapOutput.commit().
        // The last block marks baseInputAttemptIdentifier (which is the first InputAttemptIdentifier
        // belonging to srcAttemptId) as finished.
        //
        // copySucceeded() may trigger ShuffleScheduler shutdown, which calls ShuffleScheduler.close()
        // which in turn calls this.shutDown().
        long copyDuration = System.currentTimeMillis() - startTime;
        shuffleScheduler.copySucceeded(identifierForCurrentBlock, null, compressedSize, decompressedSize, copyDuration,
            mapOutput, false);
      }
      assert totalReceivedBytes == blockLength;

      // If copySucceeded() triggers ShuffleScheduler shutdown, rssShuffleInputStream may have already been closed.
      // We take an optimistic approach and do not check 'rssShuffleInputStream.read() == -1' to see if there are
      // remaining bytes. This makes sense because copySucceeded() may trigger ShuffleScheduler shutdown,
      // in which case there is no point in checking 'rssShuffleInputStream.read() == -1'.
    } catch (Exception e) {
      LOG.error("Ordered - RssFetcher failed: shuffleId={}, from {} to {}, attemptNumber={}, partitionId={}, expected data={}",
          shuffleId, mapIndexStart, mapIndexEnd, srcAttemptId.getAttemptNumber(), partitionId, blockLength, e);
      throw e;
    } finally {
      synchronized (lock) {
        closeRssShuffleInputStream();
      }
    }
  }

  private void setupRssShuffleInputStream(int mapIndexStart, int mapIndexEnd, int mapAttemptNumber, int partitionId)
      throws IOException {
    synchronized (lock) {
      if (!isShutdown) {
        rssShuffleInputStream = rssShuffleClient.readPartition(shuffleId, partitionId, mapAttemptNumber,
            mapIndexStart, mapIndexEnd);
        // rssShuffleInputStream.close() is usually called inside the current thread, but it may
        // be called ShuffleScheduler.close() thread
      } else {
        LOG.warn("RssFetcherOrderedGrouped.shutDown() is called before it connects to RSS");
        throw new IllegalStateException("RssFetcherOrderedGrouped - shutdown detected");
      }
    }
  }

  // Invariant: inside synchronized (lock)
  private void closeRssShuffleInputStream() {
    if (rssShuffleInputStream != null && !rssShuffleInputStreamClosed) {
      try {
        rssShuffleInputStream.close();
      } catch (IOException e) {
        LOG.error("Failed to close rssShuffleInputStream", e);
      }
      rssShuffleInputStreamClosed = true;
      // Do not set rssShuffleInputStream to null because if this call is from shutDown(),
      // rssShuffleInputStream may still be read in doFetch() and we do not want to see NPE.
    }
  }
}
