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
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.RssShuffleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

  public RssFetcherOrderedGrouped(
      FetchedInputAllocatorOrderedGrouped allocator,
      ShuffleScheduler shuffleScheduler,
      ExceptionReporter exceptionReporter,
      MapHost mapHost,
      ShuffleClient rssShuffleClient,
      int shuffleId,
      int partitionId,
      CompositeInputAttemptIdentifier srcAttemptId,
      long blockLength, int mapIndexStart, int mapIndexEnd,
      boolean readPartitionAllOnce) {
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

    assert blockLength >= 0;
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
    try {
      synchronized (lock) {
        isShutdown = true;
        if (rssShuffleInputStream != null && !rssShuffleInputStreamClosed) {
          rssShuffleInputStreamClosed = true;
          rssShuffleInputStream.close();
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to close rssShuffleInputStream while shutting down RssFetcherOrderedGrouped.", e);
    }
  }

  private void fetchSingleBlock() throws IOException {
      LOG.info("Ordered - fetchSingleBlock : Ordered_shuffleId_taskIndex_attemptNumber={}_{}_{}_{}, dataLength={}",
          shuffleId,
          srcAttemptId.getTaskIndex(), srcAttemptId.getAttemptNumber(), partitionId,
          srcAttemptId.getPartitionSize(partitionId));

    if (blockLength == 0) {
      shuffleScheduler.copySucceeded(srcAttemptId, null, blockLength, blockLength, 0L, null,
          false);
      return;
    }

    long actualSize = blockLength + RssShuffleUtils.EOF_MARKERS_SIZE - Long.BYTES;
    MapOutput mapOutput = allocator.reserve(srcAttemptId, actualSize, actualSize, fetcherId, true);
    assert mapOutput.getType() != MapOutput.Type.WAIT;

    long startTime = System.currentTimeMillis();

    setupRssShuffleInputStream(mapIndexStart, mapIndexEnd, srcAttemptId.getAttemptNumber(), partitionId);

    try {
      long dataLength = getLengthFromHeader(rssShuffleInputStream);
      if (dataLength + Long.BYTES != blockLength) {
        String message =
            "The length of blocks coming from DME and InputStream does not match. " +
            String.format("DME: %d, InputStream: %d", blockLength, dataLength + Long.BYTES);
        LOG.error(message);
        throw new IOException(message);
      }

      if (mapOutput.getType() == MapOutput.Type.MEMORY) {
        RssShuffleUtils.shuffleToMemory(rssShuffleInputStream, mapOutput.getMemory(), dataLength);
      } else if (mapOutput.getType() == MapOutput.Type.DISK) {
        try (OutputStream outputStream = mapOutput.getDisk()) {
          RssShuffleUtils.shuffleToDisk(rssShuffleInputStream, outputStream, dataLength);
        }
      } else {
        throw new TezUncheckedException("Unexpected MapOutput.Type: " + mapOutput);
      }
    } finally {
      closeRssShuffleInputStream();
    }

    long copyDuration = System.currentTimeMillis() - startTime;
    shuffleScheduler.copySucceeded(srcAttemptId, null, blockLength, blockLength, copyDuration, mapOutput,
        false);
  }

  private void fetchMultipleBlocks() throws IOException {
    int numBlocks = mapIndexEnd - mapIndexStart;
    LOG.info("Ordered - RssFetcher starts fetching {} concatenated blocks from RSS: {} to {}, total block length={}",
        numBlocks, mapIndexStart, mapIndexEnd, blockLength);

    if (blockLength == 0) {
      for (InputAttemptIdentifier inputAttemptId : srcAttemptId.getInputIdentifiersForReadPartitionAllOnce()) {
          shuffleScheduler.copySucceeded(inputAttemptId, null, 0L, 0L, 0, null, false);
      }
      return;
    }

    setupRssShuffleInputStream(mapIndexStart, mapIndexEnd, srcAttemptId.getAttemptNumber(), partitionId);

    long totalReceivedBytes = 0L;
    int numFetchedBlocks = 0;
    try {
      while (numFetchedBlocks < numBlocks && totalReceivedBytes < blockLength) {
        // 'index' (Mapper index) does not match InputAttemptIdentifier that is generated by the
        // Mapper because Celeborn does not order Mapper output blocks with their indexes.
        // Still this is safe for our purpose because we only have to process 'numBlocks' output blocks.

        int index = numFetchedBlocks;
        numFetchedBlocks++;

        long startTime = System.currentTimeMillis();

        CompositeInputAttemptIdentifier inputAttemptId =
            srcAttemptId.getInputIdentifiersForReadPartitionAllOnce().get(index);
        int currentMapIndex = inputAttemptId.getTaskIndex();
        long currentPartitionSize = inputAttemptId.getPartitionSize(partitionId);
        assert mapIndexStart <= currentMapIndex && currentMapIndex < mapIndexEnd;
        LOG.info("Ordered - RssFetcher is reading (totalReceivedBytes={}): index={}, mapIndex={}, partitionSize={}",
            totalReceivedBytes, index, currentMapIndex, currentPartitionSize);

        long dataLength = getLengthFromHeader(rssShuffleInputStream);
        totalReceivedBytes += Long.BYTES;

        long actualSize = dataLength + RssShuffleUtils.EOF_MARKERS_SIZE;
        MapOutput mapOutput = allocator.reserve(inputAttemptId, actualSize, actualSize, fetcherId, true);

        if (mapOutput.getType() == MapOutput.Type.MEMORY) {
          RssShuffleUtils.shuffleToMemory(rssShuffleInputStream, mapOutput.getMemory(), dataLength);
        } else if (mapOutput.getType() == MapOutput.Type.DISK) {
          try (OutputStream outputStream = mapOutput.getDisk()) {
            RssShuffleUtils.shuffleToDisk(rssShuffleInputStream, outputStream, dataLength);
          }
        } else {
          throw new TezUncheckedException("Unexpected MapOutput.Type: " + mapOutput);
        }
        totalReceivedBytes += dataLength;

        /*
        // wrong because ordering is not guaranteed
        if (currentPartitionSize != Long.BYTES + dataLength) {
          String message = String.format("Ordered - RssFetcher for %d received only %d bytes. Expected size: %d",
             currentMapIndex, Long.BYTES + dataLength, currentPartitionSize);
          LOG.error(message);
          throw new IOException(message);
        }
         */

        long copyDuration = System.currentTimeMillis() - startTime;
        shuffleScheduler.copySucceeded(inputAttemptId, null, dataLength, dataLength, copyDuration,
            mapOutput, false);
      }
    } finally {
      closeRssShuffleInputStream();
    }

    LOG.info("Ordered - RssFetcher finished fetching {} concatenated blocks from RSS. {} blocks are empty.",
        numFetchedBlocks, numBlocks - numFetchedBlocks);

    if (totalReceivedBytes != blockLength) {
      String message = String.format("Ordered - RssFetcher received only %d bytes. Expected size: %d",
          totalReceivedBytes, blockLength);
      LOG.error(message);
      throw new IOException(message);
    }

    if (numFetchedBlocks < numBlocks) {
      for (int i = numFetchedBlocks; i < numBlocks; i++) {
        InputAttemptIdentifier inputAttemptId =
            srcAttemptId.getInputIdentifiersForReadPartitionAllOnce().get(i);
        shuffleScheduler.copySucceeded(inputAttemptId, null, 0L, 0L, 0, null, false);
      }
    }
  }

  private void setupRssShuffleInputStream(int mapIndexStart, int mapIndexEnd, int mapAttemptNumber, int partitionId)
      throws IOException {
    synchronized (lock) {
      if (!isShutdown) {
        rssShuffleInputStream = rssShuffleClient.readPartition(shuffleId, partitionId, mapAttemptNumber,
            mapIndexStart, mapIndexEnd);
        // now rssShuffleInputStream.close() should be called inside the current thread
      } else {
        LOG.warn("RssFetcherOrderedGrouped.shutdown() is called before it connects to RSS. " +
            "Stop running RssFetcherOrderedGrouped");
        throw new IllegalStateException("Detected shutdown");
      }
    }
  }

  private long getLengthFromHeader(InputStream inputStream) throws IOException {
    DataInputStream dis = new DataInputStream(inputStream);
    return dis.readLong();
  }

  private void closeRssShuffleInputStream() {
    synchronized (lock) {
      if (!rssShuffleInputStreamClosed) {
        rssShuffleInputStreamClosed = true;
        try {
          rssShuffleInputStream.close();
        } catch (IOException e) {
          LOG.error("Failed to close rssShuffleInputStream", e);
        }
      }
    }
  }
}
