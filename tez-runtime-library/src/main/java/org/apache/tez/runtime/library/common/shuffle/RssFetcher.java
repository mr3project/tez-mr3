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

package org.apache.tez.runtime.library.common.shuffle;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier.SPILL_INFO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

public class RssFetcher implements FetcherBase {
  private final FetcherCallback fetcherCallback;
  private final FetchedInputAllocator inputAllocator;
  private final long dataLength;

  private final ShuffleClient rssShuffleClient;
  private final int shuffleId;

  private final String host;
  private final int port;
  private final int partitionId;
  private final CompositeInputAttemptIdentifier srcAttemptId;

  private static final Logger LOG = LoggerFactory.getLogger(RssFetcher.class);

  private final Object lock = new Object();

  // guarded by lock
  private boolean isShutdown = false;
  private boolean rssShuffleInputStreamClosed = false;
  private InputStream rssShuffleInputStream = null;

  private final int mapIndexStart, mapIndexEnd;
  private final boolean readPartitionAllOnce;

  // For decompress/decode InputStream
  private final CompressionCodec codec;
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;
  private final InputContext inputContext;
  private final boolean verifyDiskChecksum;

  public RssFetcher(
      FetcherCallback fetcherCallback,
      FetchedInputAllocator inputAllocator,
      ShuffleClient rssShuffleClient,
      int shuffleId,
      String host,
      int port,
      int partitionId,
      CompositeInputAttemptIdentifier srcAttemptId,
      long dataLength,
      int mapIndexStart,
      int mapIndexEnd,
      boolean readPartitionAllOnce,
      CompressionCodec codec,
      boolean ifileReadAhead,
      int ifileReadAheadLength,
      InputContext inputContext,
      boolean verifyDiskChecksum) {
    this.fetcherCallback = fetcherCallback;
    this.inputAllocator = inputAllocator;
    this.rssShuffleClient = rssShuffleClient;
    this.shuffleId = shuffleId;
    this.host = host;
    this.port = port;
    this.partitionId = partitionId;
    this.srcAttemptId = srcAttemptId;
    this.dataLength = dataLength;
    this.mapIndexStart = mapIndexStart;
    this.mapIndexEnd = mapIndexEnd;
    this.readPartitionAllOnce = readPartitionAllOnce;
    this.codec = codec;
    this.ifileReadAhead = ifileReadAhead;
    this.ifileReadAheadLength = ifileReadAheadLength;
    this.inputContext = inputContext;
    this.verifyDiskChecksum = verifyDiskChecksum;

    assert dataLength >= 0;
  }

  public FetchResult call() throws Exception {
    fetchMultipleBlocks(srcAttemptId);

    LOG.info("RssFetcher finished with readPartitionAllOnce={}: {}, num={}, partitionId={}, dataLength={}, from={}, to={}",
        readPartitionAllOnce,
        srcAttemptId,
        !readPartitionAllOnce ? 0 :
        srcAttemptId.getInputIdentifiersForReadPartitionAllOnce().size(),
        partitionId, dataLength,
        mapIndexStart, mapIndexEnd);

    return new FetchResult(host, port, partitionId, 1, new ArrayList<>());
  }

  private void fetchMultipleBlocks(InputAttemptIdentifier baseInputAttemptIdentifier) throws IOException {
    setupRssShuffleInputStream(mapIndexStart, mapIndexEnd, srcAttemptId.getAttemptNumber(), partitionId);
    DataInputStream dis = new DataInputStream(rssShuffleInputStream);

    long totalReceivedBytes = 0L;
    int numFetchedBlocks = 0;
    try {
      while (totalReceivedBytes < dataLength) {
        int index = numFetchedBlocks;
        numFetchedBlocks++;

        long startTime = System.currentTimeMillis();

        // We assume that the number of bytes read by DataInputStream.readLong() is equal to Long.BYTES.
        long compressedSize = dis.readLong();
        long decompressedSize = dis.readLong();
        totalReceivedBytes += RssShuffleUtils.RSS_SHUFFLE_HEADER_SIZE;

        boolean isLastBlock = totalReceivedBytes + compressedSize >= dataLength;

        InputAttemptIdentifier fakeIAI = new InputAttemptIdentifier(
            baseInputAttemptIdentifier.getInputIdentifier(),
            baseInputAttemptIdentifier.getAttemptNumber(),
            baseInputAttemptIdentifier.getPathComponent(),
            false,
            isLastBlock ? SPILL_INFO.FINAL_UPDATE : SPILL_INFO.INCREMENTAL_UPDATE,
            index);

        FetchedInput fetchedInput = inputAllocator.allocate(decompressedSize, compressedSize, fakeIAI);

        if (fetchedInput.getType() == FetchedInput.Type.MEMORY) {
          MemoryFetchedInput mfi = (MemoryFetchedInput) fetchedInput;
          ShuffleUtils.shuffleToMemory(mfi.getBytes(), rssShuffleInputStream, (int) decompressedSize,
              (int) compressedSize, codec, ifileReadAhead, ifileReadAheadLength, LOG, fakeIAI, inputContext);
        } else if (fetchedInput.getType() == FetchedInput.Type.DISK) {
          ShuffleUtils.shuffleToDisk(fetchedInput.getOutputStream(), host, rssShuffleInputStream,
              compressedSize, decompressedSize, LOG, fakeIAI, ifileReadAhead, ifileReadAheadLength,
              verifyDiskChecksum);
        } else {
          throw new TezUncheckedException("Unknown FetchedInput.Type: " + fetchedInput);
        }

        totalReceivedBytes += compressedSize;
        long copyDuration = System.currentTimeMillis() - startTime;

        fetcherCallback.fetchSucceeded(host, fakeIAI, fetchedInput, compressedSize, decompressedSize,
            copyDuration);
      }
    } finally {
      synchronized (lock) {
        if (!rssShuffleInputStreamClosed) {
          rssShuffleInputStreamClosed = true;
          rssShuffleInputStream.close();
        }
      }
    }

    if (totalReceivedBytes != dataLength) {
      String message = String.format("Ordered - RssFetcher received only %d bytes. Expected size: %d",
          totalReceivedBytes, dataLength);
      LOG.error(message);
      throw new IOException(message);
    }

    if (readPartitionAllOnce) {
      // ShuffleManager.getNextInput() should not get stuck in completedInputs.take():
      //   1. mark completion for every InputAttemptIdentifier except srcAttemptId
      //   2. call fetchSucceeded() on srcAttemptId and fetchedInput
      // Note that InputAttemptIdentifier's have the same partitionId, but different inputIdentifiers.
      for (CompositeInputAttemptIdentifier iai : srcAttemptId.getInputIdentifiersForReadPartitionAllOnce()) {
        if (iai.getInputIdentifier() != baseInputAttemptIdentifier.getInputIdentifier()) {
          // fetchedInput == null, so mark completion only
          fetcherCallback.fetchSucceeded(host, iai, null, 0L, 0L, 0L);
        }
      }
    }
  }

  public void shutdown() {
    try {
      synchronized (lock) {
        isShutdown = true;
        if (rssShuffleInputStream != null && !rssShuffleInputStreamClosed) {
          rssShuffleInputStreamClosed = true;
          rssShuffleInputStream.close();
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to close rssShuffleInputStream while shutting down RssFetcher.", e);
    }
  }

  private void setupRssShuffleInputStream(int mapIndexStart, int mapIndexEnd, int mapAttemptNumber,
      int partitionId) throws IOException {
    synchronized (lock) {
      if (!isShutdown) {
        rssShuffleInputStream = rssShuffleClient.readPartition(shuffleId, partitionId, mapAttemptNumber,
            mapIndexStart, mapIndexEnd);
      } else {
        LOG.warn("RssFetcher.shutdown() is called before it connects to RSS. Stop running RssFetcher");
        throw new IllegalStateException("Detected shutdown");
      }
    }
  }
}
