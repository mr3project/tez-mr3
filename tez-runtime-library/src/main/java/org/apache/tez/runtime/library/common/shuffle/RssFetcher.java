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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

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

    assert dataLength > 0;
  }

  public FetchResult call() throws Exception {
    List<CompositeInputAttemptIdentifier> inputList;
    if (readPartitionAllOnce) {
      inputList = new ArrayList<>();
      for (CompositeInputAttemptIdentifier iai: srcAttemptId.getInputIdentifiersForReadPartitionAllOnce()) {
        inputList.addAll(iai.getInputIdentifiersForReadPartitionAllOnce());
      }
    } else {
      inputList = srcAttemptId.getInputIdentifiersForReadPartitionAllOnce();
    }

    assert !inputList.isEmpty();
    assert inputList.stream().allMatch(i -> i.getInputIdentifiersForReadPartitionAllOnce() == null);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Unordered - fetchMultipleBlocks : unordered_shuffleId_taskIndex_attemptNumber={}_{}[{}-{}]_{}_{}, dataLength={}",
          shuffleId, srcAttemptId.getTaskIndex(), mapIndexStart, mapIndexEnd,
          srcAttemptId.getAttemptNumber(), partitionId, dataLength);
    }
    fetchMultipleBlocks(inputList);

    LOG.info("RssFetcher finished with readPartitionAllOnce={}: {}, numInputs={}, numBlocks={}, " +
            "partitionId={}, dataLength={}, from={}, to={}",
        readPartitionAllOnce,
        srcAttemptId,
        !readPartitionAllOnce ? 1 : srcAttemptId.getInputIdentifiersForReadPartitionAllOnce().size(),
        inputList.size(),
        partitionId,
        dataLength,
        mapIndexStart,
        mapIndexEnd);

    return new FetchResult(host, port, partitionId, 1, new ArrayList<>());
  }

  private void fetchMultipleBlocks(List<CompositeInputAttemptIdentifier> inputList) throws IOException {
    setupRssShuffleInputStream(mapIndexStart, mapIndexEnd, srcAttemptId.getAttemptNumber(), partitionId);
    DataInputStream dis = new DataInputStream(rssShuffleInputStream);

    long totalReceivedBytes = 0L;
    try {
      for (CompositeInputAttemptIdentifier iai: inputList) {
        long startTime = System.currentTimeMillis();

        // We assume that the number of bytes read by DataInputStream.readLong() is equal to Long.BYTES.
        long compressedSize = dis.readLong();
        long decompressedSize = dis.readLong();
        totalReceivedBytes += RssShuffleUtils.TEZ_RSS_SHUFFLE_HEADER_SIZE;

        FetchedInput fetchedInput = inputAllocator.allocate(decompressedSize, compressedSize, iai);

        if (fetchedInput.getType() == FetchedInput.Type.MEMORY) {
          MemoryFetchedInput mfi = (MemoryFetchedInput) fetchedInput;
          ShuffleUtils.shuffleToMemory(mfi.getBytes(), rssShuffleInputStream, (int) decompressedSize,
              (int) compressedSize, codec, ifileReadAhead, ifileReadAheadLength, LOG, iai, inputContext);
        } else if (fetchedInput.getType() == FetchedInput.Type.DISK) {
          ShuffleUtils.shuffleToDisk(fetchedInput.getOutputStream(), host, rssShuffleInputStream,
              compressedSize, decompressedSize, LOG, iai, ifileReadAhead, ifileReadAheadLength,
              verifyDiskChecksum);
        } else {
          throw new TezUncheckedException("Unknown FetchedInput.Type: " + fetchedInput);
        }
        totalReceivedBytes += compressedSize;

        // this can be checked before performing I/O, but okay because this is an error case
        if (totalReceivedBytes > dataLength) {
          String message = String.format("Unordered - RssFetcher received %d bytes. Expected size: %d",
              totalReceivedBytes, dataLength);
          throw new IOException(message);
        }

        // TODO: check if copySucceeded() can trigger ShuffleManager shutdown
        long copyDuration = System.currentTimeMillis() - startTime;
        fetcherCallback.fetchSucceeded(host, iai, fetchedInput, compressedSize, decompressedSize,
            copyDuration);
      }

      if (totalReceivedBytes < dataLength) {
        String message = String.format("Unordered - RssFetcher received only %d bytes. Expected size: %d",
            totalReceivedBytes, dataLength);
        throw new IOException(message);
      }

      // We take an optimistic approach and do not check 'rssShuffleInputStream.read() == -1' to see if there are
      // remaining bytes.
      // TODO: If copySucceeded() triggers ShuffleManager shutdown, rssShuffleInputStream.read() should not be called.
    } catch (Exception e) {
      LOG.error("Unordered - RssFetcher failed: shuffleId={}, from {} to {}, attemptNumber={}, partitionId={}, expected data={}",
          shuffleId, mapIndexStart, mapIndexEnd, srcAttemptId.getAttemptNumber(), partitionId, dataLength, e);
      throw e;
    } finally {
      synchronized (lock) {
        closeRssShuffleInputStream();
      }
    }
  }

  public void shutdown() {
    synchronized (lock) {
      if (!isShutdown) {
        isShutdown = true;
        closeRssShuffleInputStream();
      }
    }
  }

  private void setupRssShuffleInputStream(int mapIndexStart, int mapIndexEnd, int mapAttemptNumber,
      int partitionId) throws IOException {
    synchronized (lock) {
      if (!isShutdown) {
        rssShuffleInputStream = rssShuffleClient.readPartition(shuffleId, partitionId, mapAttemptNumber,
            mapIndexStart, mapIndexEnd);
      } else {
        LOG.warn("RssFetcher.shutdown() is called before it connects to RSS");
        throw new IllegalStateException("RssFetcher - shutdown detected");
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
    }
  }
}
