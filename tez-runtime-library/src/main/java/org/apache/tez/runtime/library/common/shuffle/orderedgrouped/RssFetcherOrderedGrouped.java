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
  private final long dataLength;
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
      long dataLength, int mapIndexStart, int mapIndexEnd,
      boolean readPartitionAllOnce) {
    this.allocator = allocator;
    this.shuffleScheduler = shuffleScheduler;
    this.exceptionReporter = exceptionReporter;
    this.mapHost = mapHost;
    this.rssShuffleClient = rssShuffleClient;
    this.shuffleId = shuffleId;

    this.partitionId = partitionId;
    this.srcAttemptId = srcAttemptId;
    this.dataLength = dataLength;
    this.mapIndexStart = mapIndexStart;
    this.mapIndexEnd = mapIndexEnd;
    this.readPartitionAllOnce = readPartitionAllOnce;
    assert dataLength > 0;  // TODO: support DME with partitionSizes == null.

    this.fetcherId = nextId.getAndIncrement();
  }

  @Override
  public Void call() throws Exception {
    try {
      try {
        doFetch();
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

  private void doFetch() throws IOException {
    long actualSize = dataLength + RssShuffleUtils.EOF_MARKERS_SIZE;
    MapOutput mapOutput = allocator.reserve(srcAttemptId, actualSize, actualSize, fetcherId);

    if (mapOutput.getType() == MapOutput.Type.MEMORY) {
      setupRssShuffleInputStream(mapIndexStart, mapIndexEnd, srcAttemptId.getAttemptNumber(), partitionId);

      long startTime = System.currentTimeMillis();
      shuffleToMemory(srcAttemptId, mapOutput, dataLength);

      long copyDuration = System.currentTimeMillis() - startTime;
      reportCopySucceeded(copyDuration, mapOutput);
    } else if (mapOutput.getType() == MapOutput.Type.DISK) {
      setupRssShuffleInputStream(mapIndexStart, mapIndexEnd, srcAttemptId.getAttemptNumber(), partitionId);

      long startTime = System.currentTimeMillis();
      shuffleToDisk(srcAttemptId, mapOutput, dataLength);

      long copyDuration = System.currentTimeMillis() - startTime;
      reportCopySucceeded(copyDuration, mapOutput);
    } else if (mapOutput.getType() == MapOutput.Type.WAIT) {
      // TODO: bug-fix - mapHost should not be put back because it can generate multiple fetchers
      shuffleScheduler.putBackKnownMapOutput(mapHost, srcAttemptId);
    } else {
      throw new TezUncheckedException("Unknown MapOutput.Type: " + mapOutput);
    }
  }

  private void reportCopySucceeded(long copyDuration, MapOutput mapOutput) throws IOException {
    if (readPartitionAllOnce) {
      LOG.info("Ordered - RssFetcher finished with readPartitionAllOnce: {}, num={}, partitionId={}, dataLength={}, copyDuration={}",
          srcAttemptId, srcAttemptId.getInputIdentifiersForReadPartitionAllOnce().size(), partitionId, dataLength, copyDuration);
      for (InputAttemptIdentifier inputIdentifier: srcAttemptId.getInputIdentifiersForReadPartitionAllOnce()) {
        if (inputIdentifier.getInputIdentifier() != srcAttemptId.getInputIdentifier()) {
          shuffleScheduler.copySucceeded(inputIdentifier, mapHost, 0L, 0L, copyDuration, mapOutput, false);
        }
      }
      shuffleScheduler.copySucceeded(srcAttemptId, mapHost, dataLength, dataLength, copyDuration, mapOutput, false);
    } else {
      shuffleScheduler.copySucceeded(srcAttemptId, mapHost, dataLength, dataLength, copyDuration, mapOutput, false);
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

  private void shuffleToMemory(InputAttemptIdentifier srcAttempt, MapOutput mapOutput, long dataLength)
      throws IOException {
    try {
      RssShuffleUtils.shuffleToMemory(rssShuffleInputStream, mapOutput.getMemory(), dataLength);
    } finally {
      synchronized (lock) {
        if (!rssShuffleInputStreamClosed) {
          rssShuffleInputStreamClosed = true;
          rssShuffleInputStream.close();
        }
      }
    }
  }

  private void shuffleToDisk(InputAttemptIdentifier srcAttempt, MapOutput mapOutput, long dataLength)
      throws IOException{
    try (OutputStream outputStream = mapOutput.getDisk()) {
      RssShuffleUtils.shuffleToDisk(rssShuffleInputStream, outputStream, dataLength);
    } finally {
      synchronized (lock) {
        if (!rssShuffleInputStreamClosed) {
          rssShuffleInputStreamClosed = true;
          rssShuffleInputStream.close();
        }
      }
    }
  }
}
