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
  private final String rssApplicationId;
  private final int shuffleId;

  private final int fetcherId;

  private final Object lock = new Object();

  // guarded by lock
  private InputStream rssShuffleInputStream = null;
  private boolean isShutdown = false;

  public RssFetcherOrderedGrouped(
      FetchedInputAllocatorOrderedGrouped allocator,
      ShuffleScheduler shuffleScheduler,
      ExceptionReporter exceptionReporter,
      MapHost mapHost,
      ShuffleClient rssShuffleClient,
      String rssApplicationId,
      int shuffleId) {
    this.allocator = allocator;
    this.shuffleScheduler = shuffleScheduler;
    this.exceptionReporter = exceptionReporter;
    this.mapHost = mapHost;
    this.rssShuffleClient = rssShuffleClient;
    this.rssApplicationId = rssApplicationId;
    this.shuffleId = shuffleId;

    this.fetcherId = nextId.getAndIncrement();
  }

  @Override
  public Void call() throws Exception {
    try {
      try {
        doFetch();
      } finally {
        shuffleScheduler.freeHost(mapHost);
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
        if (rssShuffleInputStream != null) {
          rssShuffleInputStream.close();
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to close rssShuffleInputStream while shutting down RssFetcherOrderedGrouped.", e);
    }
  }

  private void doFetch() throws IOException {
    assert mapHost.getPort() == 0;
    assert mapHost.getPartitionCount() == 1;
    assert mapHost.getNumKnownMapOutputs() == 1;

    // skip calling shuffleScheduler.getMapsForHost(mapHost)
    // as there is only 1 IAI in each MapHost when RSS is enabled.
    CompositeInputAttemptIdentifier srcAttemptId =
        (CompositeInputAttemptIdentifier) mapHost.getAndClearKnownMaps().get(0);

    int mapIndex = Integer.parseInt(mapHost.getHost());
    int mapAttemptNumber = srcAttemptId.getAttemptNumber();
    int partitionId = mapHost.getPartitionId();

    long dataLength = srcAttemptId.getPartitionSize(partitionId);
    assert dataLength > 0;  // TODO: support DME with partitionSizes == null.

    long actualSize = dataLength + RssShuffleUtils.EOF_MARKERS_SIZE;
    MapOutput mapOutput = allocator.reserve(srcAttemptId, actualSize, actualSize, fetcherId);

    if (mapOutput.getType() == MapOutput.Type.MEMORY) {
      setupRssShuffleInputStream(mapIndex, mapAttemptNumber, partitionId);
      shuffleToMemory(srcAttemptId, mapOutput, dataLength);
    } else if (mapOutput.getType() == MapOutput.Type.DISK) {
      setupRssShuffleInputStream(mapIndex, mapAttemptNumber, partitionId);
      shuffleToDisk(srcAttemptId, mapOutput, dataLength);
    } else if (mapOutput.getType() == MapOutput.Type.WAIT) {
      shuffleScheduler.putBackKnownMapOutput(mapHost, srcAttemptId);
    } else {
      throw new TezUncheckedException("Unknown MapOutput.Type: " + mapOutput);
    }
  }

  private void setupRssShuffleInputStream(int mapIndex, int mapAttemptNumber, int partitionId)
      throws IOException {
    synchronized (lock) {
      if (!isShutdown) {
        rssShuffleInputStream = rssShuffleClient.readPartition(rssApplicationId, shuffleId, partitionId,
            mapAttemptNumber, mapIndex, mapIndex + 1);
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
      long startTime = System.currentTimeMillis();

      RssShuffleUtils.shuffleToMemory(rssShuffleInputStream, mapOutput.getMemory(), dataLength);

      long copyDuration = System.currentTimeMillis() - startTime;
      shuffleScheduler.copySucceeded(srcAttempt, mapHost, dataLength, dataLength, copyDuration, mapOutput,
          false);
    } finally {
      rssShuffleInputStream.close();
    }
  }

  private void shuffleToDisk(InputAttemptIdentifier srcAttempt, MapOutput mapOutput, long dataLength)
      throws IOException{
    try (OutputStream outputStream = mapOutput.getDisk()) {
      long startTime = System.currentTimeMillis();

      RssShuffleUtils.shuffleToDisk(rssShuffleInputStream, outputStream, dataLength);

      long copyDuration = System.currentTimeMillis() - startTime;
      shuffleScheduler.copySucceeded(srcAttempt, mapHost, dataLength, dataLength, copyDuration, mapOutput,
          false);
    } finally {
      rssShuffleInputStream.close();
    }
  }
}
