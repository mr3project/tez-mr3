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
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

public class RssFetcher implements FetcherBase {
  private final FetcherCallback fetcherCallback;
  private final FetchedInputAllocator inputAllocator;
  private final long dataLength;

  private final ShuffleClient rssShuffleClient;
  private final String rssApplicationId;
  private final int shuffleId;

  private final String host;
  private final int port;
  private final int partitionId;
  private final CompositeInputAttemptIdentifier srcAttemptId;

  private static final Logger LOG = LoggerFactory.getLogger(RssFetcher.class);

  private static final int BUFFER_SIZE = 64 * 1024;
  private static final int EOF_MARKERS_SIZE = 2 * WritableUtils.getVIntSize(IFile.EOF_MARKER);

  private final Object lock = new Object();

  // guarded by lock
  private InputStream rssShuffleInputStream = null;
  private boolean isShutdown = false;

  private final int mapIndexStart, mapIndexEnd;
  private final boolean readPartitionAllOnce;

  public RssFetcher(
      FetcherCallback fetcherCallback,
      FetchedInputAllocator inputAllocator,
      ShuffleClient rssShuffleClient,
      String rssApplicationId,
      int shuffleId,
      String host,
      int port,
      int partitionId,
      CompositeInputAttemptIdentifier srcAttemptId,
      long dataLength, int mapIndexStart, int mapIndexEnd,
      boolean readPartitionAllOnce) {
    assert (dataLength == -1 || dataLength > 0);

    this.fetcherCallback = fetcherCallback;
    this.inputAllocator = inputAllocator;
    this.rssShuffleClient = rssShuffleClient;
    this.rssApplicationId = rssApplicationId;
    this.shuffleId = shuffleId;
    this.host = host;
    this.port = port;
    this.partitionId = partitionId;
    this.srcAttemptId = srcAttemptId;
    this.dataLength = dataLength;
    this.mapIndexStart = mapIndexStart;
    this.mapIndexEnd = mapIndexEnd;
    this.readPartitionAllOnce = readPartitionAllOnce;
  }

  public FetchResult call() throws Exception {
    long startTime = System.currentTimeMillis();

    FetchedInput fetchedInput;
    if (dataLengthUnknown()) {
      fetchedInput = inputAllocator.allocateType(FetchedInput.Type.DISK, dataLength, dataLength, srcAttemptId);
    } else {
      long actualSize = dataLength + EOF_MARKERS_SIZE;
      fetchedInput = inputAllocator.allocate(actualSize, actualSize, srcAttemptId);
    }

    synchronized (lock) {
      if (!isShutdown) {
        rssShuffleInputStream = rssShuffleClient.readPartition(rssApplicationId, shuffleId, partitionId,
            srcAttemptId.getAttemptNumber(), mapIndexStart, mapIndexEnd);
      } else {
        LOG.warn("RssFetcher.shutdown() is called before it connects to RSS. Stop running RssFetcher");
        throw new IllegalStateException("Detected shutdown");
      }
    }

    if (fetchedInput.getType() == FetchedInput.Type.MEMORY) {
      shuffleToMemory(rssShuffleInputStream, (MemoryFetchedInput) fetchedInput);
    } else if (fetchedInput.getType() == FetchedInput.Type.DISK) {
      shuffleToDisk(rssShuffleInputStream, (DiskFetchedInput) fetchedInput);
    } else {
      rssShuffleInputStream.close();
      throw new TezUncheckedException("Unknown FetchedInput.Type: " + fetchedInput);
    }

    long copyDuration = System.currentTimeMillis() - startTime;
    if (readPartitionAllOnce) {
      LOG.info("RssFetcher finished with readPartitionAllOnce: {}, num={}",
          srcAttemptId, srcAttemptId.getInputIdentifiersForReadPartitionAllOnce().size());
      // fetchSucceeded(srcAttemptId) must be called before the loop so as to mark completion correctly
      fetcherCallback.fetchSucceeded(host, srcAttemptId, fetchedInput, dataLength, dataLength, copyDuration);
      for (InputAttemptIdentifier inputIdentifier: srcAttemptId.getInputIdentifiersForReadPartitionAllOnce()) {
        // fetchedInput == null, so mark completion
        fetcherCallback.fetchSucceeded(host, inputIdentifier, null, 0L, 0L, 0L);
      }
    } else {
      fetcherCallback.fetchSucceeded(host, srcAttemptId, fetchedInput, dataLength, dataLength, copyDuration);
    }

    return new FetchResult(host, port, partitionId, 1, new ArrayList<>());
  }

  public void shutdown() {
    try {
      synchronized (lock) {
        isShutdown = true;
        if (rssShuffleInputStream != null) {
          rssShuffleInputStream.close();
        }
      }
    } catch (IOException e) {
      LOG.warn("Fail to close rssShuffleInputStream while shutting down RssFetcher.", e);
    }
  }

  private void shuffleToMemory(InputStream inputStream, MemoryFetchedInput fetchedInput) throws IOException {
    try {
      byte[] buffer = fetchedInput.getBytes();
      IOUtils.readFully(inputStream, buffer, 0, (int) dataLength);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      WritableUtils.writeVInt(dos, IFile.EOF_MARKER);
      WritableUtils.writeVInt(dos, IFile.EOF_MARKER);
      byte[] eofMarker = baos.toByteArray();

      System.arraycopy(eofMarker, 0, buffer, (int) dataLength, EOF_MARKERS_SIZE);

    } catch (IOException e) {
      LOG.error("Failed to read shuffle data from rssShuffleInputStream", e);
      throw e;
    } finally {
      inputStream.close();
    }
  }

  private void shuffleToDisk(InputStream inputStream, DiskFetchedInput fetchedInput) throws IOException {
    try (OutputStream diskOutputStream = fetchedInput.getOutputStream()) {
      byte[] buffer = new byte[BUFFER_SIZE];
      boolean reachEOF = false;
      int bytesWritten = 0;
      while (!reachEOF) {
        int curBytesRead = inputStream.read(buffer, 0, BUFFER_SIZE);

        if (curBytesRead <= 0) {
          reachEOF = true;
        } else {
          reachEOF = curBytesRead < BUFFER_SIZE;

          diskOutputStream.write(buffer, 0, curBytesRead);
          bytesWritten += curBytesRead;
        }
      }

      assert (!(!dataLengthUnknown()) || bytesWritten == dataLength);

      DataOutputStream dos = new DataOutputStream(diskOutputStream);
      WritableUtils.writeVInt(dos, IFile.EOF_MARKER);
      WritableUtils.writeVInt(dos, IFile.EOF_MARKER);
      bytesWritten += EOF_MARKERS_SIZE;

      if (dataLengthUnknown()) {
        fetchedInput.setSize(bytesWritten);
      }
    } finally {
      inputStream.close();
    }
  }

  private boolean dataLengthUnknown() {
    return dataLength == -1;
  }
}
