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
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

public class RssFetcher implements FetcherBase {
  private final FetcherCallback fetcherCallback;
  private final FetchedInputAllocator inputAllocator;
  private final long dataLength;

  private final ShuffleClient shuffleClient;
  private final String rssApplicationId;
  private final int shuffleId;

  private final String host;
  private final int port;
  private final int partition;
  private final InputAttemptIdentifier srcAttemptId;

  private static final Logger LOG = LoggerFactory.getLogger(RssFetcher.class);

  private final int bufferSize = 64 * 1024;

  private final Object lock = new Object();

  // guarded by lock
  private InputStream rssShuffleInputStream = null;
  private boolean isShutdown = false;

  public RssFetcher(
      FetcherCallback fetcherCallback,
      FetchedInputAllocator inputAllocator,
      ShuffleClient rssShuffleClient,
      String rssApplicationId,
      int shuffleId,
      String host,
      int port,
      int partition,
      InputAttemptIdentifier srcAttemptId) {
    this(fetcherCallback, inputAllocator, rssShuffleClient, rssApplicationId, shuffleId, host, port,
        partition, srcAttemptId, -1);
  }

  public RssFetcher(
      FetcherCallback fetcherCallback,
      FetchedInputAllocator inputAllocator,
      ShuffleClient rssShuffleClient,
      String rssApplicationId,
      int shuffleId,
      String host,
      int port,
      int partition,
      InputAttemptIdentifier srcAttemptId,
      int dataLength) {
    this.fetcherCallback = fetcherCallback;
    this.inputAllocator = inputAllocator;
    this.shuffleClient = rssShuffleClient;
    this.rssApplicationId = rssApplicationId;
    this.shuffleId = shuffleId;
    this.host = host;
    this.port = port;
    this.partition = partition;
    this.srcAttemptId = srcAttemptId;
    this.dataLength = dataLength;
  }

  public FetchResult call() throws Exception {
    long startTime = System.currentTimeMillis();

    FetchedInput fetchedInput;
    if (dataLengthUnknown()) {
      fetchedInput =
          inputAllocator.allocateType(FetchedInput.Type.DISK, dataLength, dataLength, srcAttemptId);
    } else {
      fetchedInput = inputAllocator.allocate(dataLength, dataLength, srcAttemptId);
    }

    int mapIndex = Integer.parseInt(host);
    synchronized (lock) {
      if (!isShutdown) {
        rssShuffleInputStream = shuffleClient.readPartition(rssApplicationId, shuffleId,
            srcAttemptId.getInputIdentifier(), srcAttemptId.getAttemptNumber(), mapIndex, mapIndex + 1);
      } else {
        LOG.warn("RssFetcher.shutdown() is called before it connects to RSS. Stop running RssFetcher");
        throw new IllegalStateException("Detected shutdown");
      }
    }

    if (fetchedInput.getType() == FetchedInput.Type.MEMORY) {
      byte[] buffer = ((MemoryFetchedInput) fetchedInput).getBytes();

      try {
        IOUtils.readFully(rssShuffleInputStream, buffer, 0, (int) dataLength);
      } catch (IOException e) {
        rssShuffleInputStream.close();
        LOG.error("Fail to read shuffle data from rssShfufleInputStream", e);
        throw e;
      }
    } else if (fetchedInput.getType() == FetchedInput.Type.DISK) {
      try (OutputStream diskOutputStream = fetchedInput.getOutputStream()) {
        byte[] buffer = new byte[bufferSize];
        boolean reachEOF = false;
        int bytesWritten = 0;
        while (!reachEOF) {
          int curBytesRead = rssShuffleInputStream.read(buffer, 0, bufferSize);

          if (curBytesRead <= 0) {
            reachEOF = true;
          } else {
            reachEOF = curBytesRead < bufferSize;

            diskOutputStream.write(buffer, 0, curBytesRead);
            bytesWritten += curBytesRead;
          }
        }

        int eofFooterSize = 2 * WritableUtils.getVIntSize(IFile.EOF_MARKER);

        assert (!(!dataLengthUnknown()) || bytesWritten + eofFooterSize == dataLength);

        DataOutputStream dos = new DataOutputStream(diskOutputStream);
        WritableUtils.writeVInt(dos, IFile.EOF_MARKER);
        WritableUtils.writeVInt(dos, IFile.EOF_MARKER);
        bytesWritten += eofFooterSize;

        if (dataLengthUnknown()) {
          ((DiskFetchedInput) fetchedInput).setSize(bytesWritten);
        }
      }
    } else {
      throw new TezUncheckedException("Bad fetchedInput type while fetching shuffle data " + fetchedInput);
    }

    long copyDuration  = System.currentTimeMillis() - startTime;
    fetcherCallback.fetchSucceeded(host, srcAttemptId, fetchedInput, dataLength, dataLength, copyDuration);
    // TODO: fetchFailed()?

    return new FetchResult(host, port, partition, 1, new ArrayList<>());
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
      LOG.warn("Fail to close RSS InputStream while shutting down RssFetcher.", e);
    }
  }

  private boolean dataLengthUnknown() {
    return dataLength == -1;
  }
}

