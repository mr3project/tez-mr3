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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.tez.common.Preconditions;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkFetchedInput extends FetchedInput {

  private static final Logger LOG = LoggerFactory.getLogger(NetworkFetchedInput.class);

  private boolean fetchStarted = false;
  private long compressedSize;

  private TezCounter compressedBytesCounter;
  private TezCounter decompressedBytesCounter;

  SharedInputStream sharedInputStream;

  public NetworkFetchedInput(
      InputAttemptIdentifier inputAttemptIdentifier,
      FetchedInputCallback callbackHandler) {
    super(inputAttemptIdentifier, callbackHandler);
  }

  @Override
  public Type getType() {
    return Type.NETWORK;
  }

  @Override
  public long getSize() {
    if (fetchStarted) {
      return compressedSize;
    } else {
      throw new IllegalStateException("getSize() is available after start() has been called");
    }
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return null;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return sharedInputStream.getInputStream();
  }

  public void initialize(SharedInputStream sharedInputStream, TezCounter compressedBytesCounter,
      TezCounter decompressedBytesCounter) {
    this.sharedInputStream = sharedInputStream;
    this.compressedBytesCounter = compressedBytesCounter;
    this.decompressedBytesCounter = decompressedBytesCounter;
  }

  public void start() throws IOException {
    if (!fetchStarted) {
      fetchStarted = true;
      DataInputStream dis = new DataInputStream(sharedInputStream.getInputStream());
      compressedSize = dis.readLong();
      long decompressedSize = dis.readLong();

      compressedBytesCounter.increment(compressedSize);
      decompressedBytesCounter.increment(decompressedSize);
    }
  }

  @Override
  public void commit() throws IOException {
    if (isState(State.PENDING)) {
      setState(State.COMMITTED);
    }
  }

  @Override
  public void abort() throws IOException {
    if (isState(State.PENDING)) {
      setState(State.ABORTED);
      sharedInputStream.abort();
    }
  }

  @Override
  public void free() {
    Preconditions.checkState(
        isState(State.COMMITTED) || isState(State.ABORTED),
        "FetchedInput can only be freed after it is committed or aborted");
    if (isState(State.COMMITTED)) {
      setState(State.FREED);
      try {
        sharedInputStream.finish();
      } catch (IOException e) {
        LOG.warn("Failed to close input stream");
      }
    }
  }

  @Override
  public String toString() {
    return "NetworkFetchedInput ["
        + ", inputAttemptIdentifier=" + getInputAttemptIdentifier()
        + ", type=" + getType() + ", id=" + getId() + ", state=" + getState() + "]";
  }

  public static class SharedInputStream {
    private final ShuffleClient rssShuffleClient;
    private final int shuffleId;
    private final int mapIndexStart;
    private final int mapIndexEnd;
    private final int reduceAttemptNumber;
    private final int reducePartitionId;

    private final Object lock = new Object();

    // guarded by lock
    private InputStream inputStream = null;
    private boolean isClosed = false;
    private int numRecipient;

    public SharedInputStream(
        ShuffleClient rssShuffleClient,
        int shuffleId,
        int mapIndexStart,
        int mapIndexEnd,
        int reduceAttemptNumber,
        int reducePartitionId,
        int numRecipient) {
      this.rssShuffleClient = rssShuffleClient;
      this.shuffleId = shuffleId;
      this.mapIndexStart = mapIndexStart;
      this.mapIndexEnd = mapIndexEnd;
      this.reduceAttemptNumber = reduceAttemptNumber;
      this.reducePartitionId = reducePartitionId;
      this.numRecipient = numRecipient;
    }

    public InputStream getInputStream() throws IOException {
      synchronized (lock) {
        if (isClosed) {
          throw new IOException("RSS InputStream already closed");
        }

        if (inputStream == null) {
          inputStream = rssShuffleClient.readPartition(shuffleId, reducePartitionId, reduceAttemptNumber,
              mapIndexStart, mapIndexEnd);
        }

        return inputStream;
      }
    }

    public void finish() throws IOException {
      synchronized (lock) {
        if (isClosed) {
          throw new IOException("RSS InputStream already closed");
        }

        numRecipient = numRecipient - 1;
        if (numRecipient == 0) {
          assert inputStream != null;
          inputStream.close();
          isClosed = true;
        }
      }
    }

    public void abort() throws IOException {
      synchronized (lock) {
        if (!isClosed && inputStream != null) {
          inputStream.close();
          isClosed = true;
        }
      }
    }
  }
}
