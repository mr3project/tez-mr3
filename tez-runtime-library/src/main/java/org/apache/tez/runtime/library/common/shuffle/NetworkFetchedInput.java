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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tez.common.Preconditions;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkFetchedInput extends FetchedInput {

  private static final Logger LOG = LoggerFactory.getLogger(NetworkFetchedInput.class);

  private boolean fetchStarted = false;
  private long compressedSize;
  private long decompressedSize;

  private InputStream inputStream = null;
  private boolean inputStreamClosed = false;

  private AtomicInteger latch = null;

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
    return inputStream;
  }

  public void initialize(InputStream inputStream, AtomicInteger latch) {
    this.inputStream = inputStream;
    this.latch = latch;
  }

  public void start() throws IOException {
    if (!fetchStarted) {
      fetchStarted = true;
      DataInputStream dis = new DataInputStream(inputStream);
      compressedSize = dis.readLong();
      decompressedSize = dis.readLong();
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
      closeInputStream();
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
        int numPendingInputs = latch.decrementAndGet();
        if (numPendingInputs == 0) {
          closeInputStream();
        }
      } catch (IOException e) {
        LOG.warn("Failed to close input stream");
      }
    }
  }

  private void closeInputStream() throws IOException {
    assert isState(State.FREED) || isState(State.ABORTED);
    if (inputStream != null && !inputStreamClosed) {
      inputStream.close();
      inputStreamClosed = true;
    }
  }

  @Override
  public String toString() {
    return "NetworkFetchedInput ["
        + ", inputAttemptIdentifier=" + getInputAttemptIdentifier()
        + ", type=" + getType() + ", id=" + getId() + ", state=" + getState() + "]";
  }
}

