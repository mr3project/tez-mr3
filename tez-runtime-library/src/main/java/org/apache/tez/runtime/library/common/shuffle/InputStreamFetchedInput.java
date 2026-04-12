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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.tez.common.Preconditions;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;

/**
 * Fetched input backed by a single-use InputStream.
 */
public class InputStreamFetchedInput extends FetchedInput {

  private InputStream inputStream;
  private final long startOffset;
  private final long size;
  private boolean streamAccessed;

  public InputStreamFetchedInput(
      InputStream inputStream,
      long startOffset,
      long size,
      InputAttemptIdentifier inputAttemptIdentifier,
      FetchedInputCallback callbackHandler) {
    super(inputAttemptIdentifier, callbackHandler);
    this.inputStream = inputStream;
    this.startOffset = startOffset;
    this.size = size;
    this.streamAccessed = false;
  }

  @Override
  public Type getType() {
    return Type.LOCAL_BYTE_CACHE;
  }

  @Override
  public long getSize() {
    return size;
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    throw new IOException("Output Stream is not supported for " + this.toString());
  }

  @Override
  public InputStream getInputStream() throws IOException {
    Preconditions.checkState(!streamAccessed,
        "InputStreamFetchedInput.getInputStream() can be called at most once");
    streamAccessed = true;

    long remaining = startOffset;
    while (remaining > 0) {
      long skipped = inputStream.skip(remaining);
      if (skipped <= 0) {
        throw new IOException("Failed to seek input stream to offset " + startOffset);
      }
      remaining -= skipped;
    }

    return new BoundedInputStream(inputStream, size);
  }

  @Override
  public void commit() {
    if (isState(State.PENDING)) {
      setState(State.COMMITTED);
      notifyFetchComplete();
    }
  }

  @Override
  public void abort() throws IOException {
    if (isState(State.PENDING)) {
      setState(State.ABORTED);
      if (inputStream != null) {
        inputStream.close();
      }
      inputStream = null;
      notifyFetchFailure();
    }
  }

  @Override
  public void free() {
    Preconditions.checkState(
        isState(State.COMMITTED) || isState(State.ABORTED),
        "FetchedInput can only be freed after it is committed or aborted");
    if (isState(State.COMMITTED)) {
      setState(State.FREED);
      notifyFreedResource();
    }
  }
}
