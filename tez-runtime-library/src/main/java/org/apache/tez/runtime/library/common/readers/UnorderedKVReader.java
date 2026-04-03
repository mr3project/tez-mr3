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

package org.apache.tez.runtime.library.common.readers;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.IOInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.library.api.KeyValueReaderEdge;
import org.apache.tez.runtime.library.common.shuffle.impl.ShuffleManager;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.InMemoryReader;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput.Type;
import org.apache.tez.runtime.library.common.shuffle.MemoryFetchedInput;

public class UnorderedKVReader extends KeyValueReaderEdge {

  private static final Logger LOG = LoggerFactory.getLogger(UnorderedKVReader.class);
  
  private final ShuffleManager shuffleManager;
  private final CompressionCodec codec;
  
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;

  private final TezCounter inputRecordCounter;
  private final InputContext context;
  
  private final BytesWritable key;
  private final BytesWritable value;
  
  private FetchedInput currentFetchedInput;
  private IFile.Reader currentReader;
  
  // TODO Remove this once per I/O counters are separated properly. Relying on
  // the counter at the moment will generate aggregate numbers. 
  private int numRecordsRead = 0;
  public UnorderedKVReader(ShuffleManager shuffleManager, Configuration conf,
      CompressionCodec codec, boolean ifileReadAhead, int ifileReadAheadLength,
      TezCounter inputRecordCounter, InputContext context)
      throws IOException {
    this.shuffleManager = shuffleManager;
    this.context = context;
    this.codec = codec;
    this.ifileReadAhead = ifileReadAhead;
    this.ifileReadAheadLength = ifileReadAheadLength;
    this.inputRecordCounter = inputRecordCounter;

    this.key = new BytesWritable();
    this.value = new BytesWritable();
  }

  /**
   * Moves to the next key/values(s) pair
   * 
   * @return true if another key/value(s) pair exists, false if there are no more.
   * @throws IOException if an error occurs
   */
  @Override  
  public boolean next() throws IOException {
    if (readNextFromCurrentReader()) {
      inputRecordCounter.increment(1);
      numRecordsRead++;
      return true;
    } else {
      boolean nextInputExists = moveToNextInput();
      while (nextInputExists) {
        if (readNextFromCurrentReader()) {
          inputRecordCounter.increment(1);
          numRecordsRead++;
          return true;
        }
        nextInputExists = moveToNextInput();
      }
      LOG.info("Num Records read: " + numRecordsRead);
      completedProcessing = true;
      return false;
    }
  }

  @Override
  public BytesWritable getCurrentKey() throws IOException {
    return key;
  }

  @Override
  public BytesWritable getCurrentValue() throws IOException {
    return value;
  }

  public float getProgress() throws IOException, InterruptedException {
    return completedProcessing ? 1.0f : 0.0f;
  }

  /**
   * Tries reading the next key and value from the current reader.
   * @return true if the current reader has more records
   * @throws IOException
   */
  private boolean readNextFromCurrentReader() throws IOException {
    if (this.currentReader == null) {
      return false;
    } else {
      boolean hasMore = this.currentReader.nextRawKey(key);
      if (hasMore) {
        this.currentReader.nextRawValue(value);
        return true;
      }
      return false;
    }
  }
  
  /**
   * Moves to the next available input. This method may block if the input is not ready yet.
   * Also takes care of closing the previous input.
   * 
   * @return true if the next input exists, false otherwise
   * @throws IOException
   */
  private boolean moveToNextInput() throws IOException {
    if (currentReader != null) {  // Close the current reader.
      currentReader.close();
      /**
       * clear reader explicitly. Otherwise this could point to stale reference when next() is
       * called and end up throwing EOF exception from IFIle. Ref: TEZ-2348
       */
      currentReader = null;
      currentFetchedInput.free();
    }
    try {
      currentFetchedInput = shuffleManager.getNextInput();
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for next available input", e);
      Thread.currentThread().interrupt();
      throw new IOInterruptedException(e);
    }
    if (currentFetchedInput == null) {
      hasCompletedProcessing();
      return false; // No more inputs
    } else {
      currentReader = openIFileReader(currentFetchedInput);
      return true;
    }
  }

  private IFile.Reader openIFileReader(FetchedInput fetchedInput)
      throws IOException {
    if (fetchedInput.getType() == Type.MEMORY) {
      MemoryFetchedInput mfi = (MemoryFetchedInput) fetchedInput;
      return new InMemoryReader(null, mfi.getInputAttemptIdentifier(),
          mfi.getBytes(), 0, (int) mfi.getSize(), 0);
    } else {
      return new IFile.Reader(fetchedInput.getInputStream(),
          fetchedInput.getSize(), codec, null, null,
          ifileReadAhead, ifileReadAheadLength, context);
    }
  }
}
