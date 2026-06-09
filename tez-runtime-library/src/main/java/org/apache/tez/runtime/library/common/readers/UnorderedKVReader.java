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
import org.apache.tez.runtime.library.common.shuffle.ShuffleClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.library.api.KeyValueReaderEdge;
import org.apache.tez.runtime.library.api.KeyValueReaderEdgeVector;
import org.apache.tez.runtime.library.common.shuffle.impl.ShuffleManager;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.InMemoryReader;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.MemoryFetchedInput;

public class UnorderedKVReader extends KeyValueReaderEdge implements KeyValueReaderEdgeVector {

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
  private IFile.KeyValueReaderBytesWritable currentReader;
  
  private long numRecordsRead = 0;
  private enum ReaderState {
    INITIAL,
    CURRENT_KEY_VALUE,
    CURRENT_VECTOR_BATCH,
    CONSUMING_ALL,
    END_OF_INPUT,
    FAILED
  }

  private enum LogicalMode {
    UNDECIDED,
    KEY_VALUE,
    VECTOR_BATCH
  }

  private ReaderState state = ReaderState.INITIAL;
  private LogicalMode logicalMode = LogicalMode.UNDECIDED;

  public UnorderedKVReader(ShuffleManager shuffleManager, Configuration conf,
      CompressionCodec codec, boolean ifileReadAhead, int ifileReadAheadLength,
      TezCounter inputRecordCounter, InputContext context) {
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
    if (state != ReaderState.INITIAL) {
      throw new RuntimeException("next() is only valid as the initial advance operation");
    }
    try {
      if (advanceKeyValue()) {
        state = ReaderState.CURRENT_KEY_VALUE;
        return true;
      }
      finishInput();
      return false;
    } catch (IOException e) {
      state = ReaderState.FAILED;
      throw e;
    }
  }

  @Override
  public NextResult nextVectorBatchAware() throws IOException {
    if (state == ReaderState.END_OF_INPUT) {
      throw new IOException("Input is already exhausted");
    }
    if (state != ReaderState.INITIAL && state != ReaderState.CURRENT_VECTOR_BATCH) {
      throw new RuntimeException("Invalid vector-aware advance in state " + state);
    }
    try {
      while (true) {
        if (currentReader == null && !moveToNextInput()) {
          finishInput();
          return NextResult.END_OF_INPUT;
        }
        boolean vector = currentReader.isVectorBatch();
        boolean hasRecord;
        if (vector) {
          hasRecord = currentReader.nextRawVectorValue(value);
        } else {
          hasRecord = readNextFromCurrentReader();
        }
        if (!hasRecord) {
          if (!moveToNextInput()) {
            finishInput();
            return NextResult.END_OF_INPUT;
          }
          continue;
        }
        LogicalMode physicalMode = vector ? LogicalMode.VECTOR_BATCH : LogicalMode.KEY_VALUE;
        establishMode(physicalMode);
        inputRecordCounter.increment(1);
        numRecordsRead++;
        state = vector ? ReaderState.CURRENT_VECTOR_BATCH : ReaderState.CURRENT_KEY_VALUE;
        return vector ? NextResult.VECTOR_BATCH : NextResult.KEY_VALUE;
      }
    } catch (IOException e) {
      state = ReaderState.FAILED;
      throw e;
    }
  }

  private boolean advanceKeyValue() throws IOException {
    while (true) {
      if (currentReader == null && !moveToNextInput()) {
        return false;
      }
      if (currentReader.isVectorBatch()) {
        throw new IOException("Vector-batch input requires nextVectorBatchAware()");
      }
      if (readNextFromCurrentReader()) {
        establishMode(LogicalMode.KEY_VALUE);
        inputRecordCounter.increment(1);
        numRecordsRead++;
        return true;
      }
      if (!moveToNextInput()) {
        return false;
      }
    }
  }

  private void establishMode(LogicalMode physicalMode) throws IOException {
    if (logicalMode == LogicalMode.UNDECIDED) {
      logicalMode = physicalMode;
    } else if (logicalMode != physicalMode) {
      throw new IOException("Mixed non-empty upstream IFile record formats");
    }
  }

  private void finishInput() {
    LOG.info("Num Records read: {}", numRecordsRead);
    completedProcessing = true;
    state = ReaderState.END_OF_INPUT;
  }

  // The backing byte[] array of key is immutable, so the consumer may keep pointers to it.
  @Override
  public BytesWritable getCurrentKey() throws IOException {
    if (state != ReaderState.CURRENT_KEY_VALUE) {
      throw new RuntimeException("Current key is unavailable in state " + state);
    }
    return key;
  }

  // The backing byte[] array of value is immutable, so the consumer may keep pointers to it.
  @Override
  public BytesWritable getCurrentValue() throws IOException {
    if (state != ReaderState.CURRENT_KEY_VALUE && state != ReaderState.CURRENT_VECTOR_BATCH) {
      throw new RuntimeException("Current value is unavailable in state " + state);
    }
    return value;
  }

  @Override
  public long consumeAll(KeyValueReaderEdge.ThrowingBiConsumer<BytesWritable, BytesWritable> consumer) throws Exception {
    if (state != ReaderState.CURRENT_KEY_VALUE) {
      throw new RuntimeException("consumeAll() requires one prepared key/value record");
    }
    state = ReaderState.CONSUMING_ALL;
    try {
      consumer.accept(key, value);
      while (advanceKeyValue()) {
        consumer.accept(key, value);
      }
      finishInput();
      return numRecordsRead;
    } catch (Exception e) {
      state = ReaderState.FAILED;
      throw e;
    }
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
      boolean hasMore = this.currentReader.readRawKey(key) != IFile.Reader.KeyState.NO_KEY;
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

  private IFile.KeyValueReaderBytesWritable openIFileReader(FetchedInput fetchedInput)
      throws IOException {
    if (fetchedInput.getType() == ShuffleClient.Type.MEMORY) {
      MemoryFetchedInput mfi = (MemoryFetchedInput) fetchedInput;
      return new InMemoryReader(null, mfi.getInputAttemptIdentifier(),
          mfi.getBytes(), 0, (int) mfi.getSize(), 0, mfi.getTezOffsetRecord());
    } else {
      return new IFile.Reader(fetchedInput.getInputStream(),
          fetchedInput.getSize(), codec, null, null,
          ifileReadAhead, ifileReadAheadLength, context, fetchedInput.getTezOffsetRecord());
    }
  }
}
