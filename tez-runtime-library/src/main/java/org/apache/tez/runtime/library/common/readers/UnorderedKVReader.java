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

  private enum LogicalMode {
    UNDECIDED,
    KEY_VALUE,
    VECTOR_BATCH
  }

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
    assert logicalMode != LogicalMode.VECTOR_BATCH;
    assert !(currentReader != null) || !currentReader.isVectorBatch();

    if (readNextFromCurrentReader()) {
      inputRecordCounter.increment(1);
      numRecordsRead++;
      return true;
    }
    while (moveToNextInput()) {
      assert !currentReader.isVectorBatch();
      if (readNextFromCurrentReader()) {
        inputRecordCounter.increment(1);
        numRecordsRead++;
        return true;
      }
    }
    finishInput();
    return false;
  }

  @Override
  public NextResult nextVectorBatchAware() throws IOException {
    assert logicalMode != LogicalMode.KEY_VALUE;

    if (logicalMode == LogicalMode.UNDECIDED) {
      if (currentReader == null && !moveToNextInput()) {
        finishInput();
        return NextResult.END_OF_INPUT;
      }
      if (!currentReader.isVectorBatch()) {
        logicalMode = LogicalMode.KEY_VALUE;
        return NextResult.KEY_VALUE;
      }
      logicalMode = LogicalMode.VECTOR_BATCH;
    }

    // Reaching here means either:
    //  - we just opened a vector-batch reader, or
    //  - a previous call returned VECTOR_BATCH and did not reach END_OF_INPUT.
    // The caller must NOT call this method again after END_OF_INPUT.
    assert currentReader != null;

    while (true) {
      assert currentReader != null;
      if (!currentReader.isVectorBatch()) {
        throw new IOException("Mixed non-empty upstream IFile record formats");
      }
      if (currentReader.nextRawVectorValue(value)) {
        inputRecordCounter.increment(1);
        numRecordsRead++;
        return NextResult.VECTOR_BATCH;
      }
      if (!moveToNextInput()) {
        finishInput();
        return NextResult.END_OF_INPUT;
      }
    }
  }

  private void finishInput() {
    completedProcessing = true;
  }

  // The backing byte[] array of key is immutable, so the consumer may keep pointers to it.
  @Override
  public BytesWritable getCurrentKey() throws IOException {
    return key;
  }

  // The backing byte[] array of value is immutable, so the consumer may keep pointers to it.
  @Override
  public BytesWritable getCurrentValue() throws IOException {
    return value;
  }

  @Override
  public long consumeAll(KeyValueReaderEdge.ThrowingBiConsumer<BytesWritable, BytesWritable> consumer) throws Exception {
    assert logicalMode != LogicalMode.VECTOR_BATCH;
    assert numRecordsRead == 0L;  // must not be mixed with next()
    // currentReader != null if this call is made after nextVectorBatchAware() returns NextResult.KEY_VALUE
    assert !(currentReader != null) || (logicalMode == LogicalMode.KEY_VALUE);

    if (currentReader != null) {
      assert !currentReader.isVectorBatch();
      consumeCurrentReader(consumer);
    }
    while (moveToNextInput()) {
      if (currentReader.isVectorBatch()) {
        throw new IOException("Mixed non-empty upstream IFile record formats");
      }
      consumeCurrentReader(consumer);
    }
    finishInput();
    return numRecordsRead;
  }

  private void consumeCurrentReader(
      KeyValueReaderEdge.ThrowingBiConsumer<BytesWritable, BytesWritable> consumer) throws Exception {
    long currentConsumed = currentReader.consumeAll(consumer);
    inputRecordCounter.increment(currentConsumed);
    numRecordsRead += currentConsumed;
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
