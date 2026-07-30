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

package org.apache.tez.runtime.library.input;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.runtime.library.api.LogicalInputEdge;
import org.apache.tez.runtime.api.ProgressFailedException;
import org.apache.tez.runtime.library.api.KeyValueReaderEdge;
import org.apache.tez.runtime.library.common.shuffle.ShuffleServer;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.readers.UnorderedKVReader;
import org.apache.tez.runtime.library.common.shuffle.ShuffleEventHandler;
import org.apache.tez.runtime.library.common.shuffle.impl.ShuffleInputEventHandlerImpl;
import org.apache.tez.runtime.library.common.shuffle.impl.ShuffleManager;
import org.apache.tez.runtime.library.common.shuffle.impl.SimpleFetchedInputAllocator;
import org.apache.tez.runtime.library.utils.CodecUtils;
import org.apache.tez.common.Preconditions;

/**
 * {@link UnorderedKVInput} provides unordered key value input by
 * bringing together (shuffling) a set of distributed data and providing a 
 * unified view to that data. There are no ordering constraints applied by
 * this input.
 */
public class UnorderedKVInput extends AbstractLogicalInput implements LogicalInputEdge {

  private static final Logger LOG = LoggerFactory.getLogger(UnorderedKVInput.class);

  private Configuration conf;
  private ShuffleManager shuffleManager;
  private final BlockingQueue<Event> pendingEvents = new LinkedBlockingQueue<Event>();
  private long firstEventReceivedTime = -1;
  private MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;
  private UnorderedKVReader kvReader;

  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private TezCounter inputRecordCounter;

  private SimpleFetchedInputAllocator inputManager;
  private ShuffleEventHandler inputEventHandler;

  private boolean isClosed = false;

  private boolean compositeFetch;

  public UnorderedKVInput(InputContext inputContext, int numPhysicalInputs) {
    super(inputContext, numPhysicalInputs);
  }

  @Override
  public synchronized List<Event> initialize() throws Exception {
    Preconditions.checkArgument(getNumPhysicalInputs() != -1, "Number of Inputs has not been set");

    Configuration commonJobConf = getContext().getCommonJobConf(false);  // not null
    Configuration vertexJobConfDiff = getContext().getConfigurationFromUserPayload(false);
    this.conf = new Configuration(false);
    for (Map.Entry<String, String> kv : commonJobConf) {
      this.conf.set(kv.getKey(), kv.getValue());
    }
    for (Map.Entry<String, String> kv : vertexJobConfDiff) {
      this.conf.set(kv.getKey(), kv.getValue());
    }

    this.compositeFetch = ShuffleUtils.isTezShuffleHandler(conf);

    if (getNumPhysicalInputs() == 0) {
      getContext().requestInitialMemory(0L, null);
      isStarted.set(true);
      getContext().inputIsReady();
      LOG.info("input fetch not required since there are 0 physical inputs for input vertex: {}",
          getContext().getSourceVertexName());
      return Collections.emptyList();
    }

    this.memoryUpdateCallbackHandler = new MemoryUpdateCallbackHandler();
    getContext().requestInitialMemory(
        SimpleFetchedInputAllocator.getInitialMemoryReq(conf, getContext().getTotalMemoryAvailableToTask()),
        memoryUpdateCallbackHandler);

    this.conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, getContext().getWorkDirs());
    this.inputRecordCounter = getContext().getCounters().findCounter(TaskCounter.INPUT_RECORDS);
    return Collections.emptyList();
  }

  @Override
  public synchronized void start() throws IOException {
    if (!isClosed && !isStarted.get()) {
      ////// Initial configuration
      memoryUpdateCallbackHandler.validateUpdateReceived();

      Object shuffleServer = getContext().peekShuffleServer();
      Configuration codecConf = ShuffleServer.getCodecConf(shuffleServer, conf);
      Class<? extends CompressionCodec> codecClass =
          ShuffleServer.getCodecClass(shuffleServer, codecConf);
      CompressionCodec codec = CodecUtils.getCodec(
          codecConf,
          codecClass,
          ShuffleServer.getCodecBufferSize(shuffleServer, codecConf, codecClass));

      boolean ifileReadAhead = conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD,
          TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT);
      int ifileReadAheadLength = 0;

      if (ifileReadAhead) {
        ifileReadAheadLength = conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
            TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT);
      }

      InputContext inputContext = getContext();
      String srcNameTrimmed = TezUtilsInternal.cleanVertexName(inputContext.getSourceVertexName());
      long assignedMemoryBytes = memoryUpdateCallbackHandler.getMemoryAssigned();
      this.inputManager = new SimpleFetchedInputAllocator(
          srcNameTrimmed,
          inputContext.getUniqueIdentifierForOutputFiles(),
          inputContext.getDagIdentifier(), conf,
          inputContext.getTotalMemoryAvailableToTask(),
          assignedMemoryBytes,
          inputContext.getExecutionContext().getEnvContainerId(),
          inputContext.getTaskVertexIndex(), compositeFetch);

      // UnorderedKVInput
      LOG.info("{} SimpleFetchedInputAllocator for {}: assignedMemoryBytes={}",
          inputContext.getTaskAttemptIdStr(), srcNameTrimmed,
          assignedMemoryBytes);

      this.shuffleManager = new ShuffleManager(inputContext, conf, getNumPhysicalInputs(), inputManager, srcNameTrimmed);
      this.inputEventHandler = new ShuffleInputEventHandlerImpl(inputContext, shuffleManager,
          inputManager, codec, ifileReadAhead, ifileReadAheadLength, compositeFetch);

      ////// End of Initial configuration

      this.shuffleManager.run();
      this.kvReader = createReader(inputRecordCounter, codec,
          ifileReadAhead, ifileReadAheadLength);
      List<Event> pending = new LinkedList<Event>();
      pendingEvents.drainTo(pending);
      if (!pending.isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(inputContext.getSourceVertexName() + ": NoAutoStart delay in processing first event: "
              + (System.currentTimeMillis() - firstEventReceivedTime));
        }
        inputEventHandler.handleEvents(pending);
      }
      isStarted.set(true);
    }
  }

  @Override
  public synchronized KeyValueReaderEdge getReader() throws Exception {
    Preconditions.checkState(isStarted.get(), "Must start input before invoking this method");
    if (getNumPhysicalInputs() == 0) {
      return new KeyValueReaderEdge() {
        @Override
        public boolean next() throws IOException {
          hasCompletedProcessing();
          completedProcessing = true;
          return false;
        }

        @Override
        public BytesWritable getCurrentKey() throws IOException {
          throw new RuntimeException("No data available in Input");
        }

        @Override
        public BytesWritable getCurrentValue() throws IOException {
          throw new RuntimeException("No data available in Input");
        }

        @Override
        public long consumeAll(
            Runnable setupForEachReader,
            KeyValueReaderEdge.ThrowingBiConsumer<BytesWritable, BytesWritable> consumer) throws Exception {
          hasCompletedProcessing();
          completedProcessing = true;
          return 0L;
        }
      };
    }
    return this.kvReader;
  }

  @Override
  public void handleEvents(List<Event> inputEvents) throws IOException {
    ShuffleEventHandler inputEventHandlerLocalRef;
    synchronized (this) {
      if (getNumPhysicalInputs() == 0) {
        throw new RuntimeException("No input events expected as numInputs is 0");
      }
      if (!isStarted.get()) {
        if (firstEventReceivedTime == -1) {
          firstEventReceivedTime = System.currentTimeMillis();
        }
        // This queue will keep growing if the Processor decides never to
        // start the event. The Input, however has no idea, on whether start
        // will be invoked or not.
        pendingEvents.addAll(inputEvents);
        return;
      }
      inputEventHandlerLocalRef = inputEventHandler;
    }
    inputEventHandlerLocalRef.handleEvents(inputEvents);
  }

  @Override
  public synchronized List<Event> close() throws Exception {
    if (this.inputEventHandler != null) {
      this.inputEventHandler.logProgress(true);
    }

    if (this.shuffleManager != null) {
      this.shuffleManager.shutdown();
    }
    
    isClosed = true;

    return null;
  }

  private UnorderedKVReader createReader(TezCounter inputRecordCounter, CompressionCodec codec,
      boolean ifileReadAheadEnabled, int ifileReadAheadLength)
      throws IOException {
    return new UnorderedKVReader(shuffleManager, conf, codec, ifileReadAheadEnabled,
        ifileReadAheadLength, inputRecordCounter, getContext());
  }

  private static final Set<String> confKeys = new HashSet<String>();

  static {
    confKeys.add(TezRuntimeConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_FETCHED_INPUT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_FREE_MEMORY_FACTOR_FOR_FETCHED_INPUT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_UNORDERED_MEMORY_STREAMING);
    // include to be able to override TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_SPECULATIVE_FETCH_WAIT_MILLIS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_STUCK_FETCHER_THRESHOLD_MILLIS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_STUCK_FETCHER_RELEASE_MILLIS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_SPECULATIVE_FETCH_ATTEMPTS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
  }

  public static Set<String> getConfigurationKeySet() {
    return Collections.unmodifiableSet(confKeys);
  }

  @Override
  public float getProgress() throws ProgressFailedException, InterruptedException {
    try {
      return kvReader.getProgress();
    } catch (IOException e) {
      throw new ProgressFailedException("getProgress encountered IOException ", e);
    }
  }
}
