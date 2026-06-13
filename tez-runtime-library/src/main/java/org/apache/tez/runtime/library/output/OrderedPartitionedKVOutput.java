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
package org.apache.tez.runtime.library.output;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.Deflater;

import com.google.common.collect.Lists;

import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.runtime.library.api.LogicalOutputEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.KeyValuesWriterEdge;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.sort.impl.PipelinedSorter;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;

import org.apache.tez.common.Preconditions;

/**
 * {@link OrderedPartitionedKVOutput} is an {@link AbstractLogicalOutput} which sorts
 * key/value pairs written to it. It also partitions the output based on a
 * {@link Partitioner}
 */
public class OrderedPartitionedKVOutput extends AbstractLogicalOutput implements LogicalOutputEdge {

  private static final Logger LOG = LoggerFactory.getLogger(OrderedPartitionedKVOutput.class);

  protected PipelinedSorter sorter;
  protected Configuration conf;
  private RawLocalFileSystem localFs;
  protected MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private final Deflater deflater;

  boolean isPipelinedShuffle;
  boolean isFinalMergeEnabled;

  private String auxiliaryService;
  private boolean compositeFetch;

  public OrderedPartitionedKVOutput(OutputContext outputContext, int numPhysicalOutputs) {
    super(outputContext, numPhysicalOutputs);
    deflater = TezCommonUtils.newBestCompressionDeflater();
  }

  @Override
  public synchronized List<Event> initialize() throws IOException {
    this.conf = getContext().getConfigurationFromUserPayload(true);
    this.localFs = (RawLocalFileSystem) FileSystem.getLocal(conf).getRaw();

    // Initializing this parameter in this conf since it is used in multiple
    // places (wherever LocalDirAllocator is used) - TezTaskOutputFiles, TezMerger, etc.
    this.conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, getContext().getWorkDirs());
    this.memoryUpdateCallbackHandler = new MemoryUpdateCallbackHandler();
    getContext().requestInitialMemory(
        PipelinedSorter.getInitialMemoryRequirement(conf,
            getContext().getTotalMemoryAvailableToTask()), memoryUpdateCallbackHandler);

    auxiliaryService = ShuffleUtils.getTezShuffleHandlerServiceId(conf);
    compositeFetch = ShuffleUtils.isTezShuffleHandler(conf);

    return Collections.emptyList();
  }

  @Override
  public synchronized void start() throws Exception {
    if (!isStarted.get()) {
      memoryUpdateCallbackHandler.validateUpdateReceived();

      // Keep this mode in sync with PipelinedSorter. In pipelined mode the sorter returns the
      // lastEvent=true DME; in final-merge mode this output generates the single final DME.
      isPipelinedShuffle = conf.getBoolean(
          TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ORDERED_ENABLED,
          TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ORDERED_ENABLED_DEFAULT);
      // We do not use TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT.
      isFinalMergeEnabled = !this.isPipelinedShuffle;

      sorter = new PipelinedSorter(getContext(), conf, getNumPhysicalOutputs(),
          memoryUpdateCallbackHandler.getMemoryAssigned());

      isStarted.set(true);
    }
  }

  @Override
  public synchronized KeyValuesWriterEdge getWriter() throws IOException {
    Preconditions.checkState(isStarted.get(), "Cannot get writer before starting the Output");
    return new KeyValuesWriterEdge() {
      @Override
      public void closeWriter() {
        sorter.closeWriter();
      }

      @Override
      public void write(BytesWritable key, BytesWritable value) throws IOException {
        sorter.write(key, value);
      }

      @Override
      public void write(BytesWritable key, Iterable<BytesWritable> values) throws IOException {
        sorter.write(key, values);
      }

      @Override
      public int getNumUnorderedPartitions() {
        return -1;  // because this is ordered
      }
    };
  }

  @Override
  public synchronized void handleEvents(List<Event> outputEvents) {
    // Not expecting any events.
  }

  @Override
  public synchronized List<Event> close() throws IOException {
    List<Event> returnEvents = Lists.newLinkedList();
    if (sorter != null) {
      sorter.flush();
      returnEvents.addAll(sorter.close());
      returnEvents.addAll(generateEvents());
      sorter = null;
    } else {
      LOG.warn(getContext().getDestinationVertexName() +
          ": Attempting to close output {} of type {} before it was started. Generating empty events",
          getContext().getDestinationVertexName(), this.getClass().getSimpleName());
      returnEvents = generateEmptyEvents();
    }

    return returnEvents;
  }

  private List<Event> generateEvents() throws IOException {
    List<Event> eventList = Lists.newLinkedList();
    if (!isPipelinedShuffle) {
      assert isFinalMergeEnabled;
      if (!sorter.getFinalIndexComputed()) {
        // return an empty list because TezSpillRecord() throws NPE
        // this occurs when PipelinedSorter threads gets interrupted and LogicalOutput.close() is closed
        return eventList;
      }

      // In PipelinedSorter.flush() skips renaming output directories if finalMergeEnabled == true && numSpills == 1.
      // Here we adjust pathComponent in accordance so that downstream tasks can request, e.g., ".../...10031_0/file.out".
      String pathComponent = (sorter.getNumSpills() == 1) ?
          getContext().getUniqueIdentifier() + "_0" :   // use original output directory ".../...10031_0"
          getContext().getUniqueIdentifier();           // use renamed output directory ".../...10031"
      String mapId = compositeFetch ?
          ShuffleUtils.buildTezShuffleMapId(getContext().getTaskVertexIndex(), pathComponent) : pathComponent;
      TezSpillRecord tezSpillRecord = ShuffleUtils.getTezSpillRecord(
          getContext(), mapId, sorter.getFinalIndexFile(), localFs);

      boolean isLastEvent = true;
      ShuffleUtils.generateEventOnSpill(eventList, isFinalMergeEnabled, isLastEvent,
          getContext(), 0, tezSpillRecord,
          getNumPhysicalOutputs(), pathComponent,
          sorter.getPartitionStats(), sorter.reportDetailedPartitionStats(), auxiliaryService, deflater);
    }
    return eventList;
  }

  private List<Event> generateEmptyEvents() throws IOException {
    List<Event> eventList = Lists.newLinkedList();
    ShuffleUtils.generateEventsForNonStartedOutput(eventList, getNumPhysicalOutputs(), getContext(), true, true, deflater);
    return eventList;
  }

  private static final Set<String> confKeys = new HashSet<String>();

  static {
    confKeys.add(TezRuntimeConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_USE_SOFT_REFERENCE);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_RLE_THRESHOLD_FRACTION);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ORDERED_ENABLED);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_FREE_MEMORY_WRITER_OUTPUT_THRESHOLD_MB);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
  }

  public static Set<String> getConfigurationKeySet() {
    return Collections.unmodifiableSet(confKeys);
  }
}
