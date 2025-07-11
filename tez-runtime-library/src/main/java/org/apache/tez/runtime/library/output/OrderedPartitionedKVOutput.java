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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.Deflater;

import com.google.common.collect.Lists;

import org.apache.tez.runtime.library.conf.OrderedPartitionedKVOutputConfig.SorterImpl;
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
import org.apache.tez.runtime.library.api.KeyValuesWriter;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.sort.impl.ExternalSorter;
import org.apache.tez.runtime.library.common.sort.impl.PipelinedSorter;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;

import org.apache.tez.common.Preconditions;

/**
 * {@link OrderedPartitionedKVOutput} is an {@link AbstractLogicalOutput} which sorts
 * key/value pairs written to it. It also partitions the output based on a
 * {@link Partitioner}
 */
public class OrderedPartitionedKVOutput extends AbstractLogicalOutput {

  private static final Logger LOG = LoggerFactory.getLogger(OrderedPartitionedKVOutput.class);

  protected ExternalSorter sorter;
  private boolean usePipelinedSorter = false;
  protected Configuration conf;
  private RawLocalFileSystem localFs;
  protected MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private final Deflater deflater;

  boolean pipelinedShuffle;
  boolean finalMergeEnabled;
  private boolean sendEmptyPartitionDetails;

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
        ExternalSorter.getInitialMemoryRequirement(conf,
            getContext().getTotalMemoryAvailableToTask()), memoryUpdateCallbackHandler);

    sendEmptyPartitionDetails = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED,
        TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED_DEFAULT);

    auxiliaryService = ShuffleUtils.getTezShuffleHandlerServiceId(conf);
    compositeFetch = ShuffleUtils.isTezShuffleHandler(conf);

    return Collections.emptyList();
  }

  @Override
  public synchronized void start() throws Exception {
    if (!isStarted.get()) {
      memoryUpdateCallbackHandler.validateUpdateReceived();
      String sorterClass = conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS,
          TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS_DEFAULT).toUpperCase(Locale.ENGLISH);
      SorterImpl sorterImpl = null;
      try {
        sorterImpl = SorterImpl.valueOf(sorterClass);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid sorter class specified in config"
            + ", propertyName=" + TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS
            + ", value=" + sorterClass
            + ", validValues=" + Arrays.asList(SorterImpl.values()));
      }

      pipelinedShuffle = conf.getBoolean(
          TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED,
          TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED_DEFAULT);
      // set finalMergeEnabled = !pipelinedShuffleConf unless set explicitly in tez-site.xml
      finalMergeEnabled = conf.getBoolean(
          TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT,
          !pipelinedShuffle);

      if (pipelinedShuffle) {
        if (finalMergeEnabled) {
          LOG.info(getContext().getDestinationVertexName() + " disabling final merge as "
              + TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED + " is enabled.");
          finalMergeEnabled = false;
          conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, false);
        }

        Preconditions.checkArgument(sorterImpl.equals(SorterImpl.PIPELINED),
            TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED + "only works with PipelinedSorter.");
      }

      if (sorterImpl.equals(SorterImpl.PIPELINED)) {
        sorter = new PipelinedSorter(getContext(), conf, getNumPhysicalOutputs(),
            memoryUpdateCallbackHandler.getMemoryAssigned());
        usePipelinedSorter = true;
      } else {
        throw new UnsupportedOperationException("Unsupported sorter class specified in config"
            + ", propertyName=" + TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS
            + ", value=" + sorterClass
            + ", validValues=" + Arrays.asList(SorterImpl.values()));
      }

      isStarted.set(true);
    }
  }

  @Override
  public synchronized KeyValuesWriter getWriter() throws IOException {
    Preconditions.checkState(isStarted.get(), "Cannot get writer before starting the Output");
    return new KeyValuesWriter() {
      @Override
      public void write(Object key, Object value) throws IOException {
        sorter.write(key, value);
      }

      @Override
      public void write(Object key, Iterable<Object> values) throws IOException {
        sorter.write(key, values);
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
    if (finalMergeEnabled && !pipelinedShuffle) {
      if (!sorter.getFinalIndexComputed()) {
        // return an empty list because TezSpillRecord() throws NPE
        // this occurs when PipelinedSorter threads gets interrupted and LogicalOutput.close() is closed
        return eventList;
      }

      // In PipelinedSorter.flush() skips renaming output directories if finalMergeEnabled == true && numSpills == 1.
      // Here we adjust pathComponent in accordance so that downstream tasks can request, e.g., ".../...10031_0/file.out".
      String pathComponent = (usePipelinedSorter && sorter.getNumSpills() == 1) ?
          getContext().getUniqueIdentifier() + "_0" :   // use original output directory ".../...10031_0"
          getContext().getUniqueIdentifier();           // use renamed output directory ".../...10031"
      String pathComponentExpanded = ShuffleUtils.expandPathComponent(getContext(), compositeFetch, pathComponent);
      TezSpillRecord tezSpillRecord = ShuffleUtils.getTezSpillRecord(
          getContext(), pathComponentExpanded, sorter.getFinalIndexFile(), localFs);

      boolean isLastEvent = true;
      ShuffleUtils.generateEventOnSpill(eventList, finalMergeEnabled, isLastEvent,
          getContext(), 0, tezSpillRecord,
          getNumPhysicalOutputs(), sendEmptyPartitionDetails, pathComponent,
          sorter.getPartitionStats(), sorter.reportDetailedPartitionStats(), auxiliaryService, deflater,
          compositeFetch);
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
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINE_MIN_SPILLS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_SORT_THREADS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_USE_SOFT_REFERENCE);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINER_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_INTERNAL_SORTER_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT);
  }

  public static Set<String> getConfigurationKeySet() {
    return Collections.unmodifiableSet(confKeys);
  }
}
