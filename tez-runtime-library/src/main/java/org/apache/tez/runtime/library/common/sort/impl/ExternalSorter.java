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

package org.apache.tez.runtime.library.common.sort.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputStatisticsReporter;
import org.apache.tez.runtime.library.api.IOInterruptedException;
import org.apache.tez.runtime.library.common.shuffle.ShuffleServer;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration.ReportPartitionStats;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.combine.Combiner;
import org.apache.tez.runtime.library.common.serializer.SerializationContext;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleHeader;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.tez.runtime.api.TezTaskOutput;
import org.apache.tez.runtime.library.utils.CodecUtils;
import org.apache.tez.common.Preconditions;

@SuppressWarnings({"rawtypes"})
public abstract class ExternalSorter {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalSorter.class);

  public List<Event> close() throws IOException {
    if (writeSpillRecord) {
      spillFileIndexPaths.clear();
    }
    spillFilePaths.clear();
    reportStatistics();
    return Collections.emptyList();
  }

  public abstract void flush() throws IOException;

  public abstract void write(Object key, Object value) throws IOException;

  public void write(Object key, Iterable<Object> values) throws IOException {
    //TODO: Sorter classes should override this method later.
    Iterator<Object> it = values.iterator();
    while(it.hasNext()) {
      write(key, it.next());
    }
  }

  protected final Progressable progressable = new Progressable() {
    @Override
    public void progress() {
    }
  };

  protected final OutputContext outputContext;
  protected final Combiner combiner;
  protected final Partitioner partitioner;
  protected final Configuration conf;
  protected final RawLocalFileSystem localFs;
  protected final FileSystem rfs;
  protected final int partitions;
  protected final RawComparator comparator;

  protected final SerializationContext serializationContext;
  protected final Serializer keySerializer;
  protected final Serializer valSerializer;
  
  protected final boolean ifileReadAhead;
  protected final int ifileReadAheadLength;

  protected final long availableMemoryMb;

  protected final IndexedSorter sorter;

  // Compression for map-outputs
  protected final CompressionCodec codec;

  protected final Map<Integer, Path> spillFilePaths = Maps.newHashMap();
  // update only if writeSpillRecord == true
  protected final Map<Integer, Path> spillFileIndexPaths = Maps.newHashMap();

  // protected final boolean physicalHostFetch;  // TODO: currently unused
  protected final String auxiliaryService;
  protected final boolean compositeFetch;
  protected final TezTaskOutput mapOutputFile;

  protected Path finalOutputFile;

  // null if writeSpillRecord == false
  protected Path finalIndexFile;
  protected boolean finalIndexComputed;   // true if final TezSpillRecord is effectively computed

  protected int numSpills;

  protected final boolean cleanup;

  protected OutputStatisticsReporter statsReporter;
  // uncompressed size for each partition
  protected final long[] partitionStats;
  protected final boolean sendEmptyPartitionDetails;

  // Counters
  // MR compatilbity layer needs to rename counters back to what MR requries.

  // Represents final deserialized size of output (spills are not counted)
  protected final TezCounter mapOutputByteCounter;
  // Represents final number of records written (spills are not counted)
  protected final TezCounter mapOutputRecordCounter;
  // Represents the size of the final output - with any overheads introduced by
  // the storage/serialization mechanism. This is an uncompressed data size.
  protected final TezCounter outputBytesWithOverheadCounter;
  // Represents the size of the final output - which will be transmitted over
  // the wire (spills are not counted). Factors in compression if it is enabled.
  protected final TezCounter fileOutputByteCounter;
  // Represents total number of records written to disk (includes spills. Min
  // value for this is equal to number of output records)
  protected final TezCounter spilledRecordsCounter;
  // Bytes written as a result of additional spills. The single spill for the
  // final output data is not considered. (This will be 0 if there's no
  // additional spills. Compressed size - so may not represent the size in the
  // sort buffer)
  protected final TezCounter additionalSpillBytesWritten;
  
  protected final TezCounter additionalSpillBytesRead;
  // Number of spills written & consumed by the same task to generate the final file
  protected final TezCounter numAdditionalSpills;
  // Number of files offered via shuffle-handler to consumers.
  protected final TezCounter numShuffleChunks;
  // How partition stats should be reported.
  final ReportPartitionStats reportPartitionStats;

  protected final boolean writeSpillRecord;

  public ExternalSorter(OutputContext outputContext, Configuration conf, int numOutputs,
      long initialMemoryAvailable) throws IOException {
    this.outputContext = outputContext;
    this.conf = conf;
    this.localFs = (RawLocalFileSystem) FileSystem.getLocal(conf).getRaw();
    this.partitions = numOutputs;
    reportPartitionStats = ReportPartitionStats.fromString(
        conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS,
        TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS_DEFAULT));
    partitionStats = reportPartitionStats.isEnabled() ?
        (new long[partitions]) : null;

    cleanup = conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT,
        TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT_DEFAULT);

    rfs = ((LocalFileSystem)FileSystem.getLocal(this.conf)).getRaw();

    if (LOG.isDebugEnabled()) {
      LOG.debug(outputContext.getDestinationVertexName() + ": Initial Mem bytes : " +
          initialMemoryAvailable + ", in MB=" + ((initialMemoryAvailable >> 20)));
    }
    int assignedMb = (int) (initialMemoryAvailable >> 20);
    // Let the overflow checks happen in appropriate sorter impls
    this.availableMemoryMb = assignedMb;

    // sorter
    sorter = ReflectionUtils.newInstance(this.conf.getClass(
        TezRuntimeConfiguration.TEZ_RUNTIME_INTERNAL_SORTER_CLASS, QuickSort.class,
        IndexedSorter.class), this.conf);

    comparator = ConfigUtils.getIntermediateOutputKeyComparator(this.conf);

    // k/v serialization
    this.serializationContext = new SerializationContext(this.conf);
    keySerializer = serializationContext.getKeySerializer();
    valSerializer = serializationContext.getValueSerializer();
    LOG.info("{}, memoryMb={}", outputContext.getDestinationVertexName(), assignedMb);
    if (LOG.isDebugEnabled()) {
      LOG.debug("keySerializerClass=" + serializationContext.getKeyClass()
          + ", valueSerializerClass=" + valSerializer
          + ", comparator=" + (RawComparator) ConfigUtils.getIntermediateOutputKeyComparator(conf)
          + ", partitioner=" + conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS)
          + ", serialization=" + conf.get(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY)
          + ", reportPartitionStats=" + reportPartitionStats);
    }

    //    counters    
    mapOutputByteCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES);
    mapOutputRecordCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_RECORDS);
    outputBytesWithOverheadCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_WITH_OVERHEAD);
    fileOutputByteCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_PHYSICAL);
    spilledRecordsCounter = outputContext.getCounters().findCounter(TaskCounter.SPILLED_RECORDS);
    additionalSpillBytesWritten = outputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_WRITTEN);
    additionalSpillBytesRead = outputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_READ);
    numAdditionalSpills = outputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILL_COUNT);
    numShuffleChunks = outputContext.getCounters().findCounter(TaskCounter.SHUFFLE_CHUNK_COUNT);

    Configuration codecConf = ShuffleServer.getCodecConf(outputContext.peekShuffleServer(), conf);
    this.codec = CodecUtils.getCodec(codecConf);

    this.ifileReadAhead = this.conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD,
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT);
    if (this.ifileReadAhead) {
      this.ifileReadAheadLength = conf.getInt(
          TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
          TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT);
    } else {
      this.ifileReadAheadLength = 0;
    }

    // this.physicalHostFetch = conf.getBoolean(
    //     TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH,
    //     TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_DEFAULT);

    this.auxiliaryService = ShuffleUtils.getTezShuffleHandlerServiceId(conf);
    this.compositeFetch = ShuffleUtils.isTezShuffleHandler(conf);

    this.mapOutputFile = TezRuntimeUtils.instantiateTaskOutputManager(
        this.conf, outputContext, this.compositeFetch);

    this.conf.setInt(TezRuntimeFrameworkConfigs.TEZ_RUNTIME_NUM_EXPECTED_PARTITIONS, this.partitions);
    this.partitioner = TezRuntimeUtils.instantiatePartitioner(this.conf);
    this.combiner = TezRuntimeUtils.instantiateCombiner(this.conf, outputContext);

    this.statsReporter = outputContext.getStatisticsReporter();
    this.sendEmptyPartitionDetails = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED,
        TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED_DEFAULT);

    this.writeSpillRecord = !compositeFetch;

    finalIndexComputed = false;
  }

  public TezTaskOutput getMapOutput() {
    return mapOutputFile;
  }

  public boolean getFinalIndexComputed() {
    return finalIndexComputed;
  }

  // returns null if writeSpillRecord == false
  public Path getFinalIndexFile() {
    return finalIndexFile;
  }

  public Path getFinalOutputFile() {
    return finalOutputFile;
  }

  protected void runCombineProcessor(
      TezRawKeyValueIterator kvIter, Writer writer) throws IOException {
    try {
      combiner.combine(kvIter, writer);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOInterruptedException("Combiner interrupted", e);
    }
  }

  /**
   * Rename srcPath to dstPath on the same volume. This is the same as
   * RawLocalFileSystem's rename method, except that it will not fall back to a
   * copy, and it will create the target directory if it doesn't exist.
   */
  protected void sameVolRename(Path srcPath, Path dstPath) throws IOException {
    RawLocalFileSystem rfs = (RawLocalFileSystem) this.rfs;
    File src = rfs.pathToFile(srcPath);
    File dst = rfs.pathToFile(dstPath);
    if (!dst.getParentFile().exists()) {
      if (!dst.getParentFile().mkdirs()) {
        throw new IOException("Unable to rename " + src + " to " + dst
            + ": couldn't create parent directory");
      }
    }

    if (!src.renameTo(dst)) {
      throw new IOException("Unable to rename " + src + " to " + dst);
    }
  }

  public InputStream getSortedStream(int partition) {
    throw new UnsupportedOperationException("getSortedStream isn't supported!");
  }

  public ShuffleHeader getShuffleHeader(int reduce) {
    throw new UnsupportedOperationException("getShuffleHeader isn't supported!");
  }

  public static long getInitialMemoryRequirement(Configuration conf, long maxAvailableTaskMemory) {
    int initialMemRequestMb = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB,
        TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB_DEFAULT);
    long reqBytes = ((long) initialMemRequestMb) << 20;
    //Higher bound checks are done in individual sorter implementations
    Preconditions.checkArgument(initialMemRequestMb > 0 && reqBytes < maxAvailableTaskMemory,
        "{} {} should be larger than 0 and should be less than the available task memory (MB): {}",
    TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, initialMemRequestMb, maxAvailableTaskMemory >> 20);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Requested SortBufferSize ("
          + TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB + "): " + initialMemRequestMb);
    }
    return reqBytes;
  }

  public int getNumSpills() {
    return numSpills;
  }

  protected synchronized void cleanup() throws IOException {
    if (!cleanup) {
      return;
    }
    cleanup(spillFilePaths);
    cleanup(finalOutputFile);

    if (writeSpillRecord) {
      cleanup(spillFileIndexPaths);
      cleanup(finalIndexFile);
    }
  }

  protected synchronized void cleanup(Path path) {
    if (path == null || !cleanup) {
      return;
    }
    try {
      LOG.info("Deleting " + path);
      rfs.delete(path, true);
    } catch(IOException ioe) {
      LOG.warn("Error in deleting "  + path);
    }
  }

  protected synchronized void cleanup(Map<Integer, Path> spillMap) {
    if (!cleanup) {
      return;
    }
    for(Map.Entry<Integer, Path> entry : spillMap.entrySet()) {
      cleanup(entry.getValue());
    }
  }

  public long[] getPartitionStats() {
    return partitionStats;
  }

  protected boolean reportPartitionStats() {
    return (partitionStats != null);
  }

  protected synchronized void reportStatistics() {
    // This works for non-started outputs since new counters will be created with an initial value of 0
    long outputSize = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES).getValue();
    statsReporter.reportDataSize(outputSize);
    long outputRecords = outputContext.getCounters()
        .findCounter(TaskCounter.OUTPUT_RECORDS).getValue();
    statsReporter.reportItemsProcessed(outputRecords);
  }

  public boolean reportDetailedPartitionStats() {
    return reportPartitionStats.isPrecise();
  }
}
