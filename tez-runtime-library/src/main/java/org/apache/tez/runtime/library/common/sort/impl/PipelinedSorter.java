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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.zip.Deflater;

import com.google.common.collect.Maps;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.tez.common.Preconditions;
import com.google.common.collect.Lists;

import org.apache.hadoop.fs.FileSystem;
import org.apache.tez.runtime.api.MultiByteArrayOutputStream;
import org.apache.tez.runtime.library.api.IOInterruptedException;
import org.apache.tez.runtime.library.utils.CodecUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.tez.common.io.NonSyncDataOutputStream;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TezTaskOutput;
import org.apache.tez.runtime.library.common.comparator.TezBytesComparator;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration.ReportPartitionStats;
import org.apache.tez.runtime.library.common.serializer.SerializationContext;
import org.apache.tez.runtime.library.common.shuffle.ShuffleServer;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.sort.impl.IFile.WriterBytesWritable;
import org.apache.tez.runtime.library.common.sort.impl.IFile.WriterDataInputBuffer;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger.DiskSegment;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger.Segment;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.util.FastByteComparisons;

import static org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord.ensureSpillFilePermissions;

@SuppressWarnings({"unchecked", "rawtypes"})
public final class PipelinedSorter {
  
  private static final Logger LOG = LoggerFactory.getLogger(PipelinedSorter.class);
  private static final boolean isDebugEnabled = LOG.isDebugEnabled();

  private final OutputContext outputContext;
  private final Configuration conf;
  private final int partitions;

  private final RawLocalFileSystem localFs;
  private final boolean localFsSpillFilePerms;

  final ReportPartitionStats reportPartitionStats;
  private final long[] partitionStats;
  private final boolean sendEmptyPartitionDetails;

  private int numSpills;
  private final boolean cleanup;
  private final long availableMemoryMb;
  private final Partitioner partitioner;
  private final CompressionCodec codec;
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;
  private final String auxiliaryService;
  private final boolean compositeFetch;
  private final TezTaskOutput mapOutputFile;
  private final boolean writeSpillRecord;
  private final Map<Integer, Path> spillFilePaths;
  private final Map<Integer, Path> spillFileIndexPaths;
  private final TezCounter outputRecordsCounter;
  private final TezCounter outputRecordBytesCounter;
  private final TezCounter outputBytesWithOverheadCounter;
  private final TezCounter fileOutputBytesCounter;
  private final TezCounter fileOutputBytesMemoryCounter;
  private final TezCounter spilledRecordsCounter;
  private final TezCounter additionalSpillBytesWrittenCounter;
  private final TezCounter additionalSpillBytesReadCounter;
  private final TezCounter numAdditionalSpillsCounter;

  private Path finalOutputFile;
  private Path finalIndexFile;
  private boolean finalIndexComputed;

  /**
   * The size of each record in the index file for the map-outputs.
   */
  public static final int MAP_OUTPUT_INDEX_RECORD_LENGTH = 24;
  private final static int APPROX_HEADER_LENGTH = 150;

  private final int partitionBits;

  private static final int KEYSTART = 0;         // key offset in acct
  private static final int VALSTART = 1;         // val offset in acct
  private static final int PARTITION = 2;        // partition offset in acct
  private static final int VALLEN = 3;           // val len in acct
  private static final int NMETA = 4;            // num meta ints
  private static final int METASIZE = NMETA * 4; // size in bytes

  // Assume: ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN
  // This is checked in the static block of FastByteComparisons, so no need to check here again.

  private final boolean lazyAllocateMem;
  private final int MIN_BLOCK_SIZE;
  private final boolean useSoftReference;

  private final boolean isPipelinedShuffle;
  private final boolean isFinalMergeEnabled;

  private long currentAllocatableMemory;
  final int maxNumberOfBlocks;
  //total memory capacity allocated to sorter
  private final long capacity;

  // Maintain a list of ByteBuffers
  final List<ByteBuffer> buffers;
  final List<Integer> bufferUsage;
  private int bufferIndex = -1;

  private SortSpan span;
  private final SpanMerger merger;
  private final ExecutorService sortmaster;

  private final Deflater deflater;

  /**
   * Store the events to be send in close.
   */
  private final List<Event> finalEvents;

  private final byte[] writeBuffer;

  private final long freeMemoryThreshold;
  private final boolean useFreeMemoryWriterOutput;  // use availableMemory as threshold

  private static final class SpillInfo {
    final TezSpillRecord spillRecord;
    final Path spillFilePath;
    final Path spillIndexPath;
    final MultiByteArrayOutputStream spillOutput;

    SpillInfo(TezSpillRecord spillRecord, Path spillFilePath, Path spillIndexPath,
        MultiByteArrayOutputStream spillOutput) {
      this.spillRecord = spillRecord;
      this.spillFilePath = spillFilePath;
      this.spillIndexPath = spillIndexPath;
      this.spillOutput = spillOutput;
    }
  }

  private final ArrayList<SpillInfo> spillInfoList = new ArrayList<SpillInfo>();

  // track buffer overflow recursively in all buffers
  private int bufferOverflowRecursion = 0;

  public PipelinedSorter(OutputContext outputContext, Configuration conf, int numOutputs,
      long initialMemoryAvailable) throws IOException {
    this.outputContext = outputContext;
    this.conf = conf;
    this.partitions = numOutputs;

    this.localFs = (RawLocalFileSystem) FileSystem.getLocal(this.conf).getRaw();
    this.localFsSpillFilePerms = TezSpillRecord.SPILL_FILE_PERMS.equals(
        TezSpillRecord.SPILL_FILE_PERMS.applyUMask(FsPermission.getUMask(this.localFs.getConf())));

    this.reportPartitionStats = ReportPartitionStats.fromString(
        conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS,
            TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS_DEFAULT));
    this.partitionStats = reportPartitionStats.isEnabled() ? (new long[partitions]) : null;
    this.sendEmptyPartitionDetails = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED,
        TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED_DEFAULT);

    this.numSpills = 0;
    this.cleanup = conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT,
        TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT_DEFAULT);

    if (LOG.isDebugEnabled()) {
      LOG.debug(outputContext.getDestinationVertexName() + ": Initial Mem bytes : " +
          initialMemoryAvailable + ", in MB=" + ((initialMemoryAvailable >> 20)));
    }
    int assignedMb = (int) (initialMemoryAvailable >> 20);
    this.availableMemoryMb = assignedMb;

    this.conf.setInt(TezRuntimeFrameworkConfigs.TEZ_RUNTIME_NUM_EXPECTED_PARTITIONS, this.partitions);
    this.partitioner = TezRuntimeUtils.instantiatePartitioner(this.conf);

    LOG.info("{}, memoryMb={}", outputContext.getDestinationVertexName(), assignedMb);
    if (LOG.isDebugEnabled()) {
      LOG.debug("keyClass=" + SerializationContext.getKeyClass()
          + ", valueClass=" + SerializationContext.getValueClass()
          + ", partitioner=" + conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS)
          + ", reportPartitionStats=" + reportPartitionStats);
    }

    Object shuffleServer = outputContext.peekShuffleServer();
    Configuration codecConf = ShuffleServer.getCodecConf(shuffleServer, conf);
    Class<? extends CompressionCodec> codecClass =
        ShuffleServer.getCodecClass(shuffleServer, codecConf);
    this.codec = CodecUtils.getCodec(
        codecConf,
        codecClass,
        ShuffleServer.getCodecBufferSize(shuffleServer, codecConf, codecClass));

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

    this.auxiliaryService = ShuffleUtils.getTezShuffleHandlerServiceId(conf);
    this.compositeFetch = ShuffleUtils.isTezShuffleHandler(conf);
    this.mapOutputFile = TezRuntimeUtils.instantiateTaskOutputManager(
        this.conf, outputContext, this.compositeFetch);

    this.writeSpillRecord = !compositeFetch;
    this.spillFilePaths = Maps.newHashMap();
    this.spillFileIndexPaths = this.writeSpillRecord ? Maps.newHashMap() : null;

    this.outputRecordsCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_RECORDS);
    this.outputRecordBytesCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES);
    this.outputBytesWithOverheadCounter =
        outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_WITH_OVERHEAD);

    this.fileOutputBytesCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_DISK);
    this.fileOutputBytesMemoryCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_MEMORY);

    this.spilledRecordsCounter = outputContext.getCounters().findCounter(TaskCounter.SPILLED_RECORDS);
    this.additionalSpillBytesWrittenCounter =
        outputContext.getCounters().findCounter(TaskCounter.SPILL_BYTES_DISK);
    this.additionalSpillBytesReadCounter =
        outputContext.getCounters().findCounter(TaskCounter.SPILL_BYTES_READ_ADDITIONAL);
    this.numAdditionalSpillsCounter =
        outputContext.getCounters().findCounter(TaskCounter.SPILL_COUNT_ADDITIONAL);

    this.finalIndexComputed = false;

    this.partitionBits = bitcount(partitions) + 1;

    this.lazyAllocateMem = this.conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY,
        TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY_DEFAULT);

    if (this.lazyAllocateMem) {
      /**
       * When lazy-allocation is enabled, framework takes care of auto
       * allocating memory on need basis. Desirable block size is set to 256MB
       */
      // 256MB - 64 bytes. See comment for the 32MB allocation.
      this.MIN_BLOCK_SIZE = ((256 << 20) - 64);
    } else {
      int minBlockSize = conf.getInt(
          TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB,
          TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB_DEFAULT);
      Preconditions.checkArgument(
          (minBlockSize > 0 && minBlockSize < 2047),
          "{}={} should be a positive value between 0 and 2047",
          TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB, minBlockSize);
      this.MIN_BLOCK_SIZE = minBlockSize << 20;
    }
    this.useSoftReference = this.conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_USE_SOFT_REFERENCE,
        TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_USE_SOFT_REFERENCE_DEFAULT);

    this.isPipelinedShuffle = this.conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ORDERED_ENABLED,
        TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ORDERED_ENABLED_DEFAULT);
    // We do not use TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT.
    this.isFinalMergeEnabled = !this.isPipelinedShuffle;

    LOG.info("Setting up PipelinedSorter for {}", outputContext.getDestinationVertexName());

    // buffers and accounting
    long maxMemLimit = this.availableMemoryMb << 20;

    long totalCapacityWithoutMeta = 0;
    long availableMem = maxMemLimit;
    int numBlocks = 0;
    while (availableMem > 0) {
      long size = Math.min(availableMem, computeBlockSize(availableMem, maxMemLimit));
      int sizeWithoutMeta = (int) ((size) - (size % METASIZE));
      totalCapacityWithoutMeta += sizeWithoutMeta;
      availableMem -= size;
      numBlocks++;
    }
    this.currentAllocatableMemory = maxMemLimit;
    this.maxNumberOfBlocks = numBlocks;
    this.capacity = totalCapacityWithoutMeta;

    this.buffers = Lists.newArrayListWithCapacity(maxNumberOfBlocks);
    this.bufferUsage = Lists.newArrayListWithCapacity(maxNumberOfBlocks);
    allocateSpace();  // Allocate the first block
    if (!this.lazyAllocateMem) {
      // LOG.info("Pre allocating rest of memory buffers upfront");
      while (allocateSpace() != null);
    }

    Preconditions.checkState(!buffers.isEmpty(), "At least one buffer needs to be present");
    if (isDebugEnabled) {
      StringBuilder sb = new StringBuilder("PipelinedSorter for ")
        .append(outputContext.getDestinationVertexName())
        .append(": #blocks=").append(maxNumberOfBlocks)
        .append(", maxMemUsage=").append(maxMemLimit)
        .append(", lazyAllocateMem=").append(lazyAllocateMem)
        .append(", useSoftReference=").append(useSoftReference)
        .append(", minBlockSize=").append(MIN_BLOCK_SIZE)
        .append(", initial BLOCK_SIZE=").append(buffers.get(0).capacity())
        .append(", isFinalMergeEnabled=").append(isFinalMergeEnabled)
        .append(", pipelinedShuffle=").append(isPipelinedShuffle)
        .append(", sendEmptyPartitions=").append(sendEmptyPartitionDetails);
      LOG.debug(sb.toString());
    }

    this.span = new SortSpan(buffers.get(bufferIndex), 1024 * 1024, 16);
    this.merger = new SpanMerger(); // SpanIterators are comparable
    this.sortmaster = outputContext.getSorterThreadPool();

    this.deflater = TezCommonUtils.newBestCompressionDeflater();

    this.finalEvents = Lists.newLinkedList();

    this.writeBuffer = IFile.allocateWriteBuffer();

    this.freeMemoryThreshold = 1024L * 1024L * conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_FREE_MEMORY_WRITER_OUTPUT_THRESHOLD_MB,
        TezRuntimeConfiguration.TEZ_RUNTIME_FREE_MEMORY_WRITER_OUTPUT_THRESHOLD_MB_DEFAULT);
    // useFreeMemoryWriterOutput = false if compositeFetch == false, i.e, when using mapreduce_shuffle
    this.useFreeMemoryWriterOutput = compositeFetch && conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT_DEFAULT);
  }

  ByteBuffer allocateSpace() {
    if (currentAllocatableMemory <= 0) {
      // No space available.
      return null;
    }

    int size = computeBlockSize(currentAllocatableMemory, availableMemoryMb << 20);
    currentAllocatableMemory -= size;
    int sizeWithoutMeta = (size) - (size % METASIZE);

    ByteBuffer space;
    if (useSoftReference) {
      ByteBuffer bufferFromCache = outputContext.getSoftByteBuffer(sizeWithoutMeta);
      if (bufferFromCache != null) {
        bufferFromCache.clear();
        space = bufferFromCache;
        LOG.info("Reusing ByteBuffer from soft cache: {} {}",sizeWithoutMeta, space.capacity());
      } else {
        LOG.info("Creating a new ByteBuffer: " + sizeWithoutMeta);
        space = ByteBuffer.allocate(sizeWithoutMeta);
      }
    } else {
      space = ByteBuffer.allocate(sizeWithoutMeta);
    }

    buffers.add(space);
    bufferIndex++;
    bufferUsage.add(0);

    Preconditions.checkState(buffers.size() <= maxNumberOfBlocks,
        "{} exceeds {}", buffers.size(), maxNumberOfBlocks);

    if (isDebugEnabled) {
      StringBuilder allocLog = new StringBuilder("Newly allocated block size=" + size);
      allocLog.append(", index=").append(bufferIndex);
      allocLog.append(", Number of buffers=").append(buffers.size());
      allocLog.append(", currentAllocatableMemory=").append(currentAllocatableMemory);
      allocLog.append(", currentBufferSize=").append(space.capacity());
      allocLog.append(", total=").append(availableMemoryMb << 20);
      LOG.debug(allocLog.toString());
    }
    return space;
  }

  int computeBlockSize(long availableMem, long maxAllocatedMemory) {
    int maxBlockSize = 0;
    /**
     * When lazy-allocation is enabled, framework takes care of auto allocating
     * memory on need basis. In such cases, first buffer starts with 32 MB.
     */
    if (lazyAllocateMem) {
      if (buffers == null || buffers.isEmpty()) {
        // 32 MB - 64 bytes
        // These buffers end up occupying 33554456 (32M + 24) bytes.
        // On large JVMs (64G+), with G1GC - the region size maxes out at 32M.
        // Without the -64, this structure would end up using 2 regions.
        return ((32 << 20) - 64);
      }
    }

    // Honor MIN_BLOCK_SIZE
    maxBlockSize = Math.max(MIN_BLOCK_SIZE, maxBlockSize);

    if (availableMem < maxBlockSize) {
      maxBlockSize = (int) availableMem;
    }

    int maxMem = (maxAllocatedMemory > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) maxAllocatedMemory;
    if (maxBlockSize > maxMem) {
      maxBlockSize = maxMem;
    }

    availableMem -= maxBlockSize;
    if (availableMem < MIN_BLOCK_SIZE) {
      if ((maxBlockSize + availableMem) < Integer.MAX_VALUE) {
        //Merge remaining with last block
        maxBlockSize += availableMem;
      }
    }
    return maxBlockSize;
  }

  private int bitcount(int n) {
    int bit = 0;
    while(n!=0) {
      bit++;
      n >>= 1;
    }
    return bit;
  }

  private void sort() throws IOException {
    SortSpan newSpan = span.next();

    if (newSpan == null) {
      //avoid sort/spill of empty span
      // sort in the same thread, do not wait for the thread pool
      merger.add(span.sort());
      boolean ret = spill(true);
      if (isPipelinedShuffle && ret) {
        sendPipelinedShuffleEvents();
      }
      // Use the next buffer
      bufferIndex = (bufferIndex + 1) % buffers.size();
      bufferUsage.set(bufferIndex, bufferUsage.get(bufferIndex) + 1);
      int items = 1024*1024;
      int perItem = 16;
      if (span.length() != 0) {
        items = span.length();
        perItem = span.kvbuffer.limit()/items;
        items = (int) ((span.capacity)/(METASIZE+perItem));
        if (items > 1024*1024) {
            // our goal is to have 1M splits and sort early
            items = 1024*1024;
        }
      }
      Preconditions.checkArgument(buffers.get(bufferIndex) != null, "block should not be empty");
      span = new SortSpan((ByteBuffer)buffers.get(bufferIndex).clear(), (1024*1024), perItem);
    } else {
      // queue up the sort
      SortTask task = new SortTask(span);
      // LOG.debug("Submitting span={} for sort", span.toString());
      Future<SpanIterator> future = sortmaster.submit(task);
      merger.add(future);
      span = newSpan;
    }
  }

  // if pipelined shuffle is enabled, this method is called to send events for every spill
  private void sendPipelinedShuffleEvents() throws IOException{
    List<Event> events = Lists.newLinkedList();
    String pathComponent = ShuffleUtils.getUniqueIdentifierSpillId(outputContext, numSpills - 1);
    ShuffleUtils.generateEventOnSpill(events, isFinalMergeEnabled, false,
        outputContext, (numSpills - 1), spillInfoList.get(numSpills - 1).spillRecord,
        partitions, sendEmptyPartitionDetails, pathComponent, partitionStats,
        reportDetailedPartitionStats(), auxiliaryService, deflater);
    outputContext.sendEvents(events);
    if (isDebugEnabled) {
      LOG.debug("{}: Added spill event for spill (final update=false), spillId={}",
          outputContext.getDestinationVertexName(), (numSpills - 1));
    }
  }

  synchronized public void closeWriter() {
    LOG.info("Closing up PipelinedSorter KeyValueWriterEdge for {}",
        outputContext.getDestinationVertexName());
  }

  // Invariants on setDefaultLengths()/write()/closeWriter()/flush()/close():
  //  - setDefaultLengths()/write()/closeWriter() are called from the same thread and never called concurrently.
  //  - closeWriter() is the last call.
  //  - OrderedPartitionedKVOutput.close() is called after closeWriter() is called,
  //    so flush()/close() are called only after closeWriter() is called.
  //
  // Hence, it is safe to skip guarding collect() with synchronized:
  //  - closeWriter(), called after all collect() calls, is guarded with synchronized.
  //  - flush()/close() are guided with synchronized.

  public void write(BytesWritable key, BytesWritable value) throws IOException {
    collect(key, value, partitioner.getPartition(key, value, partitions));
  }

  // TODO: optimize by directly calling collect(), if this method is actually called
  public void write(BytesWritable key, Iterable<BytesWritable> values) throws IOException {
    Iterator<BytesWritable> it = values.iterator();
    while (it.hasNext()) {
      write(key, it.next());
    }
  }

  /**
   * Serialize the key, value to intermediate storage.
   * When this method returns, kvindex must refer to sufficient unused
   * storage to store one METADATA.
   */
  private void collect(BytesWritable key, BytesWritable value, final int partition) throws IOException {
    if (partition < 0 || partition >= partitions) {
      throw new IOException("Illegal partition for " + key + " (" +
          partition + ")");
    }
    // TBD:FIX in TEZ-2574
    if (span.metaRemaining() < METASIZE) {
      this.sort();
      if (span.length() == 0) {
        spillSingleRecord(key, value, partition);
        return;
      }
    }
    int keystart = span.kvbuffer.position();
    int valstart = -1;
    int valend = -1;
    try {
      span.out.write(key.getBytesRaw(), key.getOffset(), key.getLength());
      valstart = span.kvbuffer.position();      
      span.out.write(value.getBytesRaw(), value.getOffset(), value.getLength());
      valend = span.kvbuffer.position();
    } catch (BufferOverflowException overflow) {
      // restore limit
      span.kvbuffer.position(keystart);
      this.sort();
      if (span.length() == 0 || bufferOverflowRecursion > buffers.size()) {
        // spill the current key value pair
        spillSingleRecord(key, value, partition);
        bufferOverflowRecursion = 0;
        return;
      }
      bufferOverflowRecursion++;
      // try again
      this.collect(key, value, partition);
      return;
    }

    if (bufferOverflowRecursion > 0) {
      bufferOverflowRecursion--;
    }

    int prefix = TezBytesComparator.getProxy(key);
    prefix = (partition << (32 - partitionBits)) | (prefix >>> partitionBits);

    /* maintain order as in KEYSTART, VALSTART, PARTITION, VALLEN */
    span.putMetaLong(keystart, valstart);
    span.putMetaLong(prefix, valend - valstart);
    outputRecordsCounter.increment(1);
    outputRecordBytesCounter.increment(valend - keystart);
  }

  private void adjustSpillCounters(long rawLength, long compLength) {
    if (!isFinalMergeEnabled) {
      outputBytesWithOverheadCounter.increment(rawLength);
    } else {
      if (numSpills > 0) {
        additionalSpillBytesWrittenCounter.increment(compLength);
        // Reset the value will be set during the final merge.
        outputBytesWithOverheadCounter.setValue(0);
      } else {
        // Set this up for the first write only. Subsequent ones will be handled in the final merge.
        outputBytesWithOverheadCounter.increment(rawLength);
      }
    }
  }

  // it is guaranteed that when spillSingleRecord is called, there is
  // no merger spans queued in executor.
  private void spillSingleRecord(final BytesWritable key, final BytesWritable value,
          int partition) throws IOException {
    final TezSpillRecord spillRec = new TezSpillRecord(partitions);
    // getSpillFileForWrite with size -1 as the serialized size of KV pair is still unknown
    final Path outputFilePath = mapOutputFile.getSpillFileForWrite(numSpills, -1);
    spillFilePaths.put(numSpills, outputFilePath);
    Path indexFilename = null;
    FSDataOutputStream out = localFs.create(outputFilePath, true, 4096);
    ensureSpillFilePermissions(outputFilePath, localFs, localFsSpillFilePerms);

    try {
      LOG.info("{}: Spilling single record to {}", outputContext.getDestinationVertexName(), outputFilePath.toString());

      // writer = WriterBytesWritable, so RLE encoding is not used
      for (int i = 0; i < partitions; ++i) {
        if (isThreadInterrupted()) {
          return;
        }
        WriterBytesWritable writer = null;
        try {
          long segmentStart = out.getPos();
          if (!sendEmptyPartitionDetails || (i == partition)) {
            writer = new WriterBytesWritable(out,
                codec, spilledRecordsCounter, null,
                false, false,
                -1, -1,
                writeBuffer, null, outputContext);
          }
          // we need not check for combiner since its a single record
          if (i == partition) {
            final long recordStart = out.getPos();
            writer.appendNoRle(key, value);
            outputRecordsCounter.increment(1);
            outputRecordBytesCounter.increment(out.getPos() - recordStart);
          }
          long rawLength = 0;
          long partLength = 0;
          if (writer != null) {
            writer.close();
            rawLength = writer.getRawLength();
            partLength = writer.getCompressedLength();
          }
          adjustSpillCounters(rawLength, partLength);
          // record offsets
          final TezIndexRecord rec = new TezIndexRecord(segmentStart, rawLength, partLength);
          spillRec.putIndex(rec, i);
          writer = null;
        } finally {
          if (null != writer) {
            writer.close();
          }
        }
      }

      if (writeSpillRecord) {
        indexFilename = mapOutputFile.getSpillIndexFileForWrite(
            numSpills, partitions * MAP_OUTPUT_INDEX_RECORD_LENGTH);
        spillFileIndexPaths.put(numSpills, indexFilename);
        spillRec.writeToFile(indexFilename, localFs, localFsSpillFilePerms);
      } else {
        ShuffleUtils.writeSpillInfoToIndexPathCacheAndByteCache(
            outputContext, numSpills, outputFilePath, spillRec, null, null);
      }

      //TODO: honor cache limits
      spillInfoList.add(new SpillInfo(spillRec, outputFilePath, indexFilename, null));
      ++numSpills;

      if (isPipelinedShuffle) {
        // This output file is directly served to downstream tasks, so increment fileOutputBytesCounter.
        fileOutputBytesCounter.increment(localFs.getFileStatus(outputFilePath).getLen());
        // No final merge. Set the number of files offered via shuffle-handler
        // numShuffleChunks.setValue(numSpills);
        sendPipelinedShuffleEvents();
      }
    } finally {
      out.close();
    }
  }

  private boolean spill(boolean ignoreEmptySpills) throws IOException {
    try {
      boolean ret = merger.ready();
      // if merger returned false and ignore merge is true,
      // then return directly without spilling
      if (!ret && ignoreEmptySpills){
        return false;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info(outputContext.getDestinationVertexName() + ": Interrupted while waiting for mergers to complete");
      throw new IOInterruptedException(outputContext.getDestinationVertexName() + ": Interrupted while waiting for mergers to complete", e);
    }

    // create spill file

    final long size = capacity + (partitions * APPROX_HEADER_LENGTH);
    final TezSpillRecord spillRec = new TezSpillRecord(partitions);
    final Path spillFileName = mapOutputFile.getSpillFileForWrite(numSpills, size);
    spillFilePaths.put(numSpills, spillFileName);
    Path indexFilename = null;

    MultiByteArrayOutputStream byteArrayOutput = null;
    boolean canUseBuffers = false;
    boolean spillToFreeMemory = useFreeMemoryWriterOutput;
    if (spillToFreeMemory) {
      canUseBuffers = MultiByteArrayOutputStream.canUseFreeMemoryBuffers(freeMemoryThreshold);
      if (canUseBuffers) {
        byteArrayOutput = new MultiByteArrayOutputStream(localFs, spillFileName);
      }
    }

    final boolean isRleEnabled = merger.needsRLE();

    FSDataOutputStream fsOutput = null;
    Compressor compressorExternal = null;
    long sumPartLength = 0L;

    try {
      if (byteArrayOutput == null) {
        fsOutput = localFs.create(spillFileName, true, 4096);
      } else {
        fsOutput = new FSDataOutputStream(byteArrayOutput, null);
      }
      ensureSpillFilePermissions(spillFileName, localFs, localFsSpillFilePerms);

      if (isDebugEnabled) {
        LOG.debug("Spilling to {} (use in-memory buffers = {})", spillFileName.toString(), canUseBuffers);
      }

      for (int i = 0; i < partitions; ++i) {
        if (isThreadInterrupted()) {
          return false;
        }
        TezRawKeyValueIterator kvIter = merger.filter(i);
        // write merged output to disk
        long segmentStart = fsOutput.getPos();
        WriterDataInputBuffer writer = null;
        boolean hasNext = kvIter.hasNext();
        if (hasNext || !sendEmptyPartitionDetails) {
          if (codec != null && compressorExternal == null) {
            compressorExternal = outputContext.getCompressor(codec);
          }
          writer = new WriterDataInputBuffer(
              fsOutput,
              codec, spilledRecordsCounter, null, false, isRleEnabled,
              -1, -1,
              writeBuffer, compressorExternal, outputContext);
        }
        if (isRleEnabled) {
          while (kvIter.next()) {
            writer.appendRle(kvIter.getKey(), kvIter.getValue());
          }
        } else {
          while (kvIter.next()) {
            writer.appendNoRle(kvIter.getKey(), kvIter.getValue());
          }
        }

        long rawLength = 0;
        long partLength = 0;
        if (writer != null) {
          writer.close();
          rawLength = writer.getRawLength();
          partLength = writer.getCompressedLength();
        }
        adjustSpillCounters(rawLength, partLength);
        sumPartLength += partLength;

        // record offsets
        final TezIndexRecord rec = new TezIndexRecord(segmentStart, rawLength, partLength);
        spillRec.putIndex(rec, i);
        if (!isFinalMergeEnabled && reportPartitionStats()) {
          partitionStats[i] += rawLength;
        }
      } // end of for loop
    } finally {
      if (compressorExternal != null) {
        outputContext.returnCompressor(codec.getCompressorType(), compressorExternal);
      }
      if (fsOutput != null) {
        fsOutput.close();
      }
    }

    if (writeSpillRecord) {
      indexFilename = mapOutputFile.getSpillIndexFileForWrite(
          numSpills, partitions * MAP_OUTPUT_INDEX_RECORD_LENGTH);
      spillFileIndexPaths.put(numSpills, indexFilename);
      spillRec.writeToFile(indexFilename, localFs, localFsSpillFilePerms);
    } else {
      Path outputFilePath = byteArrayOutput == null ? spillFileName : null;
      ShuffleUtils.writeSpillInfoToIndexPathCacheAndByteCache(
          outputContext, numSpills, outputFilePath, spillRec, byteArrayOutput, null);
    }
    if (isDebugEnabled) {
      LOG.debug("{}: Finished spill {}", outputContext.getDestinationVertexName(), numSpills);
    }

    // TODO: honor cache limits
    spillInfoList.add(new SpillInfo(spillRec, spillFileName, indexFilename, byteArrayOutput));
    ++numSpills;

    if (!isFinalMergeEnabled) {
      // This spill is directly served to downstream tasks, so increment fileOutputByteCounter.
      if (byteArrayOutput == null) {
        fileOutputBytesCounter.increment(localFs.getFileStatus(spillFileName).getLen());
      } else {
        fileOutputBytesMemoryCounter.increment(sumPartLength);
      }
      // No final merge. Set the number of files offered via shuffle-handler
      // numShuffleChunks.setValue(numSpills);
    }

    return true;
  }

  private boolean isThreadInterrupted() throws IOException {
    if (Thread.currentThread().isInterrupted()) {
      cancelActiveSortTasks();
      if (cleanup) {
        cleanup();
      }
      return true;
    }
    return false;
  }

  private Segment createSegmentFromSpill(SpillInfo spillInfo, int partitionNumber) throws IOException {
    TezIndexRecord indexRecord = spillInfo.spillRecord.getIndex(partitionNumber);
    MultiByteArrayOutputStream byteArrayOutput = spillInfo.spillOutput;
    if (byteArrayOutput == null) {
      Path spillFilename = spillInfo.spillFilePath;
      return new DiskSegment(localFs, spillFilename, indexRecord.getStartOffset(),
          indexRecord.getPartLength(), codec, ifileReadAhead, ifileReadAheadLength,
          true, null, outputContext);
    }

    InputStream input = byteArrayOutput.createInputStreamFrom(
        indexRecord.getStartOffset(), indexRecord.getPartLength());

    IFile.KeyValueReaderDataInputBuffer reader = new IFile.Reader(input, indexRecord.getPartLength(),
        codec, null, null, ifileReadAhead, ifileReadAheadLength, outputContext, null);
    // This spill output (byteArrayOutput) can be consumed for multiple partitions during the final merge.
    // Keep it alive across partition segments and clean once all partitions are merged in cleanSpillOutputBuffers().
    return new TezMerger.IntermediateMemorySegment(reader, byteArrayOutput, false);
  }

  private void cleanSpillOutputBuffers() {
    for (SpillInfo spillInfo : spillInfoList) {
      if (spillInfo.spillOutput != null) {
        spillInfo.spillOutput.clean();
      }
    }
  }

  synchronized public void flush() throws IOException {
    final String uniqueIdentifier = outputContext.getUniqueIdentifier();

    /**
     * Possible that the thread got interrupted when flush was happening or when the flush was
     * never invoked. As a part of cleanup activity in TezTaskRunner, it would invoke close()
     * on all I/O. At that time, this is safe to cleanup
     */
    if (isThreadInterrupted()) {
      return;
    }

    try {
      if (isDebugEnabled) { LOG.debug(outputContext.getDestinationVertexName() + ": Starting flush of map output"); }
      span.end();
      merger.add(span.sort());
      // force a spill in flush()
      // case 1: we want to force because of following scenarios:
      // we have no keys written, and flush got called
      // we want at least one spill (be it empty)
      // case 2: in pipeline shuffle case, we have no way of
      // knowing the last key being written until flush is called
      // so for flush()->spill() we want to force spill so that
      // we can send pipeline shuffle event with last event true.
      spill(false);

      if (useSoftReference) {
        for (ByteBuffer buffer: buffers) {
          LOG.info("Adding soft ByteBuffer: " + buffer.capacity());
          outputContext.addSoftByteBuffer(buffer);
        }
      }

      //safe to clean up
      buffers.clear();

      if (spillInfoList.isEmpty()) {
        /*
         * If we do not have this check, and if the task gets killed in the middle, it can throw
         * NPE leading to distraction when debugging.
         */
        if (isDebugEnabled) {
          LOG.debug(outputContext.getDestinationVertexName() + ": Index list is empty... returning");
        }
        return;
      }

      if (!isFinalMergeEnabled) {
        // For pipelined shuffle, previous events are already sent. Just generate the last event alone
        assert isPipelinedShuffle;
        int startIndex = numSpills - 1;
        int endIndex = numSpills;

        for (int i = startIndex; i < endIndex; i++) {
          boolean isLastEvent = (i == numSpills - 1);
          String pathComponent = (outputContext.getUniqueIdentifier() + "_" + i);
          ShuffleUtils.generateEventOnSpill(finalEvents, isFinalMergeEnabled, isLastEvent,
              outputContext, i, spillInfoList.get(i).spillRecord, partitions,
              sendEmptyPartitionDetails, pathComponent, partitionStats,
              reportDetailedPartitionStats(), auxiliaryService, deflater);
          if (isDebugEnabled) {
            LOG.debug("{}: Adding spill event for spill (final update={}), spillId={}",
                outputContext.getDestinationVertexName(), isLastEvent, i);
          }
        }
        return;
      }

      numAdditionalSpillsCounter.increment(numSpills - 1);

      // Now, isFinalMergeEnabled == true
      // So, we have to increment fileOutputByteCounter because spill() does not increment it.

      // In case final merge is required, the following code path is executed.
      if (numSpills == 1) {
        SpillInfo spillInfo = spillInfoList.get(0);
        MultiByteArrayOutputStream spillByteArrayOutput = spillInfo.spillOutput;
        // TODO: someday be able to pass this directly to shuffle without writing to disk
        //
        // Originally we rename the directory by removing the suffix _0, e.g.:
        //   .../attempt_1734148871257_0437_1_02_000001_0_10031_0/file.out
        //   -->
        //   .../attempt_1734148871257_0437_1_02_000001_0_10031/file.out
        //
        // As a minor optimization, we skip renaming and use the existing output, e.g., ".../...10031_0/file.out".
        // OrderedPartitionedKVOutput.generateEvents() adjusts pathComponent by appending "_0" so that
        // downstream tasks can request ".../...10031_0/file.out" instead of ".../...10031/file.out".
        finalOutputFile = spillInfo.spillFilePath;
        if (writeSpillRecord) {
          finalIndexFile = spillInfo.spillIndexPath;
        }
        finalIndexComputed = true;  // because final TezSpillRecord can be obtained

        if (isDebugEnabled) {
          LOG.debug(outputContext.getDestinationVertexName() + ": numSpills=" + numSpills +
              ", finalOutputFile=" + finalOutputFile + ", finalIndexFile=" + finalIndexFile);
        }

        String uniqueId = ShuffleUtils.getUniqueIdentifierSpillId(outputContext, 0);
        String pathComponent = compositeFetch ?
            ShuffleUtils.buildTezShuffleMapId(outputContext.getTaskVertexIndex(), uniqueId) : uniqueId;
        // read back TezSpillRecord (which might be on local disk)
        TezSpillRecord spillRecord = ShuffleUtils.getTezSpillRecord(
            outputContext, pathComponent, finalIndexFile, localFs);

        if (reportPartitionStats()) {
          for (int i = 0; i < spillRecord.size(); i++) {
            partitionStats[i] += spillRecord.getIndex(i).getRawLength();
          }
        }
        // numShuffleChunks.setValue(numSpills);

        if (spillByteArrayOutput == null) {
          // finalOutputFile is served to downstream tasks, so increment fileOutputByteCounter
          fileOutputBytesCounter.increment(localFs.getFileStatus(finalOutputFile).getLen());
          spillInfoList.clear();
          return;
        }

        long sumPartLength = 0L;
        for (int i = 0; i < spillRecord.size(); i++) {
          sumPartLength += spillRecord.getIndex(i).getPartLength();
        }
        fileOutputBytesMemoryCounter.increment(sumPartLength);

        // TODO: why are events not being sent here???
        spillInfoList.clear();
        return;
      }

      finalOutputFile = mapOutputFile.getOutputFileForWrite(0);
      if (writeSpillRecord) {
        finalIndexFile = mapOutputFile.getOutputIndexFileForWrite(0);
      }
      finalIndexComputed = true;  // because final TezSpillRecord can be obtained

      if (isDebugEnabled) {
        LOG.debug(outputContext.getDestinationVertexName() + ": numSpills: " + numSpills +
            ", finalOutputFile:" + finalOutputFile + ", finalIndexFile:" + finalIndexFile);
      }

      MultiByteArrayOutputStream byteArrayOutput = null;
      if (useFreeMemoryWriterOutput
          && MultiByteArrayOutputStream.canUseFreeMemoryBuffers(freeMemoryThreshold)) {
        byteArrayOutput = new MultiByteArrayOutputStream(localFs, finalOutputFile);
      }

      // the output stream for the final single output file
      FSDataOutputStream finalOut = null;
      if (byteArrayOutput == null) {
        finalOut = localFs.create(finalOutputFile, true, 4096);
        ensureSpillFilePermissions(finalOutputFile, localFs, localFsSpillFilePerms);
      } else {
        finalOut = new FSDataOutputStream(byteArrayOutput, null);
      }

      final TezSpillRecord spillRec = new TezSpillRecord(partitions);
      final boolean isFinalMergeRleEnabled = merger.needsRLE();
      long finalOutputSize = 0;
      try {
        for (int parts = 0; parts < partitions; parts++) {
          boolean shouldWrite = false;
          //create the segments to be merged
          List<Segment> segmentList = new ArrayList<Segment>(numSpills);
          for (int i = 0; i < numSpills; i++) {
            SpillInfo spillInfo = spillInfoList.get(i);
            TezIndexRecord indexRecord = spillInfo.spillRecord.getIndex(parts);
            if (indexRecord.hasData() || !sendEmptyPartitionDetails) {
              shouldWrite = true;
              segmentList.add(createSegmentFromSpill(spillInfo, parts));
            }
          }

          int mergeFactor = this.conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR,
              TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR_DEFAULT);
          // sort the segments only if there are intermediate merges
          boolean sortSegments = segmentList.size() > mergeFactor;
          // merge
          TezRawKeyValueIterator kvIter = TezMerger.merge(conf, localFs,
              codec, segmentList, mergeFactor, 0,
              new Path(uniqueIdentifier),
              sortSegments, null, spilledRecordsCounter,
              additionalSpillBytesReadCounter, isFinalMergeRleEnabled, outputContext);
          // write merged output to disk
          long segmentStart = finalOut.getPos();
          long rawLength = 0;
          long partLength = 0;
          WriterDataInputBuffer writer = null;
          if (shouldWrite) {
            writer = new WriterDataInputBuffer(
                finalOut,
                codec, spilledRecordsCounter, null, false, isFinalMergeRleEnabled,
                -1, -1,
                writeBuffer, null, outputContext);
            TezMerger.writeFile(kvIter, writer,
                TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);

            //close
            writer.close();
            rawLength = writer.getRawLength();
            partLength = writer.getCompressedLength();
          }
          outputBytesWithOverheadCounter.increment(rawLength);
          finalOutputSize += partLength;

          // record offsets
          final TezIndexRecord rec = new TezIndexRecord(segmentStart, rawLength, partLength);
          spillRec.putIndex(rec, parts);
          if (reportPartitionStats()) {
            partitionStats[parts] += rawLength;
          }
        }
      } finally {
        finalOut.close();
      }

      // numShuffleChunks.setValue(1); // final merge has happened.

      // final output is served to downstream tasks.
      if (byteArrayOutput == null) {
        fileOutputBytesCounter.increment(finalOutputSize);
      } else {
        fileOutputBytesMemoryCounter.increment(finalOutputSize);
        assert !writeSpillRecord;
      }

      if (writeSpillRecord) {
        spillRec.writeToFile(finalIndexFile, localFs, localFsSpillFilePerms);
      } else {
        Path outputFilePath = byteArrayOutput == null ? finalOutputFile : null;
        ShuffleUtils.writeToIndexPathCacheAndByteCache(outputContext,
            outputFilePath, spillRec, byteArrayOutput, null);
      }

      for (int i = 0; i < numSpills; i++) {
        Path spillFilename = spillFilePaths.get(i);
        localFs.delete(spillFilename, true);
      }
      spillFilePaths.clear();

      if (writeSpillRecord) {
        for (int i = 0; i < numSpills; i++) {
          Path indexFilename = spillFileIndexPaths.get(i);
          localFs.delete(indexFilename, true);
        }
        spillFileIndexPaths.clear();
      }
      cleanSpillOutputBuffers();
      spillInfoList.clear();
    } catch(InterruptedException ie) {
      cancelActiveSortTasks();
      if (cleanup) {
        cleanup();
      }
      Thread.currentThread().interrupt();
      throw new IOInterruptedException("Interrupted while closing Output", ie);
    }
  }

  /**
   * Close and send events.
   * @return events to be returned by the edge.
   * @throws IOException parent can throw this.
   */
  synchronized public final List<Event> close() throws IOException {
    if (writeSpillRecord) {
      spillFileIndexPaths.clear();
    }
    spillFilePaths.clear();
    return finalEvents;
  }

  public TezTaskOutput getMapOutput() {
    return mapOutputFile;
  }

  public boolean getFinalIndexComputed() {
    return finalIndexComputed;
  }

  public Path getFinalIndexFile() {
    return finalIndexFile;
  }

  public Path getFinalOutputFile() {
    return finalOutputFile;
  }

  public static long getInitialMemoryRequirement(Configuration conf, long maxAvailableTaskMemory) {
    int initialMemRequestMb = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB,
        TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB_DEFAULT);
    long reqBytes = ((long) initialMemRequestMb) << 20;
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

  private synchronized void cleanup() throws IOException {
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

  private synchronized void cleanup(Path path) {
    if (path == null || !cleanup) {
      return;
    }
    try {
      LOG.info("Deleting " + path);
      localFs.delete(path, true);
    } catch (IOException ioe) {
      LOG.warn("Error in deleting " + path);
    }
  }

  private synchronized void cleanup(Map<Integer, Path> spillMap) {
    if (!cleanup) {
      return;
    }
    for (Map.Entry<Integer, Path> entry : spillMap.entrySet()) {
      cleanup(entry.getValue());
    }
  }

  public long[] getPartitionStats() {
    return partitionStats;
  }

  private boolean reportPartitionStats() {
    return (partitionStats != null);
  }

  public boolean reportDetailedPartitionStats() {
    return reportPartitionStats.isPrecise();
  }


  private interface PartitionedRawKeyValueIterator extends TezRawKeyValueIterator {
    int getPartition();
    Integer peekPartition();
  }

  private static class BufferStreamWrapper extends OutputStream
  {
    private final ByteBuffer out;
    public BufferStreamWrapper(ByteBuffer out) {
      this.out = out;
    }
    
    @Override
    public void write(int b) throws IOException { out.put((byte)b); }
    @Override
    public void write(byte[] b) throws IOException { out.put(b); }
    @Override
    public void write(byte[] b, int off, int len) throws IOException { out.put(b, off, len); }
  }

  private static final class InputByteBuffer extends DataInputBuffer {
    private byte[] buffer = new byte[256]; 
    private ByteBuffer wrapped = ByteBuffer.wrap(buffer);
    private void resize(int length) {
      if (length > buffer.length || (buffer.length > 10 * (1+length))) {
        // scale down as well as scale up across values
        buffer = new byte[length];
        wrapped = ByteBuffer.wrap(buffer);
      }
      wrapped.limit(length);
    }

    // shallow copy
    public void reset(DataInputBuffer clone) {
      byte[] data = clone.getData();
      int start = clone.getPosition();
      int length = clone.getLength() - start;
      super.reset(data, start, length);
    }

    // deep copy
    @SuppressWarnings("unused")
    public void copy(DataInputBuffer clone) {
      byte[] data = clone.getData();
      int start = clone.getPosition();
      int length = clone.getLength() - start;
      resize(length);
      System.arraycopy(data, start, buffer, 0, length);
      super.reset(buffer, 0, length);
    }
  }

  private final class SortSpan {
    final byte[] kvmetaArray;
    final long kvmetaBaseOffset;
    final int kvmetaCapacity;
    final ByteBuffer kvbuffer;
    final byte[] kvbufferArray;
    final int kvbufferArrayOffset;
    final NonSyncDataOutputStream out;

    private int index = 0;
    private long eq = 0;
    private boolean reinit = false;
    private int capacity;
    private int kvmetaPosition;
    private int kvmetaLimit;

    public SortSpan(ByteBuffer source, int maxItems, int perItem) {
      Preconditions.checkArgument(source.hasArray(), "SortSpan source must be backed by a byte[]");
      capacity = source.remaining();
      int metasize = METASIZE*maxItems;
      long dataSize = (long) maxItems * (long) perItem;
      if (capacity < (metasize+dataSize)) {
        // try to allocate less meta space, because we have sample data
        metasize = METASIZE*(capacity/(perItem+METASIZE));
      }
      int sourcePosition = source.position();
      ByteBuffer reserved = source.duplicate();
      reserved.mark();
      if (isDebugEnabled) {
        LOG.debug("{}: reserved.remaining()={}, reserved.metasize={}",
            outputContext.getDestinationVertexName(), reserved.remaining(), metasize);
      }
      reserved.position(sourcePosition + metasize);
      kvbuffer = reserved.slice();
      kvbufferArray = kvbuffer.array();
      kvbufferArrayOffset = kvbuffer.arrayOffset();
      kvmetaArray = source.array();
      kvmetaBaseOffset = FastByteComparisons.BYTE_ARRAY_BASE_OFFSET + source.arrayOffset() + sourcePosition;
      kvmetaCapacity = metasize;
      kvmetaPosition = 0;
      kvmetaLimit = metasize;
      out = new NonSyncDataOutputStream(
              new BufferStreamWrapper(kvbuffer));
    }

    public SpanIterator sort() {
      if (length() > 1) {
        quicksort(length());
      }
      if (isDebugEnabled) { LOG.debug("{}: done sorting span={}, length={}",
          outputContext.getDestinationVertexName(), index, length()); }
      return new SpanIterator(this);
    }

    private void downHeap(final int b, int i, final int N) {
      for (int idx = i << 1; idx < N; idx = i << 1) {
        if (idx + 1 < N && this.compare(b + idx, b + idx + 1) < 0) {
          if (this.compare(b + i, b + idx + 1) < 0) {
            this.swap(b + i, b + idx + 1);
          } else return;
          i = idx + 1;
        } else if (this.compare(b + i, b + idx) < 0) {
          this.swap(b + i, b + idx);
          i = idx;
        } else return;
      }
    }

    private void heapSort(final int p, final int r) {
      final int N = r - p;
      // build heap w/ reverse comparator, then write in-place from end
      final int t = Integer.highestOneBit(N);
      for (int i = t; i > 1; i >>>= 1) {
        for (int j = i >>> 1; j < i; ++j) {
          downHeap(p-1, j, N + 1);
        }
      }
      for (int i = r - 1; i > p; --i) {
        this.swap(p, i);
        downHeap(p - 1, 1, i - p + 1);
      }
    }

    private void fix(int p, int r) {
      if (this.compare(p, r) > 0) {
        this.swap(p, r);
      }
    }

    /**
     * Deepest recursion before giving up and doing a heapsort.
     * Returns 2 * ceil(log(n)).
     *
     * @param x x.
     * @return MaxDepth.
     */
    private int getMaxDepth(int x) {
      return (32 - Integer.numberOfLeadingZeros(x - 1)) << 2;
    }

    /**
     * Sort the given range of items using quick sort.
     * {@inheritDoc} If the recursion depth falls below {@link #getMaxDepth},
     * then switch to {@link HeapSort}.
     */
    private void quicksort(int r) {
      sortInternal(0, r, getMaxDepth(r));
    }

    private void sortInternal(int p, int r, int depth) {
      // from org/apache/hadoop/util/QuickSort.java
      while (true) {
      if (r-p < 13) {
        for (int i = p; i < r; ++i) {
          for (int j = i; j > p && this.compare(j-1, j) > 0; --j) {
            this.swap(j, j-1);
          }
        }
        return;
      }
      if (--depth < 0) {
        // give up
        heapSort(p, r);
        return;
      }

      // select, move pivot into first position
      fix((p+r) >>> 1, p);
      fix((p+r) >>> 1, r - 1);
      fix(p, r-1);

      // Divide
      int i = p;
      int j = r;
      int ll = p;
      int rr = r;
      int cr;
      while(true) {
        while (++i < j) {
          if ((cr = this.compare(i, p)) > 0) break;
          if (0 == cr && ++ll != i) {
            this.swap(ll, i);
          }
        }
        while (--j > i) {
          if ((cr = this.compare(p, j)) > 0) break;
          if (0 == cr && --rr != j) {
            this.swap(rr, j);
          }
        }
        if (i < j) this.swap(i, j);
        else break;
      }
      j = i;
      // swap pivot- and all eq values- into position
      while (ll >= p) {
        this.swap(ll--, --i);
      }
      while (rr < r) {
        this.swap(rr++, j++);
      }

      // Conquer
      // Recurse on smaller interval first to keep stack shallow
      assert i != j;
      if (i - p < r - j) {
        sortInternal(p, i, depth);
        p = j;
      } else {
        sortInternal(j, r, depth);
        r = i;
      }
      }
    }

    private int offsetFor(int i) {
      return (i * NMETA);
    }

    private int longOffsetFor(int i) {
      return i * (NMETA / 2);
    }

    private long offsetForIntIndex(int intIndex) {
      return kvmetaBaseOffset + (((long) intIndex) << 2);
    }

    private long offsetForLongIndex(int longIndex) {
      return kvmetaBaseOffset + (((long) longIndex) << 3);
    }

    private int metaRemaining() {
      return kvmetaLimit - kvmetaPosition;
    }

    private void putMetaLong(int first, int second) {
      long firstBits = first & 0xFFFFFFFFL;
      long secondBits = second & 0xFFFFFFFFL;
      FastByteComparisons.theUnsafe.putLong(
          kvmetaArray, kvmetaBaseOffset + kvmetaPosition, firstBits | (secondBits << Integer.SIZE));
      kvmetaPosition += Long.BYTES;
    }

    private void swap(final int mi, final int mj) {
      final int kvi = longOffsetFor(mi);
      final int kvj = longOffsetFor(mj);
      final long l1 = FastByteComparisons.theUnsafe.getLong(kvmetaArray, offsetForLongIndex(kvi));
      final long l2 = FastByteComparisons.theUnsafe.getLong(kvmetaArray, offsetForLongIndex(kvi + 1));

      FastByteComparisons.theUnsafe.putLong(
          kvmetaArray,
          offsetForLongIndex(kvi),
          FastByteComparisons.theUnsafe.getLong(kvmetaArray, offsetForLongIndex(kvj)));
      FastByteComparisons.theUnsafe.putLong(
          kvmetaArray,
          offsetForLongIndex(kvi + 1),
          FastByteComparisons.theUnsafe.getLong(kvmetaArray, offsetForLongIndex(kvj + 1)));

      FastByteComparisons.theUnsafe.putLong(kvmetaArray, offsetForLongIndex(kvj), l1);
      FastByteComparisons.theUnsafe.putLong(kvmetaArray, offsetForLongIndex(kvj + 1), l2);
    }

    private int compareKeys(final int kvi, final int kvj) {
      final long ipair = FastByteComparisons.theUnsafe.getLong(
          kvmetaArray, offsetForLongIndex(longOffsetFor(kvi >>> 2)));
      final int istart = (int) ipair;
      final int ilen   = ((int) (ipair >>> Integer.SIZE)) - istart;
      final long jpair = FastByteComparisons.theUnsafe.getLong(
          kvmetaArray, offsetForLongIndex(longOffsetFor(kvj >>> 2)));
      final int jstart = (int) jpair;
      final int jlen   = ((int) (jpair >>> Integer.SIZE)) - jstart;

      if (ilen == 0 || jlen == 0) {
        if (ilen == jlen) {
          eq++;
        }
        return ilen - jlen;
      }

      // sort by key
      final int cmp = FastByteComparisons.compareTo(
          kvbufferArray, kvbufferArrayOffset + istart, ilen,
          kvbufferArray, kvbufferArrayOffset + jstart, jlen);
      if (cmp == 0) eq++;
      return cmp;
    }

    private int compare(final int mi, final int mj) {
      final int kvi = offsetFor(mi);
      final int kvj = offsetFor(mj);
      final int kvip = FastByteComparisons.theUnsafe.getInt(
          kvmetaArray, offsetForIntIndex(kvi + PARTITION));
      final int kvjp = FastByteComparisons.theUnsafe.getInt(
          kvmetaArray, offsetForIntIndex(kvj + PARTITION));
      // sort by partition      
      if (kvip != kvjp) {
        return kvip - kvjp;
      }
      return compareKeys(kvi, kvj);
    }

    private SortSpan next() {
      ByteBuffer remaining = end();
      if (remaining != null) {
        SortSpan newSpan = null;
        int items = length();
        int perItem = kvbuffer.position()/items;
        if (reinit) { //next mem block
          //quite possible that the previous span had a length of 1. It is better to reinit here for new span.
          items = 1024*1024;
          perItem = 16;
        }
        newSpan = new SortSpan(remaining, items, perItem);
        newSpan.index = index+1;
        if (isDebugEnabled) {
          LOG.debug("{}, counter:{}",
              String.format(outputContext.getDestinationVertexName() + ": New Span%d.length = %d, perItem = %d", newSpan.index, newSpan.length(), perItem),
              outputRecordsCounter.getValue());
        }
        return newSpan;
      }
      return null;
    }

    private int length() {
      return kvmetaLimit / METASIZE;
    }

    private ByteBuffer end() {
      ByteBuffer remaining = kvbuffer.duplicate();
      remaining.position(kvbuffer.position());
      remaining = remaining.slice();
      kvbuffer.limit(kvbuffer.position());
      kvmetaLimit = kvmetaPosition;
      int items = length();
      if (items == 0) {
        return null;
      }
      int perItem = kvbuffer.position()/items;
      if (isDebugEnabled) {
        LOG.debug("{}: {}", outputContext.getDestinationVertexName(),
            String.format("Span%d.length = %d, perItem = %d", index, length(), perItem));
      }
      if (remaining.remaining() < METASIZE+perItem) {
        //Check if we can get the next Buffer from the main buffer list
        ByteBuffer space = allocateSpace();
        if (space != null) {
          LOG.info("{}: Getting memory from next block in the list, recordsWritten={}",
              outputContext.getDestinationVertexName(), outputRecordsCounter.getValue());
          reinit = true;
          return space;
        }
        return null;
      }
      return remaining;
    }

    private int compareInternal(final DataInputBuffer needle, final int needlePart, final int index) {
      int cmp = 0;
      final int keystart;
      final int valstart;
      final int partition;
      partition = FastByteComparisons.theUnsafe.getInt(
          kvmetaArray, offsetForIntIndex(this.offsetFor(index) + PARTITION));
      if (partition != needlePart) {
          cmp = (partition-needlePart);
      } else {
        long keyValStartPair = FastByteComparisons.theUnsafe.getLong(
            kvmetaArray, offsetForLongIndex(longOffsetFor(index)));
        keystart = (int) keyValStartPair;
        valstart = (int) (keyValStartPair >>> Integer.SIZE);
        final byte[] buf = kvbuffer.array();
        final int off = kvbuffer.arrayOffset();
        cmp = FastByteComparisons.compareTo(buf,
            keystart + off , (valstart - keystart),
            needle.getData(),
            needle.getPosition(), (needle.getLength() - needle.getPosition()));
      }
      return cmp;
    }
    
    private long getEq() {
      return eq;
    }
    
    @Override
    public String toString() {
        return String.format("Span[%d,%d]", kvmetaCapacity, kvbuffer.limit());
    }
  }

  private static class SpanIterator implements PartitionedRawKeyValueIterator, Comparable<SpanIterator> {
    private int kvindex = -1;
    private final int maxindex;
    private final byte[] kvbufferArray;
    private final int kvbufferArrayOffset;
    private final SortSpan span;
    private final InputByteBuffer key = new InputByteBuffer();
    private int partition;
    private int keyStart;
    private int keyLength;
    private int valueStart;
    private int valueLength;

    private static final int minrun = (1 << 4);

    public SpanIterator(SortSpan span) {
      ByteBuffer kvbuffer = span.kvbuffer;
      this.kvbufferArray = kvbuffer.array();
      this.kvbufferArrayOffset = kvbuffer.arrayOffset();
      this.span = span;
      this.maxindex = span.length() - 1;
    }

    public DataInputBuffer getKey()  {
      key.reset(kvbufferArray, kvbufferArrayOffset + keyStart, keyLength);
      return key;
    }

    public DataInputBuffer getValue() {
      assert false;
      return null;
    }

    private void resetKeyTo(InputByteBuffer target) {
      target.reset(kvbufferArray, kvbufferArrayOffset + keyStart, keyLength);
    }

    private void resetValueTo(InputByteBuffer target) {
      target.reset(kvbufferArray, kvbufferArrayOffset + valueStart, valueLength);
    }

    public boolean next() {
      // caveat: since we use this as a comparable in the merger 
      if (kvindex == maxindex) return false;
      kvindex += 1;
      loadCurrentRecordMetadata();
      return true;
    }

    private void loadCurrentRecordMetadata() {
      final long keyValStartPair = FastByteComparisons.theUnsafe.getLong(
          span.kvmetaArray, span.offsetForLongIndex(span.longOffsetFor(kvindex)));
      keyStart = (int) keyValStartPair;
      valueStart = (int) (keyValStartPair >>> Integer.SIZE);
      keyLength = valueStart - keyStart;
      int kvindexOffset = span.offsetFor(kvindex);
      valueLength = FastByteComparisons.theUnsafe.getInt(
          span.kvmetaArray, span.offsetForIntIndex(kvindexOffset + VALLEN));
      partition = FastByteComparisons.theUnsafe.getInt(
          span.kvmetaArray, span.offsetForIntIndex(kvindexOffset + PARTITION));
    }

    @Override
    public boolean hasNext() {
      return (kvindex < maxindex);
    }

    public void close() {
    }

    @Override
    public boolean isSameKey() {
      return false;
    }

    public int getPartition() {
      return partition;
    }

    public Integer peekPartition() {
      if (!hasNext()) {
        return null;
      } else {
          return FastByteComparisons.theUnsafe.getInt(
              span.kvmetaArray, span.offsetForIntIndex(span.offsetFor(kvindex + 1) + PARTITION));
      }
    }

    @SuppressWarnings("unused")
    public int size() {
      return (maxindex - kvindex);
    }

    public int compareTo(SpanIterator other) {
      if (partition != other.partition) {
        return partition - other.partition;
      }
      return FastByteComparisons.compareTo(
          kvbufferArray, kvbufferArrayOffset + keyStart, keyLength,
          other.kvbufferArray, other.kvbufferArrayOffset + other.keyStart, other.keyLength);
    }
    
    @Override
    public String toString() {
      return String.format("SpanIterator<%d:%d> (span=%s)", kvindex, maxindex, span.toString());
    }

    /**
     * bisect returns the next insertion point for a given raw key, skipping keys
     * which are <= needle using a binary search instead of a linear comparison.
     * This is massively efficient when long strings of identical keys occur.
     * @param needle 
     * @param needlePart
     * @return
     */
    int bisect(DataInputBuffer needle, int needlePart) {
      int start = kvindex;
      int end = maxindex-1;
      int mid = start;
      int cmp = 0;

      if (end - start < minrun) {
        return 0;
      }

      if (span.compareInternal(needle, needlePart, start) > 0) {
        return kvindex;
      }
      
      // bail out early if we haven't got a min run 
      if (span.compareInternal(needle, needlePart, start+minrun) > 0) {
        return 0;
      }

      if (span.compareInternal(needle, needlePart, end) < 0) {
        return end - kvindex;
      }
      
      boolean found = false;
      
      // Bound the search work: the span can be large, but this bisection is an
      // optimization only, and minrun already defines the minimum profitable run.
      for (int i = 0; start < end && i < minrun; i++) {
        mid = start + (end - start)/2;
        cmp = span.compareInternal(needle, needlePart, mid);
        if (cmp == 0) {
          start = mid;
          found = true;
        } else if (cmp < 0) {
          start = mid; 
          found = true;
        }
        if (cmp > 0) {
          end = mid;
        }
      }

      if (found) {
        return start - kvindex;
      }
      return 0;
    }
  }

  private static class SortTask implements Callable<SpanIterator> {
    private final SortSpan sortable;

    public SortTask(SortSpan sortable) {
        this.sortable = sortable;
    }

    @Override
    public SpanIterator call() {
      // TODO: set MDC context with TaskContext.getMdcContext() and ShuffleUtils.restoreMdc()
      return sortable.sort();
    }
  }

  private class PartitionFilter implements TezRawKeyValueIterator {
    private final PartitionedRawKeyValueIterator iter;
    private int partition;
    private boolean dirty = false;
    public PartitionFilter(PartitionedRawKeyValueIterator iter) {
      this.iter = iter;
    }
    public DataInputBuffer getKey() throws IOException { return iter.getKey(); }
    public DataInputBuffer getValue() throws IOException { return iter.getValue(); }
    public void close() throws IOException { }

    @Override
    public boolean isSameKey() {
      return iter.isSameKey();
    }

    public boolean next() throws IOException {
      if (dirty || iter.next()) {
        int prefix = iter.getPartition();

        if ((prefix >>> (32 - partitionBits)) == partition) {
          dirty = false; // we found what we were looking for, good
          return true;
        } else if (!dirty) {
          dirty = true; // we did a lookahead and failed to find partition
        }
      }
      return false;
    }

    @Override
    public boolean hasNext() throws IOException {
      if (dirty || iter.hasNext()) {
        Integer part;
        if (dirty) {
          part = iter.getPartition();
        } else {
          part = iter.peekPartition();
        }

        if (part != null) {
          return (part >>> (32 - partitionBits)) == partition;
        }
      }
      return false;
    }

    public void reset(int partition) {
      this.partition = partition;
    }

    @SuppressWarnings("unused")
    public int getPartition() {
      return this.partition;
    }
  }

  private static class SpanHeap implements Iterable<SpanIterator> {
    private SpanIterator[] spans;
    private boolean[] active;
    private int[] losers;
    private int leafCount;
    private int spanCount;
    private int activeCount;
    private int winner = -1;
    private int lastPopped = -1;
    private boolean built = false;

    public SpanHeap() {
      leafCount = 256;
      spans = new SpanIterator[leafCount];
      active = new boolean[leafCount];
      losers = new int[leafCount];
      clearLosers();
    }

    public boolean add(SpanIterator iter) {
      if (lastPopped >= 0 && spans[lastPopped] == iter) {
        if (!active[lastPopped]) {
          active[lastPopped] = true;
          activeCount++;
        }
        replay(lastPopped);
        lastPopped = -1;
        return true;
      }

      ensureCapacity(spanCount + 1);
      spans[spanCount] = iter;
      active[spanCount] = true;
      spanCount++;
      activeCount++;
      built = false;
      return true;
    }

    public boolean isEmpty() {
      return activeCount == 0;
    }

    public int size() {
      return activeCount;
    }

    /**
     * Returns the current winner. The winner remains in the tree until
     * replaceTop() replays the winner's leaf with its next record or removes it.
     * @return the smallest SpanIterator, or null if the tree is empty
     */
    public SpanIterator pop() {
      buildIfNeeded();
      if (winner < 0) {
        return null;
      }
      lastPopped = winner;
      return spans[winner];
    }

    public SpanIterator peek() {
      buildIfNeeded();
      if (lastPopped >= 0) {
        return peekAfterPop();
      }
      return winner < 0 ? null : spans[winner];
    }

    public void replaceTop(boolean hasNext) {
      if (lastPopped < 0) {
        return;
      }
      if (!hasNext && active[lastPopped]) {
        active[lastPopped] = false;
        activeCount--;
      }
      replay(lastPopped);
      lastPopped = -1;
    }

    @Override
    public Iterator<SpanIterator> iterator() {
      return new Iterator<SpanIterator>() {
        private int index = 0;

        @Override
        public boolean hasNext() {
          while (index < spanCount && !active[index]) {
            index++;
          }
          return index < spanCount;
        }

        @Override
        public SpanIterator next() {
          hasNext();
          return spans[index++];
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    private void ensureCapacity(int capacity) {
      if (capacity <= leafCount) {
        return;
      }
      int newLeafCount = leafCount;
      while (newLeafCount < capacity) {
        newLeafCount <<= 1;
      }

      SpanIterator[] newSpans = new SpanIterator[newLeafCount];
      boolean[] newActive = new boolean[newLeafCount];
      System.arraycopy(spans, 0, newSpans, 0, spanCount);
      System.arraycopy(active, 0, newActive, 0, spanCount);
      spans = newSpans;
      active = newActive;
      leafCount = newLeafCount;
      losers = new int[leafCount];
      built = false;
    }

    private void buildIfNeeded() {
      if (built) {
        return;
      }
      clearLosers();
      winner = build(1);
      built = true;
    }

    private int build(int node) {
      if (node >= leafCount) {
        int index = node - leafCount;
        return index < spanCount && active[index] ? index : -1;
      }

      int left = build(node << 1);
      int right = build((node << 1) + 1);
      if (left < 0) {
        return right;
      }
      if (right < 0) {
        return left;
      }
      if (lessThanOrEqual(left, right)) {
        losers[node] = right;
        return left;
      }
      losers[node] = left;
      return right;
    }

    private void replay(int index) {
      buildIfNeeded();
      int candidate = active[index] ? index : -1;
      for (int node = (index + leafCount) >> 1; node > 0; node >>= 1) {
        int challenger = losers[node];
        if (challenger < 0) {
          continue;
        }
        if (candidate < 0) {
          losers[node] = -1;
          candidate = challenger;
        } else if (lessThanOrEqual(candidate, challenger)) {
          losers[node] = challenger;
        } else {
          losers[node] = candidate;
          candidate = challenger;
        }
      }
      winner = candidate;
    }

    private SpanIterator peekAfterPop() {
      int next = -1;
      for (int node = (lastPopped + leafCount) >> 1; node > 0; node >>= 1) {
        int challenger = losers[node];
        if (challenger < 0 || challenger == lastPopped || !active[challenger]) {
          continue;
        }
        if (next < 0 || lessThanOrEqual(challenger, next)) {
          next = challenger;
        }
      }
      return next < 0 ? null : spans[next];
    }

    private boolean lessThanOrEqual(int left, int right) {
      int cmp = spans[left].compareTo(spans[right]);
      return cmp < 0 || (cmp == 0 && left <= right);
    }

    private void clearLosers() {
      Arrays.fill(losers, -1);
    }
  }

  public boolean needsRLE() {
    return merger.needsRLE();
  }

  private void cancelActiveSortTasks() {
    merger.cancelOutstandingSorts();
  }

  private final class SpanMerger implements PartitionedRawKeyValueIterator {
    InputByteBuffer key = new InputByteBuffer();
    InputByteBuffer value = new InputByteBuffer();
    int partition;

    private ArrayList< Future<SpanIterator>> futures = new ArrayList< Future<SpanIterator>>();

    private SpanHeap heap = new SpanHeap();
    private PartitionFilter partIter;

    private int gallop = 0;
    private SpanIterator horse;
    private long total = 0;
    private long eq = 0;
    
    public SpanMerger() {
      // SpanIterators are comparable
      partIter = new PartitionFilter(this);
    }

    public void add(SpanIterator iter) {
      if (iter.next()) {
        heap.add(iter);
      }
    }

    public void add(Future<SpanIterator> iter) {
      this.futures.add(iter);
    }

    public boolean ready() throws IOException, InterruptedException {
      int numSpanItr = futures.size();
      try {
        while (!this.futures.isEmpty()) {
          Future<SpanIterator> futureIter = this.futures.get(0);
          SpanIterator iter = futureIter.get();
          this.futures.remove(0);
          this.add(iter);
        }

        if (heap.isEmpty()) {
          return false;
        }
        for (SpanIterator sp: heap) {
          total += sp.span.length();
          eq += sp.span.getEq();
        }
        if (isDebugEnabled) {
          StringBuilder sb = new StringBuilder();
          for (SpanIterator sp: heap) {
              sb.append(sp.toString());
              sb.append(",");
          }
          LOG.debug("{}: Heap = {}", outputContext.getDestinationVertexName(), sb.toString());
        }
        return true;
      } catch (InterruptedException e) {
        cancelOutstandingSorts();
        throw e;
      } catch(ExecutionException e) {
        LOG.error("Heap size={}, total={}, eq={}, partition={}, gallop={}, totalItr={},"
                + " futures.size={}, destVertexName={}",
            heap.size(), total, eq, partition, gallop, numSpanItr, futures.size(),
            outputContext.getDestinationVertexName(), e);
        throw new IOException(e);
      }
    }

    public void cancelOutstandingSorts() {
      for (Future<SpanIterator> future : futures) {
        future.cancel(true);
      }
      futures.clear();
    }

    private SpanIterator pop() {
      if (gallop > 0) {
        gallop--;
        return horse;
      }
      SpanIterator current = heap.pop();
      SpanIterator next = heap.peek();
      if (next != null && current != null &&
        ((Object)horse) == ((Object)current)) {
        // TODO: a better threshold check than 1 key repeating
        gallop = current.bisect(next.getKey(), next.getPartition())-1;
      }
      horse = current;
      return current;
    }
    
    public boolean needsRLE() {
      return (eq > 0.1 * total);
    }

    @SuppressWarnings("unused")
    private SpanIterator peek() {
      if (gallop > 0) {
        return horse;
      }
      return heap.peek();
    }

    public boolean next() {
      SpanIterator current = pop();

      if (current != null) {
        partition = current.getPartition();
        current.resetKeyTo(key);
        current.resetValueTo(value);
        if (gallop <= 0) {
          // since all keys and values are references to the kvbuffer, no more deep copies
          heap.replaceTop(current.next());
        } else {
          // galloping, no deep copies required anyway
          current.next();
        }
        return true;
      }
      return false;
    }

    @Override
    public boolean hasNext() {
      return peek() != null;
    }

    public Integer peekPartition() {
      if (!hasNext()) {
        return null;
      } else {
        SpanIterator peek = peek();
        return peek.getPartition();
      }
    }

    public DataInputBuffer getKey() { return key; }
    public DataInputBuffer getValue() { return value; }
    public int getPartition() { return partition; }

    public void close() throws IOException {
    }

    @Override
    public boolean isSameKey() {
      return false;
    }

    public TezRawKeyValueIterator filter(int partition) {
      partIter.reset(partition);
      return partIter;
    }

  }
}
