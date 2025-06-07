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
package org.apache.tez.runtime.library.common.writers;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.Deflater;

import com.google.common.collect.Lists;
import com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.io.NonSyncDataOutputStream;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.ExecutorServiceUserGroupInformation;
import org.apache.tez.runtime.api.MultiByteArrayOutputStream;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.library.api.IOInterruptedException;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration.ReportPartitionStats;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tez.common.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;

import javax.annotation.Nullable;

import static org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord.ensureSpillFilePermissions;

public class UnorderedPartitionedKVWriter extends BaseUnorderedPartitionedKVWriter {

  private static final Logger LOG = LoggerFactory.getLogger(UnorderedPartitionedKVWriter.class);

  private static final int INT_SIZE = 4;
  private static final int NUM_META = 3; // Number of meta fields.
  private static final int INDEX_KEYLEN = 0; // KeyLength index
  private static final int INDEX_VALLEN = 1; // ValLength index
  private static final int INDEX_NEXT = 2; // Next Record Index.
  private static final int META_SIZE = NUM_META * INT_SIZE; // Size of total meta-data

  private final static int APPROX_HEADER_LENGTH = 150;

  // Maybe setup a separate statistics class which can be shared between the
  // buffer and the main path instead of having multiple arrays.

  private final String destNameTrimmed;
  private final boolean isFinalMergeEnabled;
  private final boolean pipelinedShuffle;
  // To store events when final merge is disabled
  private final List<Event> finalEvents;

  private final boolean dataViaEventsEnabled;
  private final int dataViaEventsMaxSize;
  private final boolean useCachedStream;

  private final boolean writeSpillRecord;

  private final long availableMemory;

  private final long freeMemoryThreshold;
  private final boolean useFreeMemoryWriterOutput;  // use availableMemory as threshold

  private final FileSystem rfs;

  // for single partition cases
  private final IFile.Writer writer;
  private final boolean skipBuffers;

  private int numBuffers;
  private int sizePerBuffer;
  private int lastBufferSize;
  private int spillLimit;

  private final ByteArrayOutputStream baos;
  private final NonSyncDataOutputStream dos;

  private BlockingQueue<WrappedBuffer> availableBuffers;
  private WrappedBuffer[] buffers;
  private int numInitializedBuffers;
  private WrappedBuffer currentBuffer;

  private final List<SpillInfo> spillInfoList = Collections
      .synchronizedList(new ArrayList<SpillInfo>());

  private Semaphore availableSlots;
  private ListeningExecutorService spillExecutor;

  private final int[] numRecordsPerPartition;
  // How partition stats should be reported.
  final ReportPartitionStats reportPartitionStats;
  private final long[] sizePerPartition;

  /**
   * Represents final number of records written (spills are not counted)
   */
  protected final TezCounter outputLargeRecordsCounter;

  private final long indexFileSizeEstimate;

  private long localOutputRecordBytesCounter = 0;
  private long localOutputBytesWithOverheadCounter = 0;
  private long localOutputRecordsCounter = 0;
  // notify after x records
  private static final int NOTIFY_THRESHOLD = 1000;

  // uncompressed size for each partition
  private volatile long spilledSize = 0;

  static final ThreadLocal<Deflater> deflater = new ThreadLocal<Deflater>() {
    @Override
    public Deflater initialValue() {
      return TezCommonUtils.newBestCompressionDeflater();
    }

    @Override
    public Deflater get() {
      Deflater deflater = super.get();
      deflater.reset();
      return deflater;
    }
  };

  private Throwable spillException;
  private final AtomicBoolean isShutdown = new AtomicBoolean(false);
  private final AtomicInteger numSpills = new AtomicInteger(0);
  private final AtomicInteger pendingSpillCount = new AtomicInteger(0);
  private final ReentrantLock spillLock = new ReentrantLock();
  private final Condition spillInProgress = spillLock.newCondition();

  private final List<WrappedBuffer> filledBuffers = new ArrayList<>();

  private Path finalIndexPath;  // null if writeSpillRecord == false
  private Path finalOutPath;

  public UnorderedPartitionedKVWriter(OutputContext outputContext, Configuration conf,
      int numOutputs, long availableMemoryBytes) throws IOException {
    super(outputContext, conf, numOutputs);

    Preconditions.checkArgument(availableMemoryBytes >= 0, "availableMemory should be >= 0 bytes");

    this.destNameTrimmed = TezUtilsInternal.cleanVertexName(outputContext.getDestinationVertexName());
    this.pipelinedShuffle = this.conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED,
        TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED_DEFAULT);
    // set isFinalMergeEnabled = !pipelinedShuffleConf unless set explicitly in tez-site.xml
    this.isFinalMergeEnabled = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT,
        !this.pipelinedShuffle);
    this.finalEvents = Lists.newLinkedList();

    this.dataViaEventsEnabled = conf.getBoolean(
       TezRuntimeConfiguration.TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_ENABLED,
       TezRuntimeConfiguration.TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_ENABLED_DEFAULT);
    // No max cap on size (intentional)
    this.dataViaEventsMaxSize = conf.getInt(
       TezRuntimeConfiguration.TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_MAX_SIZE,
       TezRuntimeConfiguration.TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_MAX_SIZE_DEFAULT);
    this.useCachedStream = this.dataViaEventsEnabled && (numPartitions == 1) && !pipelinedShuffle;

    this.writeSpillRecord = !this.compositeFetch;

    if (availableMemoryBytes == 0) {
      Preconditions.checkArgument(((numPartitions == 1) && !pipelinedShuffle),
        "availableMemory can be set to 0 only when numPartitions=1 and pipeline shuffle disabled");
    }

    // Ideally, should be significantly larger.
    this.availableMemory = availableMemoryBytes;

    this.freeMemoryThreshold = outputContext.getTotalMemoryAvailableToTask();
    // useFreeMemoryWriterOutput = false if compositeFetch == false, i.e, when using mapreduce_shuffle
    this.useFreeMemoryWriterOutput = compositeFetch && conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT_DEFAULT);

    this.rfs = ((LocalFileSystem) FileSystem.getLocal(this.conf)).getRaw();

    if (numPartitions == 1 && !pipelinedShuffle) {
      // special case, where in only one partition is available.
      skipBuffers = true;
      byte[] writeBuffer = IFile.allocateWriteBuffer();
      if (this.useCachedStream) {   // i.e., if dataViaEventsEnabled == true
        writer = new IFile.FileBackedInMemIFileWriter(keySerialization, valSerialization, rfs,
            outputFileHandler, keyClass, valClass, codec, outputRecordsCounter,
            outputRecordBytesCounter, dataViaEventsMaxSize,
            writeBuffer);
      } else {
        finalOutPath = outputFileHandler.getOutputFileForWrite();
        writer = new IFile.Writer(keySerialization, valSerialization, rfs, finalOutPath, keyClass, valClass,
            codec, outputRecordsCounter, outputRecordBytesCounter,
            writeBuffer);
        ensureSpillFilePermissions(finalOutPath, rfs);
      }
    } else {
      skipBuffers = false;
      writer = null;
    }

    baos = new ByteArrayOutputStream();
    dos = new NonSyncDataOutputStream(baos);
    keySerializer.open(dos);
    valSerializer.open(dos);

    if (!skipBuffers) {
      // Allow unit tests to control the buffer sizes.
      int maxSingleBufferSizeBytes = conf.getInt(
          TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_MAX_PER_BUFFER_SIZE_BYTES,
          Integer.MAX_VALUE);
      computeNumBuffersAndSize(maxSingleBufferSizeBytes);

      availableBuffers = new LinkedBlockingQueue<WrappedBuffer>();
      buffers = new WrappedBuffer[numBuffers];
      // Set up only the first buffer to start with.
      buffers[0] = new WrappedBuffer(numOutputs, sizePerBuffer);
      numInitializedBuffers = 1;
      if (LOG.isDebugEnabled()) {
        LOG.debug(destNameTrimmed + ": " + "Initializing Buffer #" +
            numInitializedBuffers + " with size=" + sizePerBuffer);
      }
      currentBuffer = buffers[0];

      int maxThreads = Math.max(2, numBuffers/2);
      ExecutorService executor = new ThreadPoolExecutor(1, maxThreads,
          60L, TimeUnit.SECONDS,
          new SynchronousQueue<Runnable>(),
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("UnorderedOutSpiller {" + outputContext.getDestinationVertexName() + "} #%d")
              .build()
      );
      // to restrict submission of more tasks than threads (e.g numBuffers > numThreads)
      // This is maxThreads - 1, to avoid race between callback thread releasing semaphore and the
      // thread calling tryAcquire.
      availableSlots = new Semaphore(maxThreads - 1, true);
      spillExecutor = MoreExecutors.listeningDecorator(executor);
    }

    numRecordsPerPartition = new int[numPartitions];
    reportPartitionStats = ReportPartitionStats.fromString(conf.get(
        TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS,
        TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS_DEFAULT));
    sizePerPartition = (reportPartitionStats.isEnabled()) ? new long[numPartitions] : null;

    outputLargeRecordsCounter = outputContext.getCounters().findCounter(
        TaskCounter.OUTPUT_LARGE_RECORDS);

    indexFileSizeEstimate = numPartitions * Constants.MAP_OUTPUT_INDEX_RECORD_LENGTH;

    LOG.info("{}: numBuffers={}, sizePerBuffer={}, numPartitions={}",
        destNameTrimmed, numBuffers, sizePerBuffer, numPartitions);
    if (LOG.isDebugEnabled()) {
      LOG.debug("skipBuffers=" + skipBuffers
          + ", availableMemory=" + availableMemory
          + ", pipelinedShuffle=" + pipelinedShuffle
          + ", isFinalMergeEnabled=" + isFinalMergeEnabled
          + ", numPartitions=" + numPartitions
          + ", reportPartitionStats=" + reportPartitionStats
          + ", dataViaEventsEnabled=" + dataViaEventsEnabled
          + ", dataViaEventsMaxSize=" + dataViaEventsMaxSize
          + ", useCachedStream=" + useCachedStream);
    }
  }

  private static final int ALLOC_OVERHEAD = 64;
  private void computeNumBuffersAndSize(int bufferLimit) {
    numBuffers = (int)(availableMemory / bufferLimit);

    if (numBuffers >= 2) {
      sizePerBuffer = bufferLimit - ALLOC_OVERHEAD;
      lastBufferSize = (int)(availableMemory % bufferLimit);
      // Use leftover memory last buffer only if the leftover memory > 50% of bufferLimit
      if (lastBufferSize > bufferLimit / 2) {
        numBuffers += 1;
      } else {
        if (lastBufferSize > 0) {
          LOG.warn("Underallocating memory. Unused memory size: {}.",  lastBufferSize);
        }
        lastBufferSize = sizePerBuffer;
      }
    } else {
      // We should have minimum of 2 buffers.
      numBuffers = 2;
      if (availableMemory / numBuffers > Integer.MAX_VALUE) {
        sizePerBuffer = Integer.MAX_VALUE;
      } else {
        sizePerBuffer = (int)(availableMemory / numBuffers);
      }
      // 2 equal sized buffers.
      lastBufferSize = sizePerBuffer;
    }
    // Ensure allocation size is multiple of INT_SIZE, truncate down.
    sizePerBuffer = sizePerBuffer - (sizePerBuffer % INT_SIZE);
    lastBufferSize = lastBufferSize - (lastBufferSize % INT_SIZE);

    int mergePercent = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_PARTITIONED_KVWRITER_BUFFER_MERGE_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_PARTITIONED_KVWRITER_BUFFER_MERGE_PERCENT_DEFAULT);
    spillLimit = numBuffers * mergePercent / 100;
    // Keep within limits.
    if (spillLimit < 1) {
      spillLimit = 1;
    }
    if (spillLimit > numBuffers) {
      spillLimit = numBuffers;
    }
  }

  @Override
  public void write(Object key, Object value) throws IOException {
    // Skipping checks for key-value types. IFile takes care of these, but should be removed from
    // there as well.

    // How expensive are checks like these ?
    if (isShutdown.get()) {
      throw new RuntimeException("Writer already closed");
    }
    if (spillException != null) {
      // Already reported as a fatalError - report to the user code
      throw new IOException("Exception during spill", new IOException(spillException));
    }
    if (skipBuffers) {
      // special case, where we have only one partition and pipelining is disabled.
      // The reason outputRecordsCounter isn't updated here:
      // For skipBuffers case, IFile writer has the reference to
      // outputRecordsCounter and during its close method call,
      // it will update the outputRecordsCounter.
      writer.append(key, value);
    } else {
      int partition = partitioner.getPartition(key, value, numPartitions);
      write(key, value, partition);
    }
  }

  @SuppressWarnings("unchecked")
  private void write(Object key, Object value, int partition) throws IOException {
    // Wrap to 4 byte (Int) boundary for metaData
    int mod = currentBuffer.nextPosition % INT_SIZE;
    int metaSkip = mod == 0 ? 0 : (INT_SIZE - mod);
    if ((currentBuffer.availableSize < (META_SIZE + metaSkip)) || (currentBuffer.full)) {
      // Move over to the next buffer.
      metaSkip = 0;
      setupNextBuffer();
    }
    currentBuffer.nextPosition += metaSkip;
    int metaStart = currentBuffer.nextPosition;
    currentBuffer.availableSize -= (META_SIZE + metaSkip);
    currentBuffer.nextPosition += META_SIZE;

    keySerializer.serialize(key);

    if (currentBuffer.full) {
      if (metaStart == 0) { // Started writing at the start of the buffer. Write Key to disk.
        // Key too large for any buffer. Write entire record to disk.
        currentBuffer.reset();
        writeLargeRecord(key, value, partition);
        return;
      } else { // Exceeded length on current buffer.
        // Try resetting the buffer to the next one, if this was not the start of a buffer,
        // and begin spilling the current buffer to disk if it has any records.
        setupNextBuffer();
        write(key, value, partition);
        return;
      }
    }

    int valStart = currentBuffer.nextPosition;
    valSerializer.serialize(value);

    if (currentBuffer.full) {
      // Value too large for current buffer, or K-V too large for entire buffer.
      if (metaStart == 0) {
        // Key + Value too large for a single buffer.
        currentBuffer.reset();
        writeLargeRecord(key, value, partition);
        return;
      } else { // Exceeded length on current buffer.
        // Try writing key+value to a new buffer - will fall back to disk if that fails.
        setupNextBuffer();
        write(key, value, partition);
        return;
      }
    }

    // Meta-data updates
    int metaIndex = metaStart / INT_SIZE;

    currentBuffer.metaBuffer.put(metaIndex + INDEX_KEYLEN, (valStart - (metaStart + META_SIZE)));
    currentBuffer.metaBuffer.put(metaIndex + INDEX_VALLEN, (currentBuffer.nextPosition - valStart));
    // This new record is currently the end of its partition's list in this buffer.
    currentBuffer.metaBuffer.put(metaIndex + INDEX_NEXT, WrappedBuffer.PARTITION_ABSENT_POSITION);

    currentBuffer.skipSize += metaSkip; // For size estimation
    // Update stats on number of records
    localOutputRecordBytesCounter += (currentBuffer.nextPosition - (metaStart + META_SIZE));
    localOutputBytesWithOverheadCounter += ((currentBuffer.nextPosition - metaStart) + metaSkip);
    localOutputRecordsCounter++;
    if (localOutputRecordBytesCounter % NOTIFY_THRESHOLD == 0) {
      updateTezCountersAndNotify();
    }

    int currentPartitionTailOffset = currentBuffer.partitionTails[partition];
    if (currentPartitionTailOffset != WrappedBuffer.PARTITION_ABSENT_POSITION) {
      // If there was a previous tail for this partition, link it to this new record.
      int previousTailMetaIndexInInts = currentPartitionTailOffset / INT_SIZE;
      currentBuffer.metaBuffer.put(previousTailMetaIndexInInts + INDEX_NEXT, metaStart);
    } else {
      // This is the first record for this partition in this buffer.
      currentBuffer.partitionHeads[partition] = metaStart;
    }
    // This new record becomes the new tail for this partition in this buffer.
    currentBuffer.partitionTails[partition] = metaStart;

    currentBuffer.recordsPerPartition[partition]++;
    currentBuffer.sizePerPartition[partition] += currentBuffer.nextPosition - (metaStart + META_SIZE);
    currentBuffer.numRecords++;
  }

  private void updateTezCountersAndNotify() {
    outputRecordBytesCounter.increment(localOutputRecordBytesCounter);
    outputBytesWithOverheadCounter.increment(localOutputBytesWithOverheadCounter);
    outputRecordsCounter.increment(localOutputRecordsCounter);
    localOutputRecordBytesCounter = 0;
    localOutputBytesWithOverheadCounter = 0;
    localOutputRecordsCounter = 0;
  }

  private void setupNextBuffer() throws IOException {
    if (currentBuffer.numRecords == 0) {
      currentBuffer.reset();
    } else {
      // Update overall stats
      final int filledBufferCount = filledBuffers.size();
      if (LOG.isDebugEnabled() || (filledBufferCount > 0 && (filledBufferCount % 10) == 0)) {
        LOG.info("{}: Moving to next buffer. Total filled buffers: {}", destNameTrimmed, filledBufferCount);
      }
      updateGlobalStats(currentBuffer);

      filledBuffers.add(currentBuffer);
      mayBeSpill(false);

      currentBuffer = getNextAvailableBuffer();

      // in case spill threads are free, check if spilling is needed
      mayBeSpill(false);
    }
  }

  private void mayBeSpill(boolean shouldBlock) throws IOException {
    if (filledBuffers.size() >= spillLimit) {
      // Do not block; possible that there are more buffers
      scheduleSpill(shouldBlock);
    }
  }

  private boolean scheduleSpill(boolean block) {
    if (filledBuffers.isEmpty()) {
      return false;
    }

    try {
      if (block) {
        availableSlots.acquire();
      } else {
        if (!availableSlots.tryAcquire()) {
          // Data in filledBuffers would be spilled in subsequent iteration.
          return false;
        }
      }

      final int filledBufferCount = filledBuffers.size();
      if (LOG.isDebugEnabled() || (filledBufferCount % 10) == 0) {
        LOG.info("{}: triggering spill. filledBuffers.size={}", destNameTrimmed, filledBufferCount);
      }
      pendingSpillCount.incrementAndGet();
      int spillNumber = numSpills.getAndIncrement();

      ListenableFuture<SpillResult> future = spillExecutor.submit(new SpillCallable(
          new ArrayList<WrappedBuffer>(filledBuffers), codec, spilledRecordsCounter, spillNumber));
      filledBuffers.clear();
      Futures.addCallback(future, new SpillCallback(spillNumber));
      // Update once per buffer (instead of every record)
      updateTezCountersAndNotify();
      return true;
    } catch(InterruptedException ie) {
      Thread.currentThread().interrupt(); // reset interrupt status
    }
    return false;
  }

  private boolean reportPartitionStats() {
    return (sizePerPartition != null);
  }

  private void updateGlobalStats(WrappedBuffer buffer) {
    for (int i = 0; i < numPartitions; i++) {
      numRecordsPerPartition[i] += buffer.recordsPerPartition[i];
      if (reportPartitionStats()) {
        sizePerPartition[i] += buffer.sizePerPartition[i];
      }
    }
  }

  private WrappedBuffer getNextAvailableBuffer() throws IOException {
    if (availableBuffers.peek() == null) {
      if (numInitializedBuffers < numBuffers) {
        buffers[numInitializedBuffers] = new WrappedBuffer(numPartitions,
            numInitializedBuffers == numBuffers - 1 ? lastBufferSize : sizePerBuffer);
        numInitializedBuffers++;
        return buffers[numInitializedBuffers - 1];
      } else {
        // All buffers initialized, and none available right now. Wait
        try {
          // Ensure that spills are triggered so that buffers can be released.
          mayBeSpill(true);
          return availableBuffers.take();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOInterruptedException("Interrupted while waiting for next buffer", e);
        }
      }
    } else {
      return availableBuffers.poll();
    }
  }

  // All spills using compression for now.
  private class SpillCallable implements Callable<SpillResult> {

    private final List<WrappedBuffer> filledBuffers;
    private final CompressionCodec codec;
    private final TezCounter numRecordsCounter;
    private SpillPathDetails spillPathDetails;
    private final int spillNumber;

    public SpillCallable(List<WrappedBuffer> filledBuffers, CompressionCodec codec,
        TezCounter numRecordsCounter, SpillPathDetails spillPathDetails) {
      this(filledBuffers, codec, numRecordsCounter, spillPathDetails.spillIndex);
      Preconditions.checkArgument(spillPathDetails.outputFilePath != null, "Spill output file "
          + "path can not be null");
      this.spillPathDetails = spillPathDetails;
    }

    public SpillCallable(List<WrappedBuffer> filledBuffers, CompressionCodec codec,
        TezCounter numRecordsCounter, int spillNumber) {
      this.filledBuffers = filledBuffers;
      this.codec = codec;
      this.numRecordsCounter = numRecordsCounter;
      this.spillNumber = spillNumber;
    }

    @Override
    public SpillResult call() throws IOException {
      // This should not be called with an empty buffer. Check before invoking.

      // Number of parallel spills determined by number of threads.
      // Last spill synchronization handled separately.
      SpillResult spillResult = null;
      if (spillPathDetails == null) {
        this.spillPathDetails = getSpillPathDetails(false, -1, spillNumber);
      }

      MultiByteArrayOutputStream byteArrayOutput = null;
      boolean canUseBuffers = false;
      if (useFreeMemoryWriterOutput) {
        canUseBuffers = MultiByteArrayOutputStream.canUseFreeMemoryBuffers(freeMemoryThreshold);
        if (canUseBuffers) {
          byteArrayOutput = new MultiByteArrayOutputStream(rfs, spillPathDetails.outputFilePath);
        }
      }

      FSDataOutputStream fsOutput = null;
      long compressedLength = 0;
      TezSpillRecord spillRecord = new TezSpillRecord(numPartitions);
      try {
        if (byteArrayOutput == null) {
          fsOutput = rfs.create(spillPathDetails.outputFilePath);
        } else {
          fsOutput = new FSDataOutputStream(byteArrayOutput, null);
        }
        ensureSpillFilePermissions(spillPathDetails.outputFilePath, rfs);

        LOG.info("Writing spill {} to {} (use in-memory buffers = {})",
            spillNumber, spillPathDetails.outputFilePath.toString(), canUseBuffers);

        DataInputBuffer key = new DataInputBuffer();
        DataInputBuffer val = new DataInputBuffer();
        byte[] writeBuffer = IFile.allocateWriteBuffer();
        for (int i = 0; i < numPartitions; i++) {
          IFile.Writer writer = null;
          try {
            long segmentStart = fsOutput.getPos();
            long numRecords = 0;
            for (WrappedBuffer buffer : filledBuffers) {
              if (buffer.partitionHeads[i] == WrappedBuffer.PARTITION_ABSENT_POSITION) {
                // Skip empty partition.
                continue;
              }
              if (writer == null) {
                // all Writer instances share the same FSDataOutputStream out
                writer = new Writer(
                    keySerialization, valSerialization, fsOutput,
                    keyClass, valClass, codec, null, null, false,
                    writeBuffer);
              }
              numRecords += writePartition(buffer.partitionHeads[i], buffer, writer, key, val);
            }
            if (writer != null) {
              if (numRecordsCounter != null) {
                // TezCounter is not thread-safe; Since numRecordsCounter would be updated from
                // multiple threads, it is good to synchronize it when incrementing it for correctness.
                synchronized (numRecordsCounter) {
                  numRecordsCounter.increment(numRecords);
                }
              }
              writer.close();   // write does not own fsOutput, so fsOutput.close() is not called
              compressedLength += writer.getCompressedLength();
              TezIndexRecord indexRecord = new TezIndexRecord(segmentStart, writer.getRawLength(),
                  writer.getCompressedLength());
              spillRecord.putIndex(indexRecord, i);
              writer = null;
            }
          } finally {
            if (writer != null) {
              writer.close();
            }
          }
        }
        key.close();
        val.close();
      } finally {
        if (fsOutput != null) {
          fsOutput.close();
        }
      }

      spillResult = new SpillResult(compressedLength, this.filledBuffers);

      // spillPathDetails.spillIndex can be -1 if spillIndex was not used in pathComponent
      handleSpillIndex(spillPathDetails, spillRecord, byteArrayOutput);
      LOG.info("{}: Finished spill {}", destNameTrimmed, spillPathDetails.spillIndex);

      return spillResult;
    }
  }

  private long writePartition(int pos, WrappedBuffer wrappedBuffer, Writer writer,
      DataInputBuffer keyBuffer, DataInputBuffer valBuffer) throws IOException {
    long numRecords = 0;
    while (pos != WrappedBuffer.PARTITION_ABSENT_POSITION) {
      int metaIndex = pos / INT_SIZE;
      int keyLength = wrappedBuffer.metaBuffer.get(metaIndex + INDEX_KEYLEN);
      int valLength = wrappedBuffer.metaBuffer.get(metaIndex + INDEX_VALLEN);
      keyBuffer.reset(wrappedBuffer.buffer, pos + META_SIZE, keyLength);
      valBuffer.reset(wrappedBuffer.buffer, pos + META_SIZE + keyLength, valLength);

      writer.append(keyBuffer, valBuffer);
      numRecords++;
      pos = wrappedBuffer.metaBuffer.get(metaIndex + INDEX_NEXT);
    }
    return numRecords;
  }

  public static long getInitialMemoryRequirement(Configuration conf, long maxAvailableTaskMemory) {
    long initialMemRequestMb = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB,
        TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB_DEFAULT);
    Preconditions.checkArgument(initialMemRequestMb != 0,
        TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB + " should be larger than 0");
    long reqBytes = initialMemRequestMb << 20;
    if (LOG.isDebugEnabled()) { LOG.debug("Requested BufferSize ({}): {}", TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB, initialMemRequestMb); }
    return reqBytes;
  }

  private boolean canSendDataOverDME() throws IOException {
    if (this.useCachedStream   // == dataViaEventsEnabled && (numPartitions == 1) && !pipelinedShuffle
        && this.finalOutPath == null) {

      // It is possible that in-mem writer spilled over to disk. Need to use
      // that path as finalOutPath and set its permission.

      if (((IFile.FileBackedInMemIFileWriter) writer).isDataFlushedToDisk()) {
        this.finalOutPath = ((IFile.FileBackedInMemIFileWriter) writer).getOutputPath();
        ensureSpillFilePermissions(finalOutPath, rfs);
        additionalSpillBytesWritternCounter.increment(writer.getCompressedLength());
      }
    }

    return (writer != null) && dataViaEventsEnabled
            && (writer.getCompressedLength() <= dataViaEventsMaxSize);
  }

  private ByteBuffer readDataForDME() throws IOException {
    if (this.useCachedStream
        && !((IFile.FileBackedInMemIFileWriter) writer).isDataFlushedToDisk()) {
      return ((IFile.FileBackedInMemIFileWriter) writer).getData();
    } else {
      try (FSDataInputStream inStream = rfs.open(finalOutPath)) {
        byte[] buf = new byte[(int) writer.getCompressedLength()];
        IOUtils.readFully(inStream, buf, 0, (int) writer.getCompressedLength());
        additionalSpillBytesReadCounter.increment(writer.getCompressedLength());
        return ByteBuffer.wrap(buf);
      }
    }
  }

  @Override
  public List<Event> close() throws IOException, InterruptedException {
    // In case there are buffers to be spilled, schedule spilling
    scheduleSpill(true);
    List<Event> eventList = Lists.newLinkedList();
    isShutdown.set(true);
    spillLock.lock();
    try {
      if (pendingSpillCount.get() != 0) {
        LOG.info("{}: Waiting for all spills to complete : Pending : {}", destNameTrimmed, pendingSpillCount.get());
      }
      while (pendingSpillCount.get() != 0 && spillException == null) {
        spillInProgress.await();
      }
    } finally {
      spillLock.unlock();
    }
    if (spillException != null) {
      LOG.error(destNameTrimmed + ": Error during spill, throwing");
      // Assuming close will be called on the same thread as the write
      cleanup();
      cleanupCurrentBuffer();
      if (spillException instanceof IOException) {
        throw (IOException) spillException;
      } else {
        throw new IOException(spillException);
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.info(destNameTrimmed + ": All spills complete");
      }
      // Assuming close will be called on the same thread as the write
      cleanup();

      List<Event> events = Lists.newLinkedList();
      if (!pipelinedShuffle) {
        if (skipBuffers) {  // numPartitions == 1 && !pipelinedShuffle, and written directly to writer
          writer.close();
          long rawLen = writer.getRawLength();
          long compLen = writer.getCompressedLength();

          BitSet emptyPartitions = new BitSet();
          if (outputRecordsCounter.getValue() == 0) {
            emptyPartitions.set(0);
          }
          if (reportPartitionStats()) {
            if (outputRecordsCounter.getValue() > 0) {
              sizePerPartition[0] = rawLen;
            }
          }
          // no need to call cleanupCurrentBuffer() because skipBuffers == true

          if (outputRecordsCounter.getValue() > 0) {
            outputBytesWithOverheadCounter.increment(rawLen);
            fileOutputBytesCounter.increment(compLen + indexFileSizeEstimate);
          }
          eventList.add(generateVMEvent());

          if (!canSendDataOverDME()) {
            TezIndexRecord rec = new TezIndexRecord(0, rawLen, compLen);
            TezSpillRecord sr = new TezSpillRecord(1);
            sr.putIndex(rec, 0);
            if (writeSpillRecord) {
              finalIndexPath = outputFileHandler.getOutputIndexFileForWrite(indexFileSizeEstimate);
              sr.writeToFile(finalIndexPath, localFs);
            } else {
              ShuffleUtils.writeToIndexPathCacheAndByteCache(outputContext, finalOutPath, sr, null);
            }
          }
          eventList.add(generateDMEvent(false, -1, false,
              outputContext.getUniqueIdentifier(), emptyPartitions));

          return eventList;
        }

        /*
          1. Final merge enabled
             - When lots of spills are there, mergeAll, generate events and return
             - If there are no existing spills, check for final spill and generate events
          2. Final merge disabled
             - If finalSpill generated data, generate events and return
             - If finalSpill did not generate data, it would automatically populate events
         */
        if (isFinalMergeEnabled) {
          if (numSpills.get() > 0) {
            mergeAll();
          } else {
            finalSpill();
          }
          updateTezCountersAndNotify();
          eventList.add(generateVMEvent());
          eventList.add(generateDMEvent());
        } else {
          // if no data is generated, finalSpill would create VMEvent & add to finalEvents
          SpillResult result = finalSpill();
          if (result != null) {
            updateTezCountersAndNotify();
            // Generate vm event
            finalEvents.add(generateVMEvent());

            // compute empty partitions based on spill result and generate DME
            int spillNum = numSpills.get() - 1;
            SpillCallback callback = new SpillCallback(spillNum);
            callback.computePartitionStats(result);
            BitSet emptyPartitions = getEmptyPartitions(callback.getRecordsPerPartition());
            String pathComponent = generatePathComponent(outputContext.getUniqueIdentifier(), spillNum);
            Event finalEvent = generateDMEvent(true, spillNum,
                true, pathComponent, emptyPartitions);
            finalEvents.add(finalEvent);
          }
          //all events to be sent out are in finalEvents.
          eventList.addAll(finalEvents);
        }
        cleanupCurrentBuffer();   // skipBuffers == false
        return eventList;
      }

      // Update Counters before call finalSpill() because it may send VME when pipelined shuffle ie enabled.
      updateTezCountersAndNotify();

      // For pipelined case, send out an event in case finalspill generated a spill file.
      if (finalSpill() != null) {
        // VertexManagerEvent is only sent at the end and thus sizePerPartition is used
        // for the sum of all spills.
        mayBeSendEventsForSpill(currentBuffer.recordsPerPartition,
            sizePerPartition, numSpills.get() - 1, true);
      }
      cleanupCurrentBuffer();
      return events;
    }
  }

  private BitSet getEmptyPartitions(int[] recordsPerPartition) {
    Preconditions.checkArgument(recordsPerPartition != null, "records per partition can not be null");
    BitSet emptyPartitions = new BitSet();
    for (int i = 0; i < numPartitions; i++) {
      if (recordsPerPartition[i] == 0 ) {
        emptyPartitions.set(i);
      }
    }
    return emptyPartitions;
  }

  public boolean reportDetailedPartitionStats() {
    return reportPartitionStats.isPrecise();
  }

  private Event generateVMEvent() throws IOException {
    return ShuffleUtils.generateVMEvent(outputContext, this.sizePerPartition,
        this.reportDetailedPartitionStats(), deflater.get());
  }

  private Event generateDMEvent() throws IOException {
    BitSet emptyPartitions = getEmptyPartitions(numRecordsPerPartition);
    return generateDMEvent(false, -1, false, outputContext.getUniqueIdentifier(), emptyPartitions);
  }

  private Event generateDMEvent(boolean addSpillDetails, int spillId,
      boolean isLastSpill, String pathComponent, BitSet emptyPartitions)
      throws IOException {

    DataMovementEventPayloadProto.Builder payloadBuilder = DataMovementEventPayloadProto
        .newBuilder();
    if (numPartitions == 1) {
      payloadBuilder.setNumRecord((int) outputRecordsCounter.getValue());
    }

    String host = getHost();
    if (emptyPartitions.cardinality() != 0) {
      // Empty partitions exist
      ByteString emptyPartitionsByteString =
          TezCommonUtils.compressByteArrayToByteString(TezUtilsInternal.toByteArray
              (emptyPartitions), deflater.get());
      payloadBuilder.setEmptyPartitions(emptyPartitionsByteString);
    }

    if (emptyPartitions.cardinality() != numPartitions) {
      // Populate payload only if at least 1 partition has data
      payloadBuilder.setHost(host);

      int[] shufflePorts = getShufflePort();
      payloadBuilder.setNumPorts(shufflePorts.length);
      for (int i = 0; i < shufflePorts.length; i++) {
        payloadBuilder.addPorts(shufflePorts[i]);
      }

      payloadBuilder.setPathComponent(ShuffleUtils.expandPathComponent(outputContext, compositeFetch, pathComponent));
    }

    if (addSpillDetails) {
      payloadBuilder.setSpillId(spillId);
      payloadBuilder.setLastEvent(isLastSpill);
    }

    if (canSendDataOverDME()) {
      ShuffleUserPayloads.DataProto.Builder dataProtoBuilder = ShuffleUserPayloads.DataProto.newBuilder();
      dataProtoBuilder.setData(UnsafeByteOperations.unsafeWrap(readDataForDME()));
      dataProtoBuilder.setRawLength((int)this.writer.getRawLength());

      dataProtoBuilder.setCompressedLength((int)this.writer.getCompressedLength());
      payloadBuilder.setData(dataProtoBuilder.build());

      this.dataViaEventSize.increment(this.writer.getCompressedLength());
      if (LOG.isDebugEnabled()) {
        LOG.debug("payload packed in DME, dataSize: " + this.writer.getCompressedLength());
      }
    }

    ByteBuffer payload = payloadBuilder.build().toByteString().asReadOnlyByteBuffer();
    return CompositeDataMovementEvent.create(0, numPartitions, payload);
  }

  private void cleanupCurrentBuffer() {
    if (!skipBuffers) {
      currentBuffer.cleanup();
      currentBuffer = null;
    }
  }

  private void cleanup() {
    if (spillExecutor != null) {
      spillExecutor.shutdownNow();
    }
    if (!skipBuffers) {
      for (int i = 0; i < buffers.length; i++) {
        if (buffers[i] != null && buffers[i] != currentBuffer) {
          buffers[i].cleanup();
          buffers[i] = null;
        }
      }
      availableBuffers.clear();
    }
  }

  // inside close()
  private SpillResult finalSpill() throws IOException {
    if (currentBuffer.nextPosition == 0) {
      if (pipelinedShuffle || !isFinalMergeEnabled) {
        List<Event> eventList = Lists.newLinkedList();
        eventList.add(ShuffleUtils.generateVMEvent(outputContext,
            reportPartitionStats() ? new long[numPartitions] : null,
            reportDetailedPartitionStats(), deflater.get()));
        if (localOutputRecordsCounter == 0 && outputLargeRecordsCounter.getValue() == 0) {
          // Should send this event (all empty partitions) only when no records are written out.
          BitSet emptyPartitions = new BitSet(numPartitions);
          emptyPartitions.flip(0, numPartitions);
          eventList.add(generateDMEvent(true, numSpills.get(), true,
              null, emptyPartitions));
        }
        if (pipelinedShuffle) {
          outputContext.sendEvents(eventList);
        } else if (!isFinalMergeEnabled) {
          finalEvents.addAll(0, eventList);
        }
      }
      return null;
    } else {
      updateGlobalStats(currentBuffer);
      filledBuffers.add(currentBuffer);

      //setup output file and index file
      SpillPathDetails spillPathDetails = getSpillPathDetails(true, -1);
      SpillCallable spillCallable = new SpillCallable(filledBuffers, codec, null, spillPathDetails);
      try {
        SpillResult spillResult = spillCallable.call();

        fileOutputBytesCounter.increment(spillResult.spillSize);
        fileOutputBytesCounter.increment(indexFileSizeEstimate);
        return spillResult;
      } catch (Exception ex) {
        throw (ex instanceof IOException) ? (IOException)ex : new IOException(ex);
      }
    }

  }

  /**
   * Set up spill output file, index file details.
   *
   * @param isFinalSpill
   * @param expectedSpillSize
   * @return SpillPathDetails
   * @throws IOException
   */
  private SpillPathDetails getSpillPathDetails(boolean isFinalSpill, long expectedSpillSize)
      throws IOException {
    int spillNumber = numSpills.getAndIncrement();
    return getSpillPathDetails(isFinalSpill, expectedSpillSize, spillNumber);
  }

  /**
   * Set up spill output file, index file details.
   *
   * @param isFinalSpill
   * @param expectedSpillSize
   * @param spillNumber
   * @return SpillPathDetails
   * @throws IOException
   */
  private SpillPathDetails getSpillPathDetails(boolean isFinalSpill, long expectedSpillSize,
      int spillNumber) throws IOException {
    long spillSize = (expectedSpillSize < 0) ?
        (currentBuffer.nextPosition + numPartitions * APPROX_HEADER_LENGTH) : expectedSpillSize;

    Path outputFilePath = null;
    Path indexFilePath = null;

    int finalSpillIndex;
    boolean indexComputed = false;   // true if TezSpillRecord is effectively computed (i.e., indexFilePath set)
    if (!pipelinedShuffle && isFinalMergeEnabled) {
      if (isFinalSpill) {
        outputFilePath = outputFileHandler.getOutputFileForWrite(spillSize);
        finalOutPath = outputFilePath;
        if (writeSpillRecord) {
          indexFilePath = outputFileHandler.getOutputIndexFileForWrite(indexFileSizeEstimate);
          finalIndexPath = indexFilePath;
        }
        indexComputed = true;   // because indexFilePath would be set when using mapreduce_shuffle (ignoring writeSpillRecord)
        finalSpillIndex = -1;   // spill index was not used
      } else {
        outputFilePath = outputFileHandler.getSpillFileForWrite(spillNumber, spillSize);
        finalSpillIndex = spillNumber;
        // indexComputed = false && indexFilePath not set
      }
    } else {
      outputFilePath = outputFileHandler.getSpillFileForWrite(spillNumber, spillSize);
      if (writeSpillRecord) {
        indexFilePath = outputFileHandler.getSpillIndexFileForWrite(spillNumber, indexFileSizeEstimate);
      }
      indexComputed = true;   // because indexFilePath would be set when using mapreduce_shuffle (ignoring writeSpillRecord)
      finalSpillIndex = spillNumber;
    }

    return new SpillPathDetails(outputFilePath, indexFilePath, finalSpillIndex, indexComputed);
  }

  private void mergeAll() throws IOException {
    long expectedSize = spilledSize;
    if (currentBuffer.nextPosition != 0) {
      expectedSize += currentBuffer.nextPosition - (currentBuffer.numRecords * META_SIZE)
          - currentBuffer.skipSize + numPartitions * APPROX_HEADER_LENGTH;
      // Update final statistics.
      updateGlobalStats(currentBuffer);
    }

    SpillPathDetails spillPathDetails = getSpillPathDetails(true, expectedSize);
    // if !writeSpillRecord, then spillPathDetails.indexFilePath == null
    if (writeSpillRecord) {
      finalIndexPath = spillPathDetails.indexFilePath;
    }
    finalOutPath = spillPathDetails.outputFilePath;

    TezSpillRecord finalSpillRecord = new TezSpillRecord(numPartitions);

    DataInputBuffer keyBuffer = new DataInputBuffer();
    DataInputBuffer valBuffer = new DataInputBuffer();

    DataInputBuffer keyBufferIFile = new DataInputBuffer();
    DataInputBuffer valBufferIFile = new DataInputBuffer();

    FSDataOutputStream out = null;
    try {
      out = rfs.create(finalOutPath);
      ensureSpillFilePermissions(finalOutPath, rfs);
      Writer writer = null;

      byte[] writeBuffer = IFile.allocateWriteBuffer();
      for (int i = 0; i < numPartitions; i++) {
        long segmentStart = out.getPos();
        if (numRecordsPerPartition[i] == 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(destNameTrimmed + ": " + "Skipping partition: " + i + " in final merge since it has no records");
          }
          continue;
        }
        // inside close()
        writer = new Writer(
            keySerialization, valSerialization, out, keyClass, valClass,
            codec, null, null, false,
            writeBuffer );
        try {
          if (currentBuffer.nextPosition != 0
              && currentBuffer.partitionHeads[i] != WrappedBuffer.PARTITION_ABSENT_POSITION) {
            // Write current buffer.
            writePartition(currentBuffer.partitionHeads[i], currentBuffer, writer, keyBuffer,
                valBuffer);
          }
          synchronized (spillInfoList) {
            for (SpillInfo spillInfo : spillInfoList) {
              TezIndexRecord indexRecord = spillInfo.spillRecord.getIndex(i);
              if (indexRecord.getPartLength() == 0) {
                // Skip empty partitions within a spill
                continue;
              }
              FSDataInputStream in = rfs.open(spillInfo.outPath);
              in.seek(indexRecord.getStartOffset());
              IFile.Reader reader = new IFile.Reader(in, indexRecord.getPartLength(), codec, null,
                  additionalSpillBytesReadCounter, ifileReadAhead, ifileReadAheadLength,
                  outputContext);
              // reader.close() may not be called if the following while{} block throws IOException.
              // In this case, reader.decompressor is not returned to the pool.
              // However, this is not memory leak because reader is eventually garbage collected, at which point
              // reader.decompressor is also garbage collected. It is just that reader.decompressor is not reused.
              // Note that reader.close() itself may throw IOException and reader.decompressor may not be returned to the pool.
              // For the same reason, this not memory leak because reader.decompressor is eventually garbage collected.
              while (reader.nextRawKey(keyBufferIFile)) {
                // TODO Inefficient. If spills are not compressed, a direct copy should be possible
                // given the current IFile format. Also exteremely inefficient for large records,
                // since the entire record will be read into memory.
                reader.nextRawValue(valBufferIFile);
                writer.append(keyBufferIFile, valBufferIFile);
              }
              reader.close();
            }
          }
          writer.close();
          fileOutputBytesCounter.increment(writer.getCompressedLength());
          TezIndexRecord indexRecord = new TezIndexRecord(segmentStart, writer.getRawLength(),
              writer.getCompressedLength());
          writer = null;
          finalSpillRecord.putIndex(indexRecord, i);
        } finally {
          if (writer != null) {
            writer.close();
          }
        }
      }
    } finally {
      if (out != null) {
        out.close();
        // always call deleteIntermediateSpills() because it does not affect VertexRerun and fault-tolerance
        deleteIntermediateSpills();
      }
    }

    if (writeSpillRecord) {
      finalSpillRecord.writeToFile(finalIndexPath, localFs);
    } else {
      ShuffleUtils.writeToIndexPathCacheAndByteCache(outputContext, finalOutPath, finalSpillRecord, null);
    }
    fileOutputBytesCounter.increment(indexFileSizeEstimate);
    LOG.info("{}: Finished final spill after merging: {} spills", destNameTrimmed, numSpills.get());
  }

  private void deleteIntermediateSpills() {
    // Delete the intermediate spill files
    ExecutorServiceUserGroupInformation executorServiceUgi = outputContext.getExecutorServiceUgi();
    ExecutorService executorService = executorServiceUgi.getExecutorService();
    UserGroupInformation taskUgi = executorServiceUgi.getUgi();
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          taskUgi.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() {
              synchronized (spillInfoList) {
                for (SpillInfo spill : spillInfoList) {
                  try {
                    LOG.info("Deleting intermediate spill: " + spill.outPath);
                    rfs.delete(spill.outPath, false);
                  } catch (IOException e) {
                    LOG.warn("Unable to delete intermediate spill " + spill.outPath, e);
                  }
                }
              }
              return null;
            }
          });
        } catch (IOException e) {
          LOG.warn("Error while deleting intermediate spills", e);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while deleting intermediate spills", e);
        }
      }
    });
  }

  private void writeLargeRecord(final Object key, final Object value, final int partition)
      throws IOException {
    numAdditionalSpillsCounter.increment(1);
    long size = sizePerBuffer - (currentBuffer.numRecords * META_SIZE) - currentBuffer.skipSize
        + numPartitions * APPROX_HEADER_LENGTH;
    SpillPathDetails spillPathDetails = getSpillPathDetails(false, size);
    int spillIndex = spillPathDetails.spillIndex;   // valid spillIndex and never -1

    FSDataOutputStream out = null;
    long outSize = 0;
    try {
      final TezSpillRecord spillRecord = new TezSpillRecord(numPartitions);
      final Path outPath = spillPathDetails.outputFilePath;
      out = rfs.create(outPath);
      ensureSpillFilePermissions(outPath, rfs);
      BitSet emptyPartitions = null;
      if (pipelinedShuffle || !isFinalMergeEnabled) {
        emptyPartitions = new BitSet(numPartitions);
      }
      for (int i = 0; i < numPartitions; i++) {
        final long recordStart = out.getPos();
        if (i == partition) {
          spilledRecordsCounter.increment(1);
          Writer writer = null;
          try {
            writer = new IFile.Writer(keySerialization, valSerialization, out, keyClass, valClass, codec, null, null, false,
                IFile.allocateWriteBufferSingle());
            writer.append(key, value);
            outputLargeRecordsCounter.increment(1);
            numRecordsPerPartition[i]++;
            if (reportPartitionStats()) {
              sizePerPartition[i] += writer.getRawLength();
            }
            writer.close();
            synchronized (additionalSpillBytesWritternCounter) {
              additionalSpillBytesWritternCounter.increment(writer.getCompressedLength());
            }
            TezIndexRecord indexRecord = new TezIndexRecord(recordStart, writer.getRawLength(),
                writer.getCompressedLength());
            spillRecord.putIndex(indexRecord, i);
            outSize = writer.getCompressedLength();
            writer = null;
          } finally {
            if (writer != null) {
              writer.close();
            }
          }
        } else {
          if (emptyPartitions != null) {
            emptyPartitions.set(i);
          }
        }
      }

      // spillPathDetails.spillIndex is never -1
      handleSpillIndex(spillPathDetails, spillRecord, null);

      mayBeSendEventsForSpill(emptyPartitions, sizePerPartition, spillIndex, false);

      LOG.info("{}: Finished writing large record of size {} to spill file {}", destNameTrimmed, outSize, spillIndex);
      if (LOG.isDebugEnabled()) {
        LOG.debug(destNameTrimmed + ": " + "LargeRecord Spill=" + spillIndex + ", indexPath="
            + spillPathDetails.indexFilePath + ", outputPath="
            + spillPathDetails.outputFilePath);
      }
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  private void handleSpillIndex(
      SpillPathDetails spillPathDetails, TezSpillRecord spillRecord,
      @Nullable MultiByteArrayOutputStream byteArrayOutput) throws IOException {
    if (spillPathDetails.indexComputed) {
      if (spillPathDetails.indexFilePath != null) {
        // write the index record
        assert writeSpillRecord;
        spillRecord.writeToFile(spillPathDetails.indexFilePath, localFs);
      } else {
        // only one of outputFilePath and byteArrayOutput is non-null
        Path outputFilePath = byteArrayOutput == null ? spillPathDetails.outputFilePath : null;

        // must check if spillPathDetails.spillIndex == -1
        if (spillPathDetails.spillIndex < 0) {
          ShuffleUtils.writeToIndexPathCacheAndByteCache(outputContext,
              outputFilePath, spillRecord, byteArrayOutput);
        } else {
          ShuffleUtils.writeSpillInfoToIndexPathCacheAndByteCache(outputContext,
              spillPathDetails.spillIndex, outputFilePath, spillRecord, byteArrayOutput);
        }
      }
    } else {
      // add to cache
      SpillInfo spillInfo = new SpillInfo(spillRecord, spillPathDetails.outputFilePath);
      spillInfoList.add(spillInfo);
      numAdditionalSpillsCounter.increment(1);
    }
  }

  private class ByteArrayOutputStream extends OutputStream {

    private final byte[] scratch = new byte[1];

    @Override
    public void write(int v) throws IOException {
      scratch[0] = (byte) v;
      write(scratch, 0, 1);
    }

    public void write(byte[] b, int off, int len) {
      if (currentBuffer.full) {
          /* no longer do anything until reset */
      } else if (len > currentBuffer.availableSize) {
        currentBuffer.full = true; /* stop working & signal we hit the end */
      } else {
        System.arraycopy(b, off, currentBuffer.buffer, currentBuffer.nextPosition, len);
        currentBuffer.nextPosition += len;
        currentBuffer.availableSize -= len;
      }
    }
  }

  private static class WrappedBuffer {

    private static final int PARTITION_ABSENT_POSITION = -1;

    // FIFO pointers for each partition
    private final int[] partitionHeads;   // Points to the first record of a partition
    private final int[] partitionTails;   // Points to the last record of a partition
    private final int[] recordsPerPartition;
    // uncompressed size for each partition
    private final long[] sizePerPartition;

    private final int size;
    private byte[] buffer;
    private IntBuffer metaBuffer;
    private int availableSize;

    private int numRecords = 0;
    private int skipSize = 0;
    private int nextPosition = 0;
    private boolean full = false;

    WrappedBuffer(int numPartitions, int size) {
      this.partitionHeads = new int[numPartitions];
      this.partitionTails = new int[numPartitions];
      this.recordsPerPartition = new int[numPartitions];
      this.sizePerPartition = new long[numPartitions];
      Arrays.fill(this.partitionHeads, PARTITION_ABSENT_POSITION);
      Arrays.fill(this.partitionTails, PARTITION_ABSENT_POSITION);
      Arrays.fill(this.recordsPerPartition, 0);
      Arrays.fill(this.sizePerPartition, 0L);

      size = size - (size % INT_SIZE);
      this.size = size;
      this.buffer = new byte[size];
      this.metaBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.nativeOrder()).asIntBuffer();
      this.availableSize = size;
    }

    void reset() {
      Arrays.fill(partitionHeads, PARTITION_ABSENT_POSITION);
      Arrays.fill(partitionTails, PARTITION_ABSENT_POSITION);
      Arrays.fill(recordsPerPartition, 0);
      Arrays.fill(sizePerPartition, 0L);
      numRecords = 0;
      nextPosition = 0;
      skipSize = 0;
      availableSize = size;
      full = false;
    }

    void cleanup() {
      buffer = null;
      metaBuffer = null;
    }
  }

  private String generatePathComponent(String uniqueId, int spillNumber) {
    return (uniqueId + "_" + spillNumber);
  }

  private List<Event> generateEventForSpill(BitSet emptyPartitions, long[] sizePerPartition,
      int spillNumber,
      boolean isFinalUpdate) throws IOException {
    List<Event> eventList = Lists.newLinkedList();
    //Send out an event for consuming.
    String pathComponent = generatePathComponent(outputContext.getUniqueIdentifier(), spillNumber);
    if (isFinalUpdate) {
      eventList.add(ShuffleUtils.generateVMEvent(outputContext,
          sizePerPartition, reportDetailedPartitionStats(), deflater.get()));
    }
    Event compEvent = generateDMEvent(true, spillNumber, isFinalUpdate,
        pathComponent, emptyPartitions);
    eventList.add(compEvent);
    return eventList;
  }

  private void mayBeSendEventsForSpill(
      BitSet emptyPartitions, long[] sizePerPartition,
      int spillNumber, boolean isFinalUpdate) {
    if (!pipelinedShuffle) {
      if (isFinalMergeEnabled) {
        return;
      }
    }
    List<Event> events = null;
    try {
      events = generateEventForSpill(emptyPartitions, sizePerPartition, spillNumber,
          isFinalUpdate);
      LOG.info("{}: Adding spill event for spill (final update={}), spillId={}",
          destNameTrimmed, isFinalUpdate, spillNumber);
      if (pipelinedShuffle) {
        //Send out an event for consuming.
        outputContext.sendEvents(events);
      } else if (!isFinalMergeEnabled) {
        this.finalEvents.addAll(events);
      }
    } catch (IOException e) {
      LOG.error(destNameTrimmed + ": Error in sending pipelined events", e);
      outputContext.reportFailure(TaskFailureType.NON_FATAL, e,
          "Error in sending events.");
    }
  }

  private void mayBeSendEventsForSpill(int[] recordsPerPartition,
      long[] sizePerPartition, int spillNumber, boolean isFinalUpdate) {
    BitSet emptyPartitions = getEmptyPartitions(recordsPerPartition);
    mayBeSendEventsForSpill(emptyPartitions, sizePerPartition, spillNumber,
        isFinalUpdate);
  }

  private class SpillCallback implements FutureCallback<SpillResult> {

    private final int spillNumber;
    private int recordsPerPartition[];
    private long sizePerPartition[];

    SpillCallback(int spillNumber) {
      this.spillNumber = spillNumber;
    }

    void computePartitionStats(SpillResult result) {
      if (result.filledBuffers.size() == 1) {
        recordsPerPartition = result.filledBuffers.get(0).recordsPerPartition;
        sizePerPartition = result.filledBuffers.get(0).sizePerPartition;
      } else {
        recordsPerPartition = new int[numPartitions];
        sizePerPartition = new long[numPartitions];
        for (WrappedBuffer buffer : result.filledBuffers) {
          for (int i = 0; i < numPartitions; ++i) {
            recordsPerPartition[i] += buffer.recordsPerPartition[i];
            sizePerPartition[i] += buffer.sizePerPartition[i];
          }
        }
      }
    }

    int[] getRecordsPerPartition() {
      return recordsPerPartition;
    }

    @Override
    public void onSuccess(SpillResult result) {
      synchronized (UnorderedPartitionedKVWriter.this) {
        spilledSize += result.spillSize;
      }

      computePartitionStats(result);

      mayBeSendEventsForSpill(recordsPerPartition, sizePerPartition, spillNumber, false);

      try {
        for (WrappedBuffer buffer : result.filledBuffers) {
          buffer.reset();
          availableBuffers.add(buffer);
        }
      } catch (Throwable e) {
        LOG.error(destNameTrimmed + ": Failure while attempting to reset buffer after spill", e);
        outputContext.reportFailure(TaskFailureType.NON_FATAL, e, "Failure while attempting to reset buffer after spill");
      }

      if (!pipelinedShuffle && isFinalMergeEnabled) {
        synchronized(additionalSpillBytesWritternCounter) {
          additionalSpillBytesWritternCounter.increment(result.spillSize);
        }
      } else {
        synchronized(fileOutputBytesCounter) {
          fileOutputBytesCounter.increment(indexFileSizeEstimate);
          fileOutputBytesCounter.increment(result.spillSize);
        }
      }

      spillLock.lock();
      try {
        if (pendingSpillCount.decrementAndGet() == 0) {
          spillInProgress.signal();
        }
      } finally {
        spillLock.unlock();
        availableSlots.release();
      }
    }

    @Override
    public void onFailure(Throwable t) {
      // spillException setup to throw an exception back to the user. Requires synchronization.
      // Consider removing it in favor of having Tez kill the task
      LOG.error(destNameTrimmed + ": Failure while spilling to disk", t);
      spillException = t;
      outputContext.reportFailure(TaskFailureType.NON_FATAL, t, "Failure while spilling to disk");
      spillLock.lock();
      try {
        spillInProgress.signal();
      } finally {
        spillLock.unlock();
        availableSlots.release();
      }
    }
  }

  private static class SpillResult {
    final long spillSize;
    final List<WrappedBuffer> filledBuffers;

    SpillResult(long size, List<WrappedBuffer> filledBuffers) {
      this.spillSize = size;
      this.filledBuffers = filledBuffers;
    }
  }

  private static class SpillInfo {
    final TezSpillRecord spillRecord;
    final Path outPath;

    SpillInfo(TezSpillRecord spillRecord, Path outPath) {
      this.spillRecord = spillRecord;
      this.outPath = outPath;
    }
  }

  String getHost() {
    return outputContext.getExecutionContext().getHostName();
  }

  int[] getShufflePort() throws IOException {
    String auxiliaryService = ShuffleUtils.getTezShuffleHandlerServiceId(conf);
    ByteBuffer shuffleMetadata = outputContext.getServiceProviderMetaData(auxiliaryService);
    int[] shufflePorts = ShuffleUtils.deserializeShuffleProviderMetaData(shuffleMetadata);
    return shufflePorts;
  }

  static class SpillPathDetails {
    // null if writeSpillRecord == false
    final Path indexFilePath;
    final Path outputFilePath;

    // if true, index was computed:
    //   1) when using hadoop_shuffle, indexFilePath is set
    //   2) when using tez_shuffle, TezSpillRecord's byte[] is available in IndexPathCache
    final boolean indexComputed;

    // spillIndex < 0: spillIndex should not be used in pathComponent, e.g.:
    //   attempt_1742983838780_0226_4_08_000150_0_12618
    // spillIndex >= 0: spillIndex should be used in pathComponent, e.g.:
    //   attempt_1742983838780_0226_4_08_000150_0_12618_0
    final int spillIndex;

    SpillPathDetails(Path outputFilePath, @Nullable Path indexFilePath, int spillIndex,
                     boolean indexComputed) {
      this.outputFilePath = outputFilePath;
      this.indexFilePath = indexFilePath;
      this.spillIndex = spillIndex;
      this.indexComputed = indexComputed;
    }
  }
}
