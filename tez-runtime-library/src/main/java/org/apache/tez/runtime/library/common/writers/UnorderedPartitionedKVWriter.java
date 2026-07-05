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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.runtime.library.common.sort.impl.RawDataBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.ExecutorServiceUserGroupInformation;
import org.apache.tez.runtime.api.MultiByteArrayOutputStream;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.TezTaskOutput;
import org.apache.tez.runtime.api.TezOffsetRecord;
import org.apache.tez.runtime.library.api.KeyValuesWriterEdge;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.library.api.IOInterruptedException;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration.ReportPartitionStats;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.IFileInputStream;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.IFile.WriterBytesWritable;
import org.apache.tez.runtime.library.common.sort.impl.IFile.WriterDataInputBuffer;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.common.shuffle.ShuffleServer;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.apache.tez.runtime.library.utils.CodecUtils;
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

public class UnorderedPartitionedKVWriter extends KeyValuesWriterEdge {

  private static final Logger LOG = LoggerFactory.getLogger(UnorderedPartitionedKVWriter.class);
  private static final boolean isDebugEnabled = LOG.isDebugEnabled();

  private static final int INT_SIZE = 4;
  private static final int INDEX_KEYLEN = 0; // KeyLength index
  private static final int INDEX_VALLEN = 1; // ValLength index
  private static final int INDEX_NEXT = 2; // Next Record Index.
  private static final int PARTITIONED_META_SIZE = 3 * INT_SIZE;

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

  // Maybe setup a separate statistics class which can be shared between the
  // buffer and the main path instead of having multiple arrays.

  private final OutputContext outputContext;
  private final Configuration conf;
  private final String destNameTrimmed;
  private final int numPartitions;
  private final String auxiliaryService;
  private final boolean compositeFetch;
  private final boolean writeSpillRecord;

  private final RawLocalFileSystem localFs;
  private final boolean localFsSpillFilePerms;

  private final CompressionCodec codec;

  private final TezTaskOutput outputFileHandler;

  private final long assignedMemoryBytes;

  private final FileSystem rfs;
  private final boolean rfsSpillFilePerms;

  private final TezCounter outputRecordsCounter;
  private final TezCounter outputLargeRecordsCounter;
  private final TezCounter outputRecordBytesCounter;
  private final TezCounter outputBytesWithOverheadCounter;

  private final TezCounter fileOutputBytesCounter;
  private final TezCounter fileOutputBytesMemoryCounter;

  private final TezCounter spilledRecordsCounter;
  private final TezCounter additionalSpillBytesWrittenCounter;
  private final TezCounter additionalSpillBytesReadCounter;
  private final TezCounter numAdditionalSpillsCounter;

  private final TezCounter shuffleDataViaEventSize;

  // read this.conf
  private final boolean isPipelinedShuffle;   // isFinalMergeEnabled == !isPipelinedShuffle
  private final long freeMemoryThreshold;
  private final boolean useFreeMemoryWriterOutput;  // use availableMemory as threshold
  private final int dataViaEventsMaxSize;
  private final ReportPartitionStats reportPartitionStats;  // how partition stats should be reported

  private final boolean spillCompressed;

  private final long[] sizePerPartition;
  private final long indexFileSizeEstimate;

  private final boolean considerDataViaEvents;
  // Tracked only when Tez offset metadata is required (compositeFetch and not DME cached-stream mode).
  private final boolean trackMaxKeyValLen;
  private int maxKeyLen = -1;
  private int maxValLen = -1;

  //
  // fields initialized in: if (numPartitions == 1) {}
  //

  private final Partitioner partitioner;

  // for single partition cases - for both direct and pipelined
  private long singlePartitionSpillSizeLimit;
  @Nullable
  private MultiByteArrayOutputStream singlePartitionByteArrayOutput;
  private IFile.WriterBytesWritable writer;

  private final ByteArrayOutputStream baos;
  private final int[] numRecordsPerPartition;

  private final List<WrappedBuffer> filledBuffers;

  //
  // fields initialized if numPartitions > 1
  //

  private boolean ifileReadAhead;
  private int ifileReadAheadLength;

  // set in computeNumBuffersAndSize()
  private int numBuffers;
  private int spillLimit;
  private int sizePerBuffer;
  private int lastBufferSize;

  private BlockingQueue<WrappedBuffer> availableBuffers;
  private WrappedBuffer[] buffers;
  private int numInitializedBuffers;
  private WrappedBuffer currentBuffer;

  private List<SpillInfo> spillInfoList;
  private Semaphore availableSlots;
  private ListeningExecutorService spillExecutor;

  //
  // final fields
  //

  private enum WriterState {
    RUNNING,
    CLOSED,
    SPILL_FAILED
  }

  // Valid transitions:
  //   1. RUNNING -> CLOSED -> SPILL_FAILED (terminal)
  //   2. RUNNING -> SPILL_FAILED (terminal)
  // Lock-free reads are allowed, but transitions must be updated inside spillLock.lock().
  private volatile WriterState writerState = WriterState.RUNNING;

  private final AtomicInteger numSpills = new AtomicInteger(0);
  private final AtomicInteger pendingSpillCount = new AtomicInteger(0);
  private final ReentrantLock spillLock = new ReentrantLock();
  private final Condition spillInProgress = spillLock.newCondition();

  //
  // updated during the execution
  //

  private long localOutputRecordBytesCounter = 0;
  private long localOutputBytesWithOverheadCounter = 0;
  private long localOutputRecordsCounter = 0;
  // notify after x records
  private static final int NOTIFY_THRESHOLD = 100_000;

  // 'single' implies 'numPartitions == 1' and 'Spill' implies pipelined
  private FSDataOutputStream singlePartitionSpillOutput;
  private SpillPathDetails singlePartitionSpillPathDetails;
  private long singlePartitionSpillRecordBytes;
  private int singlePartitionSpillRecords;

  public UnorderedPartitionedKVWriter(OutputContext outputContext, Configuration conf,
      int numOutputs, long assignedMemoryBytes) throws IOException {
    this.outputContext = outputContext;
    this.conf = conf;
    this.destNameTrimmed = TezUtilsInternal.cleanVertexName(outputContext.getDestinationVertexName());
    this.numPartitions = numOutputs;
    this.auxiliaryService = ShuffleUtils.getTezShuffleHandlerServiceId(conf);
    this.compositeFetch = ShuffleUtils.isTezShuffleHandler(conf);

    this.writeSpillRecord = !this.compositeFetch;
    if (this.writeSpillRecord) {
      try {
        this.localFs = (RawLocalFileSystem) FileSystem.getLocal(conf).getRaw();
        this.localFsSpillFilePerms = TezSpillRecord.SPILL_FILE_PERMS.equals(
          TezSpillRecord.SPILL_FILE_PERMS.applyUMask(FsPermission.getUMask(this.localFs.getConf())));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      this.localFs = null;
      this.localFsSpillFilePerms = false;
    }

    try {
      Object shuffleServer = outputContext.peekShuffleServer();
      Configuration codecConf = ShuffleServer.getCodecConf(shuffleServer, conf);
      Class<? extends CompressionCodec> codecClass = ShuffleServer.getCodecClass(shuffleServer, codecConf);
      this.codec = CodecUtils.getCodec(codecConf, codecClass,
          ShuffleServer.getCodecBufferSize(shuffleServer, codecConf, codecClass));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    this.outputFileHandler = TezRuntimeUtils.instantiateTaskOutputManager(
        this.conf, outputContext, this.compositeFetch);

    Preconditions.checkArgument(assignedMemoryBytes > 0, "availableMemory should be > 0 bytes");
    // Ideally, should be significantly larger.
    this.assignedMemoryBytes = assignedMemoryBytes;

    this.rfs = FileSystem.getLocal(this.conf).getRaw();
    this.rfsSpillFilePerms = TezSpillRecord.SPILL_FILE_PERMS.equals(
      TezSpillRecord.SPILL_FILE_PERMS.applyUMask(FsPermission.getUMask(this.rfs.getConf())));

    outputRecordsCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_RECORDS);
    outputLargeRecordsCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_LARGE_RECORDS);
    outputRecordBytesCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES);
    outputBytesWithOverheadCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_WITH_OVERHEAD);

    fileOutputBytesCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_DISK);
    fileOutputBytesMemoryCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_MEMORY);

    spilledRecordsCounter = outputContext.getCounters().findCounter(TaskCounter.SPILLED_RECORDS);
    additionalSpillBytesWrittenCounter = outputContext.getCounters().findCounter(TaskCounter.SPILL_BYTES_DISK);
    additionalSpillBytesReadCounter = outputContext.getCounters().findCounter(TaskCounter.SPILL_BYTES_READ_ADDITIONAL);
    numAdditionalSpillsCounter = outputContext.getCounters().findCounter(TaskCounter.SPILL_COUNT_ADDITIONAL);

    shuffleDataViaEventSize = outputContext.getCounters().findCounter(TaskCounter.SHUFFLE_DATA_BYTES_VIA_EVENT);

    // read this.conf
    this.isPipelinedShuffle = this.conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_UNORDERED_ENABLED,
        TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED_DEFAULT);
    this.freeMemoryThreshold = 1024L * 1024L * conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_FREE_MEMORY_WRITER_OUTPUT_THRESHOLD_MB,
        TezRuntimeConfiguration.TEZ_RUNTIME_FREE_MEMORY_WRITER_OUTPUT_THRESHOLD_MB_DEFAULT);
    // useFreeMemoryWriterOutput = false if compositeFetch == false, i.e, when using mapreduce_shuffle
    this.useFreeMemoryWriterOutput = compositeFetch && conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT_DEFAULT);
    boolean dataViaEventsEnabled = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_ENABLED,
        TezRuntimeConfiguration.TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_ENABLED_DEFAULT);
    // No max cap on size (intentional)
    this.dataViaEventsMaxSize = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_MAX_SIZE,
        TezRuntimeConfiguration.TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_MAX_SIZE_DEFAULT);
    this.reportPartitionStats = ReportPartitionStats.fromString(conf.get(
        TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS,
        TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS_DEFAULT));

    if (isPipelinedShuffle) {
      this.spillCompressed = codec != null;
    } else {
      this.spillCompressed = conf.getBoolean(
          TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_NON_PIPELINED_SPILL_COMPRESS,
          TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_NON_PIPELINED_SPILL_COMPRESS_DEFAULT) && codec != null;
    }

    this.sizePerPartition = (reportPartitionStats.isEnabled()) ? new long[numPartitions] : null;
    this.indexFileSizeEstimate = (long)numPartitions * Constants.MAP_OUTPUT_INDEX_RECORD_LENGTH;

    final boolean singlePartitionDirect = compositeFetch && numPartitions == 1 && !isPipelinedShuffle;

    this.considerDataViaEvents = singlePartitionDirect && dataViaEventsEnabled;
    // trackMaxKeyValLen == compositeFetch && (!(numPartitions == 1) || isPipelinedShuffle || !dataViaEventsEnabled)
    this.trackMaxKeyValLen = compositeFetch && !considerDataViaEvents;

    // UnorderedKVOutput, UnorderedPartitionedKVOutput
    LOG.info("{} UnorderedPartitionedKVWriter for {}: assignedMemoryBytes={}, numPartitions={}, isPipelinedShuffle={}",
        outputContext.getTaskAttemptIdStr(), outputContext.getDestinationVertexName(),
        this.assignedMemoryBytes, this.numPartitions, this.isPipelinedShuffle);

    // If numPartitions == 1 + isPipelinedShuffle == true,
    // we do NOT create WrappedBuffer[] (buffers) and perform spilling in SpillCallable threads.
    //  - Pros: we can avoid redundant byte copies and directly write to spills.
    //  - Cons: closing a spill takes place in the caller's thread, not in a separate SpillCallable thread.
    // We cannot achieve both 'avoiding redundant byte copies' and 'closing in separate threads',
    // so this decision is a trade-off between memory efficiency and latency.

    if (compositeFetch && numPartitions == 1) {
      this.partitioner = null;
      // The synchronous single-partition path has no concurrent record buffers,
      // so the current spill can use the full output-memory allocation.
      this.singlePartitionSpillSizeLimit = isPipelinedShuffle ? assignedMemoryBytes : 0;

      if (!isPipelinedShuffle) {
        byte[] writeBuffer = IFile.allocateWriteBuffer();
        this.singlePartitionByteArrayOutput =
            new MultiByteArrayOutputStream(rfs, outputFileHandler,
                Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING, assignedMemoryBytes);
        FSDataOutputStream output = new FSDataOutputStream(singlePartitionByteArrayOutput, null);
        this.writer = new IFile.WriterBytesWritable(output, codec, outputRecordsCounter,
            outputRecordBytesCounter, trackMaxKeyValLen, -1, -1, writeBuffer, null, outputContext);
      } else {
        // writer is initialized lazily in openSinglePartitionPipelinedSpill() later
        this.singlePartitionByteArrayOutput = null;
        this.writer = null;
      }

      this.filledBuffers = null;
      this.baos = null;
      this.numRecordsPerPartition = null;
      return;
    }

    this.singlePartitionByteArrayOutput = null;
    this.writer = null;
    this.filledBuffers = new ArrayList<>();

    try {
      this.partitioner = TezRuntimeUtils.instantiatePartitioner(this.conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    numRecordsPerPartition = new int[numPartitions];

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

    this.baos = new ByteArrayOutputStream();
    computeNumBuffersAndSize();

    this.availableBuffers = new LinkedBlockingQueue<WrappedBuffer>();
    this.buffers = new WrappedBuffer[numBuffers];
    // Set up only the first buffer to start with.
    buffers[0] = new WrappedBuffer(numOutputs, sizePerBuffer);
    this.numInitializedBuffers = 1;
    this.currentBuffer = buffers[0];

    // TODO: use a shared ThreadPoolExecutor
    int maxThreads = Math.max(2, numBuffers/2);
    ExecutorService executor = new ThreadPoolExecutor(1, maxThreads,
        60L, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("UnorderedOutSpiller {" + outputContext.getUniqueIdentifierForOutputFiles() + "} #%d")
            .build()
    );
    // to restrict submission of more tasks than threads (e.g numBuffers > numThreads)
    // This is maxThreads - 1, to avoid race between callback thread releasing semaphore and the
    // thread calling tryAcquire.
    this.spillInfoList = Collections.synchronizedList(new ArrayList<SpillInfo>());
    this.availableSlots = new Semaphore(maxThreads - 1, true);
    this.spillExecutor = MoreExecutors.listeningDecorator(executor);

    if (isDebugEnabled) {
      LOG.debug("numBuffers=" + numBuffers +
          ", sizePerBuffer" + sizePerBuffer +
          ", considerDataViaEvents=" + considerDataViaEvents +
          ", dataViaEventsMaxSize=" + dataViaEventsMaxSize +
          ", reportPartitionStats=" + reportPartitionStats);
    }
  }

  private static final int ALLOC_OVERHEAD = 64;

  private void computeNumBuffersAndSize() {
    int bufferLimit = Integer.MAX_VALUE;

    // Non-pipelined with final merge path: keep more in-memory buffers to reduce spill frequency.
    // Pipelined path: preserve existing behavior of eager spilling with 2 buffers.
    if (!isPipelinedShuffle) {
      numBuffers = conf.getInt(
          TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_PARTITIONED_NON_PIPELINED_NUM_BUFFERS,
          TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_PARTITIONED_NON_PIPELINED_NUM_BUFFERS_DEFAULT);
      numBuffers = Math.max(numBuffers, 2);
      spillLimit = numBuffers - 1;
      if (assignedMemoryBytes / numBuffers > Integer.MAX_VALUE) {
        sizePerBuffer = Integer.MAX_VALUE;
      } else {
        sizePerBuffer = (int)(assignedMemoryBytes / numBuffers);
      }
      Preconditions.checkArgument(sizePerBuffer >= 8 * 1024 * 1024,
          "Insufficient memory for %s: sizePerBuffer=%s (< 8MB)",
          UnorderedPartitionedKVWriter.class.getSimpleName(), sizePerBuffer);
      // equal sized buffers
      lastBufferSize = sizePerBuffer;
    } else {
      numBuffers = (int)(assignedMemoryBytes / bufferLimit);
      if (numBuffers >= 2) {
        sizePerBuffer = bufferLimit - ALLOC_OVERHEAD;
        lastBufferSize = (int)(assignedMemoryBytes % bufferLimit);
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
        numBuffers = 2;   // we should have minimum of 2 buffers
        if (assignedMemoryBytes / numBuffers > Integer.MAX_VALUE) {
          sizePerBuffer = Integer.MAX_VALUE;
        } else {
          sizePerBuffer = (int)(assignedMemoryBytes / numBuffers);
        }
        lastBufferSize = sizePerBuffer;   // 2 equal sized buffers
      }
      spillLimit = 1;
    }

    // Ensure allocation size is multiple of INT_SIZE, truncate down.
    sizePerBuffer = sizePerBuffer - (sizePerBuffer % INT_SIZE);
    lastBufferSize = lastBufferSize - (lastBufferSize % INT_SIZE);
  }

  // called from the client (Hive)
  @Override
  public void closeWriter() {
    if (isDebugEnabled) {
      if (trackMaxKeyValLen) {
        LOG.debug("Closing up Unordered maxKey/ValLen for {}: maxKeyLen={}, maxValLen={}",
            destNameTrimmed, maxKeyLen, maxValLen);
      } else {
        LOG.debug("Closing up Unordered KeyValueWriterEdge for {}", destNameTrimmed);
      }
    }
  }

  @Override
  public int getNumUnorderedPartitions() {
    return compositeFetch ? numPartitions : -1;
  }

  // should match TezRuntimeUtils.instantiatePartitioner()
  public int getPartitionerType() {
    String className = conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS);
    if (HashPartitioner.class.getName().equals(className) ||
        UnorderedKVOutput.CustomPartitioner.class.getName().equals(className)) {
      return 0;   // use key hash to get partition
    } else {
      return 1;   // use value hash to get partition - ValueHashPartitioner
    }
  }

  // TODO: optimize, if this method is actually called
  @Override
  public void write(BytesWritable key, Iterable<BytesWritable> values) throws IOException {
    Iterator<BytesWritable> it = values.iterator();
    while (it.hasNext()) {
      write(key, it.next());
    }
  }

  @Override
  public void write(BytesWritable key, BytesWritable value) throws IOException {
    if (writerState != WriterState.RUNNING) {
      throw new IOException("Write already closed or spill failed");
    }

    if (trackMaxKeyValLen) {
      maxKeyLen = Math.max(maxKeyLen, key.getLength());
      maxValLen = Math.max(maxValLen, value.getLength());
    }

    if (compositeFetch && numPartitions == 1) {
      if (isPipelinedShuffle) {
        writeSinglePartitionPipelined(key, value);
      } else if (!considerDataViaEvents) {
        writer.appendNoRleTez(key, value);
      } else {
        writer.appendNoRle(key, value);
      }
    } else {
      int partition = partitioner.getPartition(key, value, numPartitions);
      writeRecord(key, value, partition);
    }
  }

  @Override
  public void writeWithPartition(BytesWritable key, BytesWritable value, int partition) throws IOException {
    // Skipping checks for key-value types.
    // IFile takes care of these, but should be removed from there as well.

    if (writerState != WriterState.RUNNING) {
      throw new IOException("Write already closed or spill failed");
    }

    if (trackMaxKeyValLen) {
      maxKeyLen = Math.max(maxKeyLen, key.getLength());
      maxValLen = Math.max(maxValLen, value.getLength());
    }

    if (compositeFetch && numPartitions == 1) {
      // Special case where there is only one partition; no partition buffers are needed.

      // The reason outputRecordsCounter isn't updated here:
      // The non-pipelined writer updates output counters when it closes.
      // The pipelined writer updates them here because each spill uses a separate IFile writer.

      // For considerDataViaEvents path, call appendNoRle() because
      // DME payload does not include TezOffsetRecord metadata.

      if (isPipelinedShuffle) {
        writeSinglePartitionPipelined(key, value);
      } else if (!considerDataViaEvents) {
        writer.appendNoRleTez(key, value);
      } else {
        writer.appendNoRle(key, value);
      }
    } else {
      writeRecord(key, value, partition);
    }
  }

  @Override
  public WriteValueBytes requestWriteValueBytes(BytesWritable key, int partition) throws IOException {
    Preconditions.checkArgument(compositeFetch && numPartitions > 1);

    if (writerState != WriterState.RUNNING) {
      throw new IOException("Write already closed or spill failed");
    }

    assert currentBuffer.availableSize == currentBuffer.buffer.length - currentBuffer.nextPosition;

    int metaSkip = computeMetaSkip(currentBuffer.nextPosition);
    int requiredBeforeValue = metaSkip + PARTITIONED_META_SIZE + key.getLength();
    if (currentBuffer.full || currentBuffer.availableSize < requiredBeforeValue) {
      return null;
    }

    int maxValueBytes = currentBuffer.availableSize - requiredBeforeValue;
    if (maxValueBytes == 0) {
      return null;
    }

    return new WriteValueBytes(currentBuffer.buffer,
        currentBuffer.nextPosition + requiredBeforeValue, maxValueBytes);
  }

  // Invariant:
  //   completeWriteValueBytes() is the next call after requestWriteValueBytes()
  //   0 <= valLen <= WriteValueBytes.maxValueBytes
  @Override
  public void completeWriteValueBytes(BytesWritable key, int valLen, int partition) throws IOException {
    if (trackMaxKeyValLen) {
      maxKeyLen = Math.max(maxKeyLen, key.getLength());
      maxValLen = Math.max(maxValLen, valLen);
    }

    assert currentBuffer.availableSize == currentBuffer.buffer.length - currentBuffer.nextPosition;

    final int keyLen = key.getLength();
    final int nextPosition = currentBuffer.nextPosition;
    final int metaSkip = computeMetaSkip(nextPosition);
    final int requiredBeforeValue = metaSkip + PARTITIONED_META_SIZE + keyLen;
    final long recordBytesWithOverhead = (long) requiredBeforeValue + valLen;

    // because valLen <= WriteValueBytes.maxValueBytes
    Preconditions.checkArgument(
      recordBytesWithOverhead <= currentBuffer.availableSize,
      "record bytes=" + recordBytesWithOverhead + ", availableSize=" + currentBuffer.availableSize);

    final int metaStart = nextPosition + metaSkip;
    final int keyStart = metaStart + PARTITIONED_META_SIZE;
    final int valStart = keyStart + keyLen;
    final int newNextPosition = valStart + valLen;

    System.arraycopy(key.getBytesRaw(), key.getOffset(), currentBuffer.buffer, keyStart, keyLen);

    currentBuffer.nextPosition = newNextPosition;
    currentBuffer.availableSize -= (int) recordBytesWithOverhead;

    updateRecordMetadata(metaSkip, metaStart, valStart, partition);
  }

  private void writeSinglePartitionPipelined(BytesWritable key, BytesWritable value) throws IOException {
    // Encode directly into the final spill format.
    // Closing a spill is synchronous so caller-owned key/value arrays never have to be retained.
    if (writer == null) {
      openSinglePartitionPipelinedSpill();
    }

    assert compositeFetch;
    writer.appendNoRleTez(key, value);

    long recordBytes = (long) key.getLength() + value.getLength();
    singlePartitionSpillRecordBytes += recordBytes;
    singlePartitionSpillRecords++;

    if (reportPartitionStats()) {
      sizePerPartition[0] += recordBytes;
    }

    localOutputRecordBytesCounter += recordBytes;
    localOutputBytesWithOverheadCounter += recordBytes + 2 * INT_SIZE;
    localOutputRecordsCounter++;
    if (localOutputRecordsCounter % NOTIFY_THRESHOLD == 0) {
      updateTezCountersAndNotify();
    }

    long recordBytesWithOverhead = recordBytes + 2 * INT_SIZE;
    if (recordBytesWithOverhead > singlePartitionSpillSizeLimit) {
      outputLargeRecordsCounter.increment(1);
    }

    // In general, we cannot compute the exact number of final physical bytes because of compression.
    // Thus, estimatedSpillBytes is just a best effort to to start a new spill
    // before the current spill's projected raw serialized size exceeds singlePartitionSpillSizeLimit.
    // With compression, the number of physical bytes is usually smaller than estimatedSpillBytes.
    // TODO: introduce a configuration key for adjusting estimatedSpillBytes (e.g, by multiplying 0.75)
    long estimatedSpillBytes =
        singlePartitionSpillRecordBytes + (long) singlePartitionSpillRecords * 2 * INT_SIZE;
    if (estimatedSpillBytes >= singlePartitionSpillSizeLimit) {
      closeSinglePartitionPipelinedSpill(false);
    }
  }

  private void openSinglePartitionPipelinedSpill() throws IOException {
    int spillNumber = numSpills.getAndIncrement();
    boolean useMemoryOutput = spillNumber == 0
        || (useFreeMemoryWriterOutput &&
            MultiByteArrayOutputStream.canUseFreeMemoryBuffers(freeMemoryThreshold));
    singlePartitionSpillPathDetails = getSpillPathDetails(
        false, spillNumber, useMemoryOutput);

    if (spillNumber == 0) {
      // availableMemoryBytes is reserved for this UnorderedPartitionedKVWriter, so create MultiByteArrayOutputStream
      singlePartitionByteArrayOutput = new MultiByteArrayOutputStream(rfs, outputFileHandler,
          singlePartitionSpillPathDetails.uniqueName, assignedMemoryBytes);
      singlePartitionSpillOutput = new FSDataOutputStream(singlePartitionByteArrayOutput, null);
    } else {
      // we have consumed availableMemoryBytes reserved for this UnorderedPartitionedKVWriter, so check free memory
      singlePartitionByteArrayOutput = null;
      if (useMemoryOutput) {
        singlePartitionByteArrayOutput = new MultiByteArrayOutputStream(rfs, outputFileHandler,
            singlePartitionSpillPathDetails.uniqueName);
        singlePartitionSpillOutput = new FSDataOutputStream(singlePartitionByteArrayOutput, null);
      } else {
        singlePartitionSpillOutput = rfs.create(singlePartitionSpillPathDetails.outputFilePath);
        ensureSpillFilePermissions(singlePartitionSpillPathDetails.outputFilePath, rfs, rfsSpillFilePerms);
      }
    }

    CompressionCodec spillCodec = spillCompressed ? codec : null;
    writer = new IFile.WriterBytesWritable(singlePartitionSpillOutput, spillCodec, null, null,
        compositeFetch, maxKeyLen, maxValLen, IFile.allocateWriteBuffer(), null, outputContext);
    singlePartitionSpillRecordBytes = 0;
    singlePartitionSpillRecords = 0;
  }

  private void closeSinglePartitionPipelinedSpill(boolean finalUpdate) throws IOException {
    assert writer != null;
    IFile.WriterBytesWritable spillWriter = writer;
    FSDataOutputStream spillOutput = singlePartitionSpillOutput;
    MultiByteArrayOutputStream byteArrayOutput = singlePartitionByteArrayOutput;
    SpillPathDetails spillPathDetails = singlePartitionSpillPathDetails;
    int spillRecords = singlePartitionSpillRecords;
    long spillRecordBytes = singlePartitionSpillRecordBytes;

    writer = null;
    singlePartitionSpillOutput = null;
    singlePartitionByteArrayOutput = null;
    singlePartitionSpillPathDetails = null;
    singlePartitionSpillRecords = 0;
    singlePartitionSpillRecordBytes = 0;

    try {
      spillWriter.close();
    } finally {
      spillOutput.close();
    }

    long rawLength = spillWriter.getRawLength();
    long compressedLength = spillWriter.getCompressedLength();
    TezIndexRecord indexRecord = new TezIndexRecord(0, rawLength, compressedLength);
    TezSpillRecord spillRecord = new TezSpillRecord(1);
    spillRecord.putIndex(indexRecord, 0);
    Map<Integer, TezOffsetRecord> spillOffsetRecordMap = null;
    if (compositeFetch && indexRecord.hasData()) {
      spillOffsetRecordMap = new HashMap<>();
      spillOffsetRecordMap.put(0, spillWriter.getTezOffsetRecord());
    }
    handleSpillIndex(spillPathDetails, spillRecord, byteArrayOutput, spillOffsetRecordMap);

    if (byteArrayOutput == null) {
      fileOutputBytesCounter.increment(compressedLength);
      if (writeSpillRecord) {
        fileOutputBytesCounter.increment(indexFileSizeEstimate);
      }
    } else {
      fileOutputBytesMemoryCounter.increment(compressedLength);
    }
    if (!finalUpdate) {
      spilledRecordsCounter.increment(spillRecords);
      updateTezCountersAndNotify();
    }

    int[] spillRecordsPerPartition = new int[] { spillRecords };
    long[] spillSizePerPartition = reportPartitionStats()
        ? new long[] { spillRecordBytes } : null;
    sendEventsForSpillForPipelined(spillRecordsPerPartition,
        finalUpdate ? sizePerPartition : spillSizePerPartition,
        spillPathDetails.spillIndex, finalUpdate);
  }

  private void writeRecord(BytesWritable key, BytesWritable value, int partition) throws IOException {
    // Wrap to 4 byte (Int) boundary for metaData
    int metaSkip = computeMetaSkip(currentBuffer.nextPosition);
    if ((currentBuffer.availableSize < (PARTITIONED_META_SIZE + metaSkip)) || (currentBuffer.full)) {
      // Move over to the next buffer.
      metaSkip = 0;
      setupNextBuffer();
    }
    currentBuffer.nextPosition += metaSkip;
    int metaStart = currentBuffer.nextPosition;
    currentBuffer.availableSize -= (PARTITIONED_META_SIZE + metaSkip);
    currentBuffer.nextPosition += PARTITIONED_META_SIZE;

    baos.write(key.getBytesRaw(), key.getOffset(), key.getLength());

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
        writeRecord(key, value, partition);
        return;
      }
    }

    int valStart = currentBuffer.nextPosition;
    baos.write(value.getBytesRaw(), value.getOffset(), value.getLength());

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
        writeRecord(key, value, partition);
        return;
      }
    }

    updateRecordMetadata(metaSkip, metaStart, valStart, partition);
  }

  private void updateRecordMetadata(int metaSkip, int metaStart, int valStart, int partition) {
    final WrappedBuffer buffer = currentBuffer;
    final int nextPosition = buffer.nextPosition;

    final int dataStart = metaStart + PARTITIONED_META_SIZE;
    final int keyLen = valStart - dataStart;
    final int valLen = nextPosition - valStart;
    final int recordBytes = nextPosition - dataStart;
    final int bytesWithOverhead = recordBytes + PARTITIONED_META_SIZE + metaSkip;

    final int metaIndex = metaStart / INT_SIZE;
    final IntBuffer metaBuffer = buffer.metaBuffer;

    metaBuffer.put(metaIndex + INDEX_KEYLEN, keyLen);
    metaBuffer.put(metaIndex + INDEX_VALLEN, valLen);
    metaBuffer.put(metaIndex + INDEX_NEXT, WrappedBuffer.PARTITION_ABSENT_POSITION);

    localOutputRecordBytesCounter += recordBytes;
    localOutputBytesWithOverheadCounter += bytesWithOverhead;
    localOutputRecordsCounter++;

    if (localOutputRecordBytesCounter % NOTIFY_THRESHOLD == 0) {
      updateTezCountersAndNotify();
    }

    final int previousTail = buffer.partitionTails[partition];

    if (previousTail != WrappedBuffer.PARTITION_ABSENT_POSITION) {
      final int previousTailMetaIndex = previousTail >> 2;
      metaBuffer.put(previousTailMetaIndex + INDEX_NEXT, metaStart);
    } else {
      buffer.partitionHeads[partition] = metaStart;
    }

    buffer.partitionTails[partition] = metaStart;

    buffer.recordsPerPartition[partition]++;
    buffer.sizePerPartition[partition] += recordBytes;
    buffer.numRecords++;
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
      if (isDebugEnabled || (filledBufferCount > 0 && (filledBufferCount % 10) == 0)) {
        LOG.info("{}: Moving to next buffer. Total filled buffers: {}", destNameTrimmed, filledBufferCount);
      }
      updateGlobalStats(currentBuffer);

      filledBuffers.add(currentBuffer);
      mayBeSpill();

      currentBuffer = getNextAvailableBuffer();

      // in case spill threads are free, check if spilling is needed
      mayBeSpill();
    }
  }

  private void mayBeSpill() {
    if (filledBuffers.size() >= spillLimit) {
      // Do not block; possible that there are more buffers
      tryScheduleSpill();
    }
  }

  private void tryScheduleSpill() {
    if (filledBuffers.isEmpty()) {
      return;
    }
    // Data in filledBuffers would be spilled in a subsequent iteration if no slot is available.
    if (!availableSlots.tryAcquire()) {
      return;
    }
    scheduleSpillAfterSlotAcquired();
  }

  private void scheduleSpillBlocking(int minFilledBufferSize) throws InterruptedException {
    if (filledBuffers.size() < minFilledBufferSize) {
      return;
    }
    availableSlots.acquire();
    scheduleSpillAfterSlotAcquired();
  }

  private void scheduleSpillAfterSlotAcquired() {
    pendingSpillCount.incrementAndGet();
    int spillNumber = numSpills.getAndIncrement();

    // spill to free memory only in pipelined shuffling:
    //   - In non-pipelined shuffling, spilling to free memory causes too much memory pressure before merging.
    //   - E.g., query 5 and query 17
    // TODO: introduce a runtime configuration key for controlling spillToFreeMemory in non-pipelined shuffling
    boolean spillToFreeMemory = isPipelinedShuffle && useFreeMemoryWriterOutput;

    CompressionCodec spillCodec = spillCompressed ? codec : null;
    WrappedBuffer oldestBuffer = filledBuffers.remove(0);
    ListenableFuture<SpillResult> future = spillExecutor.submit(new SpillCallable(
        Collections.singletonList(oldestBuffer), spillCodec, spilledRecordsCounter,
        spillNumber, spillToFreeMemory));
    Futures.addCallback(future, new SpillCallback(spillNumber));
    // Update once per buffer (instead of every record)
    updateTezCountersAndNotify();
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
          scheduleSpillBlocking(spillLimit);
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
    private final boolean spillToFreeMemory;

    public SpillCallable(List<WrappedBuffer> filledBuffers, CompressionCodec codec,
        TezCounter numRecordsCounter, SpillPathDetails spillPathDetails, boolean spillToFreeMemory) {
      this(filledBuffers, codec, numRecordsCounter, spillPathDetails.spillIndex, spillToFreeMemory);
      Preconditions.checkArgument(spillToFreeMemory || spillPathDetails.outputFilePath != null,
          "Spill output file path cannot be null");
      this.spillPathDetails = spillPathDetails;
    }

    public SpillCallable(List<WrappedBuffer> filledBuffers, CompressionCodec codec,
        TezCounter numRecordsCounter, int spillNumber, boolean spillToFreeMemory) {
      this.filledBuffers = filledBuffers;
      this.codec = codec;
      this.numRecordsCounter = numRecordsCounter;
      this.spillNumber = spillNumber;
      this.spillToFreeMemory = spillToFreeMemory;
    }

    @Override
    public SpillResult call() throws IOException {
      // This should not be called with an empty buffer. Check before invoking.

      // Number of parallel spills determined by number of threads.
      // Last spill synchronization handled separately.
      SpillResult spillResult = null;
      MultiByteArrayOutputStream byteArrayOutput = null;
      boolean canUseBuffers = spillToFreeMemory
          && MultiByteArrayOutputStream.canUseFreeMemoryBuffers(freeMemoryThreshold);
      if (spillPathDetails == null) {
        this.spillPathDetails = getSpillPathDetails(false, spillNumber, canUseBuffers);
      }
      if (canUseBuffers) {
        byteArrayOutput = new MultiByteArrayOutputStream(
            rfs, outputFileHandler, spillPathDetails.uniqueName);
      }

      final boolean isRleEnabled = false;
      final Map<Integer, TezOffsetRecord> spillOffsetRecordMap =
        (compositeFetch && !isRleEnabled) ? new HashMap<>() : null;

      FSDataOutputStream fsOutput = null;
      long compressedLength = 0;
      TezSpillRecord spillRecord = new TezSpillRecord(numPartitions);
      Compressor compressorExternal = null;
      try {
        if (byteArrayOutput == null) {
          Path outputFilePath = spillPathDetails.outputFilePath == null
              ? outputFileHandler.getFileForWrite(spillPathDetails.uniqueName)
              : spillPathDetails.outputFilePath;
          fsOutput = rfs.create(outputFilePath);
          ensureSpillFilePermissions(outputFilePath, rfs, rfsSpillFilePerms);
        } else {
          fsOutput = new FSDataOutputStream(byteArrayOutput, null);
          // spillPathDetails.outputFilePath is not used
        }

        if (isDebugEnabled) {
          LOG.debug("Writing spill {} to {} (use in-memory buffers = {})",
              spillNumber, spillPathDetails.outputFilePath, canUseBuffers);
        }

        RawDataBuffer key = new RawDataBuffer();
        RawDataBuffer val = new RawDataBuffer();
        byte[] writeBuffer = IFile.allocateWriteBuffer();

        for (int i = 0; i < numPartitions; i++) {
          WriterDataInputBuffer writer = null;
          try {
            long segmentStart = fsOutput.getPos();
            long numRecords = 0;
            for (WrappedBuffer buffer : filledBuffers) {
              if (!hasRecordsForPartition(i, buffer)) {
                // Skip empty partition.
                continue;
              }
              if (writer == null) {
                if (codec != null && compressorExternal == null) {
                  compressorExternal = outputContext.getCompressor(codec);
                }
                // all Writer instances share the same FSDataOutputStream out
                writer = new WriterDataInputBuffer(
                    fsOutput, codec, null, null, compositeFetch, false,
                    maxKeyLen, maxValLen,
                    writeBuffer, compressorExternal, outputContext);
              }
              numRecords += writeBufferedPartition(i, buffer, writer, key, val);
            }
            if (writer != null) {
              if (numRecordsCounter != null) {
                // TezCounter (from TaskCounter) is not thread-safe; Since numRecordsCounter would be updated from
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
              if (spillOffsetRecordMap != null && indexRecord.hasData()) {
                spillOffsetRecordMap.put(i, writer.getTezOffsetRecord());
              }
              writer = null;
            }
          } finally {
            if (writer != null) {
              writer.close();
            }
          }
        }
      } finally {
        if (compressorExternal != null) {
          outputContext.returnCompressor(codec.getCompressorType(), compressorExternal);
        }
        if (fsOutput != null) {
          fsOutput.close();
        }
      }

      spillResult = new SpillResult(compressedLength, this.filledBuffers, canUseBuffers);

      // spillPathDetails.spillIndex can be -1 if spillIndex was not used in pathComponent
      handleSpillIndex(spillPathDetails, spillRecord, byteArrayOutput, spillOffsetRecordMap);
      if (isDebugEnabled) {
        LOG.debug("{}: Finished spill {}", destNameTrimmed, spillPathDetails.spillIndex);
      }

      return spillResult;
    }
  }

  private long writePartition(int pos, WrappedBuffer wrappedBuffer, WriterDataInputBuffer writer,
                              RawDataBuffer keyBuffer, RawDataBuffer valBuffer) throws IOException {
    long numRecords = 0;
    while (pos != WrappedBuffer.PARTITION_ABSENT_POSITION) {
      int metaIndex = pos / INT_SIZE;
      int keyLength = wrappedBuffer.metaBuffer.get(metaIndex + INDEX_KEYLEN);
      int valLength = wrappedBuffer.metaBuffer.get(metaIndex + INDEX_VALLEN);
      keyBuffer.reset(wrappedBuffer.buffer, pos + PARTITIONED_META_SIZE, keyLength);
      valBuffer.reset(wrappedBuffer.buffer, pos + PARTITIONED_META_SIZE + keyLength, valLength);

      if (compositeFetch) {
        writer.appendNoRleTez(keyBuffer, valBuffer);
      } else {
        writer.appendNoRle(keyBuffer, valBuffer);
      }
      numRecords++;
      pos = wrappedBuffer.metaBuffer.get(metaIndex + INDEX_NEXT);
    }
    return numRecords;
  }

  private boolean hasRecordsForPartition(int partition, WrappedBuffer wrappedBuffer) {
    return wrappedBuffer.partitionHeads[partition] != WrappedBuffer.PARTITION_ABSENT_POSITION;
  }

  private long writeBufferedPartition(int partition, WrappedBuffer wrappedBuffer,
                                      WriterDataInputBuffer writer, RawDataBuffer keyBuffer,
                                      RawDataBuffer valBuffer) throws IOException {
    return writePartition(wrappedBuffer.partitionHeads[partition], wrappedBuffer, writer, keyBuffer,
        valBuffer);
  }

  private static int computeMetaSkip(int position) {
    int mod = position % INT_SIZE;
    return mod == 0 ? 0 : INT_SIZE - mod;
  }

  public static long getInitialMemoryRequirement(Configuration conf, long totalTaskMemoryBytes) {
    long initialMemRequestMb = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB,
        TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB_DEFAULT);
    Preconditions.checkArgument(initialMemRequestMb > 0,
        TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB + " should be larger than 0");
    return initialMemRequestMb << 20;
  }

  private boolean canSendDataOverDME() {
    return considerDataViaEvents && writer.getCompressedLength() <= dataViaEventsMaxSize;
  }

  private ByteBuffer readDataForDME() throws IOException {
    int length = (int) writer.getCompressedLength();
    byte[] buf = new byte[length];
    try (InputStream in = singlePartitionByteArrayOutput.createInputStreamFrom(0, length)) {
      IOUtils.readFully(in, buf, 0, length);
    }
    return ByteBuffer.wrap(buf);
  }

  public List<Event> close() throws IOException, InterruptedException {
    spillLock.lock();
    try {
      if (writerState == WriterState.RUNNING) {
        writerState = WriterState.CLOSED;
      }
    } finally {
      spillLock.unlock();
    }

    // In case there are buffers to be spilled, schedule spilling.
    // For final-merge mode, filledBuffers are merged directly in mergeAll(), so skip scheduling.
    if (isPipelinedShuffle && !(compositeFetch && numPartitions == 1)) {
      scheduleSpillBlocking(1);
    }
    spillLock.lock();
    try {
      if (isDebugEnabled) {
        if (pendingSpillCount.get() != 0) {
          LOG.debug("{}: Waiting for all spills to complete : Pending : {}", destNameTrimmed, pendingSpillCount.get());
        }
      }
      while (pendingSpillCount.get() != 0 && writerState != WriterState.SPILL_FAILED) {
        spillInProgress.await();
      }
    } finally {
      spillLock.unlock();
    }

    if (writerState == WriterState.SPILL_FAILED) {
      LOG.error(destNameTrimmed + ": Error during spill, throwing");
      // Assuming close will be called on the same thread as the write
      cleanup();
      cleanupCurrentBuffer();
      throw new IOException("Exception during spill");
    }

    List<Event> eventList = Lists.newLinkedList();
    if (!isPipelinedShuffle) {
      if (compositeFetch && numPartitions == 1) {  // written directly to the final IFile writer
        writer.close();   // okay, the final data was written to either disk or memory
        singlePartitionByteArrayOutput.close();
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
        // no current buffer exists for a single-partition output

        if (outputRecordsCounter.getValue() > 0) {
          outputBytesWithOverheadCounter.increment(rawLen);
        }
        eventList.add(generateVMEvent());

        if (!canSendDataOverDME()) {
          // Publish the final data from disk or adaptive memory and update the corresponding counter.
          TezIndexRecord rec = new TezIndexRecord(0, rawLen, compLen);
          TezSpillRecord sr = new TezSpillRecord(1);
          sr.putIndex(rec, 0);

          if (writeSpillRecord) {
            Path finalIndexPath = outputFileHandler.getOutputIndexFileForWrite();
            sr.writeToFile(finalIndexPath, localFs, localFsSpillFilePerms);
            fileOutputBytesCounter.increment(compLen + indexFileSizeEstimate);
          } else {
            // localFs is not needed when writeSpillRecord == false; even in DME fallback we use
            // output-context index/path cache instead of writing a local spill index file.
            final boolean isRleEnabled = false;   // because we use WriterBytesWritable which does not support RLE
            final Map<Integer, TezOffsetRecord> spillOffsetRecordMap =
              (trackMaxKeyValLen && !isRleEnabled) ? new HashMap<>() : null;
            if (spillOffsetRecordMap != null && rec.hasData()) {
              spillOffsetRecordMap.put(0, writer.getTezOffsetRecord());
            }

            ShuffleUtils.writeToIndexPathCacheAndByteCache(
                outputContext, null, sr, singlePartitionByteArrayOutput, spillOffsetRecordMap);
            fileOutputBytesMemoryCounter.increment(compLen);
          }
        }
        eventList.add(generateDMEvent(false, -1, false,
            ShuffleUtils.getPathComponent(outputContext, compositeFetch), emptyPartitions));

        cleanup();
        return eventList;
      } else {
        // Final merge enabled
        //   - When lots of spills are there, mergeAll, generate events and return
        //   - If there is no data and no existing spills, skip final output generation
        // Keep the same no-data fast path in finalSpill() if there were no prior spills.
        // filledBuffers may NOT be empty because we did not schedule spills earlier.
        boolean noDataWithNoSpills =
          (numSpills.get() == 0) && filledBuffers.isEmpty() && (currentBuffer.nextPosition == 0);
        if (!noDataWithNoSpills) {
          mergeAll();
        }
        updateTezCountersAndNotify();
        eventList.add(generateVMEvent());
        eventList.add(generateDMEvent());

        cleanup();
        filledBuffers.clear();
        cleanupCurrentBuffer();
        return eventList;
      }
    }

    LOG.info("{} UnorderedPartitionedKVWriter for {}: final pipelined numSpills={}",
        outputContext.getTaskAttemptIdStr(), outputContext.getDestinationVertexName(),
        numSpills.get());
    if (compositeFetch && numPartitions == 1) {
      updateTezCountersAndNotify();
      if (writer == null) {
        BitSet emptyPartitions = new BitSet(1);
        emptyPartitions.set(0);
        sendEventsForSpillForPipelined(emptyPartitions, sizePerPartition, numSpills.get(), true);
      } else {
        closeSinglePartitionPipelinedSpill(true);
      }
      cleanup();
      return eventList;
    } else {
      // Update counters before generating the final pipelined VME.
      updateTezCountersAndNotify();

      SpillResult finalSpillResult = finalSpill();

      // A successfully closed pipelined output must terminate its spill-event sequence exactly once.
      // Use the next spill id for an empty final update, or the final spill id when close wrote data.
      // sizePerPartition contains cumulative stats from earlier spills and large records, so it must
      // also be used when the final update itself is empty.
      if (finalSpillResult == null) {
        BitSet emptyPartitions = new BitSet(numPartitions);
        emptyPartitions.set(0, numPartitions);
        sendEventsForSpillForPipelined(emptyPartitions, sizePerPartition, numSpills.get(), true);
      } else {
        // VertexManagerEvent is only sent at the end and thus sizePerPartition is used for the sum of all spills.
        sendEventsForSpillForPipelined(currentBuffer.recordsPerPartition,
            sizePerPartition, numSpills.get() - 1, true);
      }

      cleanup();
      cleanupCurrentBuffer();
      return eventList;
    }
  }

  private BitSet getEmptyPartitions(int[] recordsPerPartition) {
    assert recordsPerPartition != null;
    BitSet emptyPartitions = new BitSet();
    for (int i = 0; i < numPartitions; i++) {
      if (recordsPerPartition[i] == 0) {
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
    return generateDMEvent(false, -1, false,
        ShuffleUtils.getPathComponent(outputContext, compositeFetch), emptyPartitions);
  }

  private Event generateDMEvent(boolean addSpillDetails, int spillId,
      boolean isLastSpill, String pathComponent, BitSet emptyPartitions)
      throws IOException {

    DataMovementEventPayloadProto.Builder payloadBuilder = DataMovementEventPayloadProto.newBuilder();
    if (numPartitions == 1) {
      payloadBuilder.setNumRecord((int) outputRecordsCounter.getValue());
    }

    if (emptyPartitions.cardinality() != 0) {
      // Empty partitions exist
      ByteString emptyPartitionsByteString =
          TezCommonUtils.compressByteArrayToByteString(TezUtilsInternal.toByteArray
              (emptyPartitions), deflater.get());
      payloadBuilder.setEmptyPartitions(emptyPartitionsByteString);
    }

    if (emptyPartitions.cardinality() != numPartitions) {
      // Populate payload only if at least 1 partition has data
      String containerId = outputContext.getExecutionContext().getEnvContainerId();
      int vertexId = outputContext.getTaskVertexIndex();
      payloadBuilder.setContainerId(containerId);
      payloadBuilder.setVertexId(vertexId);
      payloadBuilder.setPathComponent(pathComponent);

      String host = outputContext.getExecutionContext().getHostName();
      payloadBuilder.setHost(host);
      int[] shufflePorts = getShufflePort();
      payloadBuilder.setNumPorts(shufflePorts.length);
      for (int i = 0; i < shufflePorts.length; i++) {
        payloadBuilder.addPorts(shufflePorts[i]);
      }
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

      this.shuffleDataViaEventSize.increment(this.writer.getCompressedLength());
      if (isDebugEnabled) {
        LOG.debug("payload packed in DME, dataSize: " + this.writer.getCompressedLength());
      }
    }

    ByteBuffer payload = payloadBuilder.build().toByteString().asReadOnlyByteBuffer();
    return CompositeDataMovementEvent.create(0, numPartitions, payload);
  }

  private void cleanupCurrentBuffer() {
    if (currentBuffer != null) {
      currentBuffer.cleanup();
      currentBuffer = null;
    }
  }

  private void cleanup() {
    if (spillExecutor != null) {
      spillExecutor.shutdownNow();
    }
    if (buffers != null) {
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
      return null;
    } else {
      updateGlobalStats(currentBuffer);
      filledBuffers.add(currentBuffer);

      // Capture spill count before assigning a spill number.
      boolean hasNoPreviousSpills = (numSpills.get() == 0);
      boolean useMemoryOutput = useFreeMemoryWriterOutput
          && MultiByteArrayOutputStream.canUseFreeMemoryBuffers(freeMemoryThreshold);
      // setup output file and index file
      SpillPathDetails spillPathDetails = getSpillPathDetails(
          true, numSpills.getAndIncrement(), useMemoryOutput);

      // finalSpill() serves two scenarios:
      //  1) isPipelinedShuffle == false && numSpills == 0:
      //     this spill is the final output and must follow final-output compression (codec).
      //  2) otherwise (pipelined path / additional spill generation):
      //     keep spill compression consistent with prior intermediate spills.
      CompressionCodec finalSpillCodec =
          (!isPipelinedShuffle && hasNoPreviousSpills) ? codec : (spillCompressed ? codec : null);
      SpillCallable spillCallable = new SpillCallable(
          filledBuffers, finalSpillCodec, null, spillPathDetails, useMemoryOutput);
      try {
        SpillResult spillResult = spillCallable.call();

        // if we used free memory to store the spill, do not increment fileOutputBytesCounter
        if (!spillResult.useFreeMemoryForOutput) {
          fileOutputBytesCounter.increment(spillResult.spillSize);
        } else {
          fileOutputBytesMemoryCounter.increment(spillResult.spillSize);
        }
        if (writeSpillRecord) {
          // finalIndexPath is used, so add indexFileSizeEstimate
          fileOutputBytesCounter.increment(indexFileSizeEstimate);
        }
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
  private SpillPathDetails getSpillPathDetails(boolean isFinalSpill)
      throws IOException {
    int spillNumber = numSpills.getAndIncrement();
    return getSpillPathDetails(isFinalSpill, spillNumber, false);
  }

  private SpillPathDetails getSpillPathDetails(boolean isFinalSpill,
      int spillNumber, boolean deferOutputPath) throws IOException {
    Path outputFilePath = null;
    Path indexFilePath = null;

    int finalSpillIndex;
    boolean indexComputed = false;   // true if TezSpillRecord is effectively computed (i.e., indexFilePath set)
    if (!isPipelinedShuffle) {
      if (isFinalSpill) {
        if (!deferOutputPath) {
          outputFilePath = outputFileHandler.getFileForWrite(
              Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING);
        }
        if (writeSpillRecord) {
          // do not bother with the file size when creating an index file
          indexFilePath = outputFileHandler.getOutputIndexFileForWrite();
        }
        indexComputed = true;   // because indexFilePath would be set when using mapreduce_shuffle (ignoring writeSpillRecord)
        finalSpillIndex = -1;   // spill index was not used
      } else {
        if (!deferOutputPath) {
          String uniqueSpillName = outputFileHandler.getSpillFileName(spillNumber);
          outputFilePath = outputFileHandler.getFileForWrite(uniqueSpillName);
        }
        finalSpillIndex = spillNumber;
        // indexComputed = false && indexFilePath not set
      }
    } else {
      if (!deferOutputPath) {
        String uniqueSpillName = outputFileHandler.getSpillFileName(spillNumber);
        outputFilePath = outputFileHandler.getFileForWrite(uniqueSpillName);
      }
      if (writeSpillRecord) {
        indexFilePath = outputFileHandler.getSpillIndexFileForWrite(spillNumber);
      }
      indexComputed = true;   // because indexFilePath would be set when using mapreduce_shuffle (ignoring writeSpillRecord)
      finalSpillIndex = spillNumber;
    }

    String uniqueName = isFinalSpill && !isPipelinedShuffle ?
        Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING :
        outputFileHandler.getSpillFileName(spillNumber);

    assert deferOutputPath || outputFilePath != null;
    assert !deferOutputPath || outputFilePath == null;
    return new SpillPathDetails(
        outputFilePath, indexFilePath, finalSpillIndex, indexComputed, uniqueName);
  }

  private void mergeAll() throws IOException {
    if (currentBuffer.nextPosition != 0) {
      // Update final statistics.
      updateGlobalStats(currentBuffer);
    }

    MultiByteArrayOutputStream byteArrayOutput = null;
    if (useFreeMemoryWriterOutput) {
      if (MultiByteArrayOutputStream.canUseFreeMemoryBuffers(freeMemoryThreshold)) {
        byteArrayOutput = new MultiByteArrayOutputStream(rfs, outputFileHandler,
            Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING);
      }
    }

    SpillPathDetails spillPathDetails = getSpillPathDetails(true,
        numSpills.getAndIncrement(), byteArrayOutput != null);

    // if !writeSpillRecord, then spillPathDetails.indexFilePath == null
    Path finalIndexPath = writeSpillRecord ? spillPathDetails.indexFilePath : null;
    Path finalOutPath = spillPathDetails.outputFilePath;

    TezSpillRecord finalSpillRecord = new TezSpillRecord(numPartitions);
    final boolean isFinalMergeRleEnabled = false;   // we do not use RLE encoding below
    final Map<Integer, TezOffsetRecord> spillOffsetRecordMap =
        (compositeFetch && !isFinalMergeRleEnabled) ? new HashMap<>() : null;

    RawDataBuffer keyBuffer = new RawDataBuffer();
    RawDataBuffer valBuffer = new RawDataBuffer();

    RawDataBuffer keyBufferIFile = new RawDataBuffer();
    RawDataBuffer valBufferIFile = new RawDataBuffer();

    FSDataOutputStream out = null;
    long finalOutSize = 0;
    try {
      if (byteArrayOutput == null) {
        Preconditions.checkState(finalOutPath != null, "Final output path must be resolved for disk output");
        out = rfs.create(finalOutPath);
        ensureSpillFilePermissions(finalOutPath, rfs, rfsSpillFilePerms);
      } else {
        out = new FSDataOutputStream(byteArrayOutput, null);
      }
      WriterDataInputBuffer writer;

      byte[] writeBuffer = IFile.allocateWriteBuffer();
      for (int i = 0; i < numPartitions; i++) {
        long segmentStart = out.getPos();
        if (numRecordsPerPartition[i] == 0) {
          if (isDebugEnabled) {
            LOG.debug(destNameTrimmed + ": " + "Skipping partition: " + i + " in final merge since it has no records");
          }
          continue;
        }
        // inside close()
        writer = new WriterDataInputBuffer(
            out, codec, null, null, compositeFetch, false,
            maxKeyLen, maxValLen,
            writeBuffer, null, outputContext);
        try {
          for (WrappedBuffer buffer : filledBuffers) {
            if (hasRecordsForPartition(i, buffer)) {
              writeBufferedPartition(i, buffer, writer, keyBuffer, valBuffer);
            }
          }
          if (hasRecordsForPartition(i, currentBuffer)) {
            // Write current buffer last to preserve oldest->newest in-memory buffer ordering.
            writeBufferedPartition(i, currentBuffer, writer, keyBuffer, valBuffer);
          }
          synchronized (spillInfoList) {
            for (SpillInfo spillInfo : spillInfoList) {
              TezIndexRecord indexRecord = spillInfo.spillRecord.getIndex(i);
              if (indexRecord.getPartLength() == 0) {
                // Skip empty partitions within a spill
                continue;
              }
              IFile.KeyValueReaderDataInputBuffer reader = null;
              TezOffsetRecord spillOffsetRecord =
                  spillInfo.offsetRecordMap != null ? spillInfo.offsetRecordMap.get(i) : null;
              if (!spillCompressed && !compositeFetch) {
                long rawDataLength = indexRecord.getRawLength()
                    - IFile.getHeaderLength() - IFile.getEOFMarkerLength();
                Preconditions.checkState(rawDataLength >= 0,
                    "Invalid raw data length for partition %s from spill %s", i, spillInfo.outPath);
                if (spillInfo.byteArrayOutput == null) {
                  FSDataInputStream in = rfs.open(spillInfo.outPath);
                  boolean closeInput = true;
                  try {
                    in.seek(indexRecord.getStartOffset());
                    IFileInputStream inputStream = IFile.Reader.openIFileInputStream(
                        in, indexRecord.getPartLength(), ifileReadAhead, ifileReadAheadLength);
                    closeInput = false;
                    try (IFileInputStream stream = inputStream) {
                      writer.append(stream, rawDataLength);
                    }
                  } finally {
                    if (closeInput) {
                      in.close();
                    }
                  }
                  additionalSpillBytesReadCounter.increment(indexRecord.getPartLength());
                } else {
                  InputStream input = spillInfo.byteArrayOutput.createInputStreamFrom(
                      indexRecord.getStartOffset(), indexRecord.getPartLength());
                  boolean closeInput = true;
                  try {
                    IFileInputStream inputStream = IFile.Reader.openIFileInputStream(
                        input, indexRecord.getPartLength(), ifileReadAhead, ifileReadAheadLength);
                    closeInput = false;
                    try (IFileInputStream stream = inputStream) {
                      writer.append(stream, rawDataLength);
                    }
                  } finally {
                    if (closeInput) {
                      input.close();
                    }
                  }
                }
              } else {
                CompressionCodec spillCodecForReader = spillCompressed ? codec : null;
                if (spillInfo.byteArrayOutput == null) {
                  FSDataInputStream in = rfs.open(spillInfo.outPath);
                  in.seek(indexRecord.getStartOffset());
                  reader = new IFile.Reader(in, indexRecord.getPartLength(), spillCodecForReader, null,
                      additionalSpillBytesReadCounter, ifileReadAhead, ifileReadAheadLength,
                      outputContext, spillOffsetRecord);
                } else {
                  InputStream input = spillInfo.byteArrayOutput.createInputStreamFrom(
                      indexRecord.getStartOffset(), indexRecord.getPartLength());
                  reader = new IFile.Reader(input, indexRecord.getPartLength(), spillCodecForReader, null, null,
                      ifileReadAhead, ifileReadAheadLength, outputContext, spillOffsetRecord);
                }
                // reader.close() may not be called if the following while{} block throws IOException.
                // In this case, reader.decompressor is not returned to the pool.
                // However, this is not memory leak because reader is eventually garbage collected, at which point
                // reader.decompressor is also garbage collected. It is just that reader.decompressor is not reused.
                // Note that reader.close() itself may throw IOException and reader.decompressor may not be returned to the pool.
                // For the same reason, this not memory leak because reader.decompressor is eventually garbage collected.
                try {
                  while (reader.readRawKey(keyBufferIFile) != IFile.Reader.KeyState.NO_KEY) {
                    // TODO Inefficient for large records, since the entire record will be read into memory.
                    reader.nextRawValue(valBufferIFile);
                    if (compositeFetch) {
                      writer.appendNoRleTez(keyBufferIFile, valBufferIFile);
                    } else {
                      writer.appendNoRle(keyBufferIFile, valBufferIFile);
                    }
                  }
                } finally {
                  reader.close();
                }
              }
            }
          }
          writer.close();
          finalOutSize += writer.getCompressedLength();
          TezIndexRecord indexRecord = new TezIndexRecord(segmentStart, writer.getRawLength(),
              writer.getCompressedLength());
          if (spillOffsetRecordMap != null && indexRecord.hasData()) {
            spillOffsetRecordMap.put(i, writer.getTezOffsetRecord());
          }
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
        cleanIntermediateSpills();
      }
    }

    if (byteArrayOutput == null) {
      fileOutputBytesCounter.increment(finalOutSize);
    } else {
      fileOutputBytesMemoryCounter.increment(finalOutSize);
      assert !writeSpillRecord;
    }

    if (writeSpillRecord) {
      finalSpillRecord.writeToFile(finalIndexPath, localFs, localFsSpillFilePerms);
      fileOutputBytesCounter.increment(indexFileSizeEstimate);
    } else {
      Path outputPath = byteArrayOutput == null ? finalOutPath : null;
      ShuffleUtils.writeToIndexPathCacheAndByteCache(outputContext,
          outputPath, finalSpillRecord, byteArrayOutput, spillOffsetRecordMap);
    }
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
                    if (spill.outPath != null && rfs.exists(spill.outPath)) {
                      LOG.info("Deleting intermediate spill: " + spill.outPath);
                      rfs.delete(spill.outPath, false);
                    }
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

  private void cleanIntermediateSpills() {
    synchronized (spillInfoList) {
      for (SpillInfo spill : spillInfoList) {
        if (spill.byteArrayOutput != null) {
          spill.byteArrayOutput.clean();
        }
      }
    }
  }

  private void writeLargeRecord(final BytesWritable key, final BytesWritable value, final int partition)
      throws IOException {
    numAdditionalSpillsCounter.increment(1);
    SpillPathDetails spillPathDetails = getSpillPathDetails(false);
    int spillIndex = spillPathDetails.spillIndex;   // valid spillIndex and never -1

    FSDataOutputStream out = null;
    long outSize = 0;
    try {
      final TezSpillRecord spillRecord = new TezSpillRecord(numPartitions);
      final Path outPath = spillPathDetails.outputFilePath == null
          ? outputFileHandler.getFileForWrite(spillPathDetails.uniqueName)
          : spillPathDetails.outputFilePath;
      out = rfs.create(outPath);
      ensureSpillFilePermissions(outPath, rfs, rfsSpillFilePerms);
      BitSet emptyPartitions = null;
      if (isPipelinedShuffle) {
        emptyPartitions = new BitSet(numPartitions);
      }
      final boolean isRleEnabled = false;
      Map<Integer, TezOffsetRecord> spillOffsetRecordMap =
          (compositeFetch && !isRleEnabled) ? new HashMap<>() : null;
      for (int i = 0; i < numPartitions; i++) {
        final long recordStart = out.getPos();
        if (i == partition) {
          spilledRecordsCounter.increment(1);
          WriterBytesWritable writer = null;
          try {
            writer = new IFile.WriterBytesWritable(out, codec, null, null,
                compositeFetch,
                maxKeyLen, maxValLen,
                IFile.allocateWriteBufferSingle(), null, outputContext);
            if (compositeFetch) {
              writer.appendNoRleTez(key, value);
            } else {
              writer.appendNoRle(key, value);
            }
            outputLargeRecordsCounter.increment(1);
            numRecordsPerPartition[i]++;
            if (reportPartitionStats()) {
              sizePerPartition[i] += writer.getRawLength();
            }
            writer.close();
            if (!isPipelinedShuffle) {
              // this is an intermediate spill, so increment additionalSpillBytesWrittenCounter.
              synchronized (additionalSpillBytesWrittenCounter) {
                additionalSpillBytesWrittenCounter.increment(writer.getCompressedLength());
              }
            }
            TezIndexRecord indexRecord = new TezIndexRecord(recordStart, writer.getRawLength(),
                writer.getCompressedLength());
            spillRecord.putIndex(indexRecord, i);
            if (spillOffsetRecordMap != null && indexRecord.hasData()) {
              spillOffsetRecordMap.put(i, writer.getTezOffsetRecord());
            }
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
      handleSpillIndex(spillPathDetails, spillRecord, null, spillOffsetRecordMap);

      if (isPipelinedShuffle) {
        // This output file is directly served to downstream tasks, so increment fileOutputBytesCounter.
        fileOutputBytesCounter.increment(rfs.getFileStatus(outPath).getLen());
        sendEventsForSpillForPipelined(emptyPartitions, sizePerPartition, spillIndex, false);
      }

      LOG.info("{}: Finished writing large record of size {} to spill file {}", destNameTrimmed, outSize, spillIndex);
      if (isDebugEnabled) {
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
      @Nullable MultiByteArrayOutputStream byteArrayOutput,
      @Nullable Map<Integer, TezOffsetRecord> offsetRecordMap) throws IOException {
    if (spillPathDetails.indexComputed) {
      if (spillPathDetails.indexFilePath != null) {
        // write the index record
        assert writeSpillRecord;
        spillRecord.writeToFile(spillPathDetails.indexFilePath, localFs, localFsSpillFilePerms);
      } else {
        // only one of outputFilePath and byteArrayOutput is non-null
        Path outputFilePath = byteArrayOutput == null ? spillPathDetails.outputFilePath : null;

        // must check if spillPathDetails.spillIndex == -1
        if (spillPathDetails.spillIndex < 0) {
          ShuffleUtils.writeToIndexPathCacheAndByteCache(outputContext,
              outputFilePath, spillRecord, byteArrayOutput, null);
        } else {
          ShuffleUtils.writeSpillInfoToIndexPathCacheAndByteCache(outputContext,
              spillPathDetails.spillIndex, outputFilePath, spillRecord, byteArrayOutput, offsetRecordMap);
        }
      }
    } else {
      // add to cache
      Path spillPath = (byteArrayOutput == null) ? spillPathDetails.outputFilePath : null;
      SpillInfo spillInfo = new SpillInfo(spillRecord, spillPath, byteArrayOutput, offsetRecordMap);
      spillInfoList.add(spillInfo);
      numAdditionalSpillsCounter.increment(1);
    }
  }

  private class ByteArrayOutputStream extends OutputStream {

    private final byte[] scratch = new byte[1];

    @Override
    public void write(int v) {
      assert false;
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

  private List<Event> generateEventForSpill(
      BitSet emptyPartitions, long[] sizePerPartition,
      int spillNumber, boolean isFinalUpdate) throws IOException {
    List<Event> eventList = Lists.newLinkedList();
    String pathComponent = generatePathComponent(
        ShuffleUtils.getPathComponent(outputContext, compositeFetch), spillNumber);
    if (isFinalUpdate) {
      eventList.add(ShuffleUtils.generateVMEvent(outputContext,
          sizePerPartition, reportDetailedPartitionStats(), deflater.get()));
    }
    Event compEvent = generateDMEvent(true, spillNumber, isFinalUpdate,
        pathComponent, emptyPartitions);
    eventList.add(compEvent);
    return eventList;
  }

  private void sendEventsForSpillForPipelined(
      int[] recordsPerPartition, long[] sizePerPartition,
      int spillNumber, boolean isFinalUpdate) throws IOException {
    BitSet emptyPartitions = getEmptyPartitions(recordsPerPartition);
    sendEventsForSpillForPipelined(emptyPartitions, sizePerPartition, spillNumber, isFinalUpdate);
  }

  private void sendEventsForSpillForPipelined(
      BitSet emptyPartitions, long[] sizePerPartition,
      int spillNumber, boolean isFinalUpdate) throws IOException {
    assert isPipelinedShuffle;
    List<Event> events = generateEventForSpill(
        emptyPartitions, sizePerPartition, spillNumber, isFinalUpdate);
    if (isDebugEnabled) {
      LOG.debug("{}: Adding spill event for spill (final update={}), spillId={}",
          destNameTrimmed, isFinalUpdate, spillNumber);
    }
    outputContext.sendEvents(events);
  }

  private void sendIntermediateEventsForSpillForPipelined(
      int[] recordsPerPartition, long[] sizePerPartition, int spillNumber) {
    try {
      sendEventsForSpillForPipelined(recordsPerPartition, sizePerPartition, spillNumber, false);
    } catch (IOException e) {
      LOG.error(destNameTrimmed + ": Error in sending pipelined events", e);
      outputContext.reportFailure(TaskFailureType.NON_FATAL, e, "Error in sending events.");
    }
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

    // onSuccess() is called only for intermediate spills, while finalSpill() calls SpillCallable.call() directly.
    @Override
    public void onSuccess(SpillResult result) {
      computePartitionStats(result);

      if (isPipelinedShuffle) {
        sendIntermediateEventsForSpillForPipelined(recordsPerPartition, sizePerPartition, spillNumber);
      }

      try {
        for (WrappedBuffer buffer : result.filledBuffers) {
          buffer.reset();
          availableBuffers.add(buffer);
        }
      } catch (Throwable e) {
        LOG.error(destNameTrimmed + ": Failure while attempting to reset buffer after spill", e);
        outputContext.reportFailure(TaskFailureType.NON_FATAL, e, "Failure while attempting to reset buffer after spill");
      }

      if (!isPipelinedShuffle) {
        // only for intermediate spills
        assert !result.useFreeMemoryForOutput;
        synchronized(additionalSpillBytesWrittenCounter) {
          // isPipelinedShuffle == false, so this is counted as an intermediate spill
          additionalSpillBytesWrittenCounter.increment(result.spillSize);
        }
      } else {
        // isPipelinedShuffle == true, so this is directly served to downstream tasks
        if (!result.useFreeMemoryForOutput) {
          synchronized(fileOutputBytesCounter) {
            fileOutputBytesCounter.increment(result.spillSize);
            if (writeSpillRecord) {
              fileOutputBytesCounter.increment(indexFileSizeEstimate);
            }
          }
        } else {
          synchronized(fileOutputBytesMemoryCounter) {
            fileOutputBytesMemoryCounter.increment(result.spillSize);
            assert !writeSpillRecord;
          }
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
      LOG.error("{}: Failure while spilling to disk", destNameTrimmed, t);
      outputContext.reportFailure(TaskFailureType.NON_FATAL, t, "Failure while spilling to disk");
      spillLock.lock();
      try {
        writerState = WriterState.SPILL_FAILED;
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
    final boolean useFreeMemoryForOutput;

    SpillResult(long size, List<WrappedBuffer> filledBuffers, boolean useFreeMemoryForOutput) {
      this.spillSize = size;
      this.filledBuffers = filledBuffers;
      this.useFreeMemoryForOutput = useFreeMemoryForOutput;
    }
  }

  private static class SpillInfo {
    final TezSpillRecord spillRecord;
    // Exactly one of outPath and byteArrayOutput must be non-null.
    @Nullable final Path outPath;
    // Optional memory-backed spill representation for merge-time reads.
    @Nullable final MultiByteArrayOutputStream byteArrayOutput;
    @Nullable final Map<Integer, TezOffsetRecord> offsetRecordMap;

    SpillInfo(TezSpillRecord spillRecord, @Nullable Path outPath,
        @Nullable MultiByteArrayOutputStream byteArrayOutput,
        @Nullable Map<Integer, TezOffsetRecord> offsetRecordMap) {
      assert (outPath == null) != (byteArrayOutput == null);
      this.spillRecord = spillRecord;
      this.outPath = outPath;
      this.byteArrayOutput = byteArrayOutput;
      this.offsetRecordMap = offsetRecordMap;
    }
  }

  int[] getShufflePort() throws IOException {
    ByteBuffer shuffleMetadata = outputContext.getServiceProviderMetaData(auxiliaryService);
    return ShuffleUtils.deserializeShuffleProviderMetaData(shuffleMetadata);
  }

  static class SpillPathDetails {
    // null if writeSpillRecord == false
    final Path indexFilePath;
    final Path outputFilePath;
    final String uniqueName;

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
                     boolean indexComputed,
                     String uniqueName) {
      this.outputFilePath = outputFilePath;
      this.indexFilePath = indexFilePath;
      this.spillIndex = spillIndex;
      this.indexComputed = indexComputed;
      this.uniqueName = uniqueName;
    }
  }
}
