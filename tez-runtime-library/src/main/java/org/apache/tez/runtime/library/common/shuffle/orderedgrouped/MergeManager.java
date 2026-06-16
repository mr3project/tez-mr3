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
package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import org.apache.tez.common.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.runtime.library.common.sort.impl.RawDataBuffer;
import org.apache.hadoop.io.FileChunk;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.ShuffleClient;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.IFile.WriterDataInputBuffer;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger.DiskSegment;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger.InputStreamSegment;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger.Segment;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Usage. Create instance. setInitialMemoryAvailable(long), configureAndStart()
 *
 */
@SuppressWarnings(value={"rawtypes"})
public class MergeManager implements FetchedInputAllocatorOrderedGrouped {
  
  private static final Logger LOG = LoggerFactory.getLogger(MergeManager.class);
  private static final boolean isDebugEnabled = LOG.isDebugEnabled();

  private final Configuration conf;
  private final FileSystem localFS;
  private final FileSystem rfs;
  
  private final TezTaskOutputFiles mapOutputFile;

  final Set<MapOutput> inMemoryMergedMapOutputs =
    new TreeSet<MapOutput>(new MapOutput.MapOutputComparator());
  private final IntermediateMemoryToMemoryMerger memToMemMerger;

  // InMemoryMapOutput or InputStreamMapOutput
  final Set<MapOutput> inMemoryMapOutputs =
    new TreeSet<MapOutput>(new MapOutput.MapOutputComparator());
  private final InMemoryMerger inMemoryMerger;

  final Set<FileChunk> onDiskMapOutputs = new TreeSet<FileChunk>();
  final OnDiskMerger onDiskMerger;
  
  private final long memoryLimitBytes;
  final long postMergeMemoryLimitBytes;

  // Lifecycle of InMemoryMapOutput:
  // - create InMemoryMapOutput, increase usedMemory
  // - 1. InMemoryMapOutput.commit()
  //      --> closeInMemoryFile()
  //      --> InMemoryReader
  //      --> releaseCommittedMemory()
  // - 2. InMemoryMapOutput.abort()

  // Lifecycle of InputStreamMapOutput (LOCAL_BYTE_CACHE):
  // - create InputStreamMapOutput, do not increase usedMemory
  // - 1. InputStreamMapOutput.commit()
  //      --> closeInMemoryFile()
  //      --> createMapOutputReader() creates IFile.Reader over InputStream
  //      --> InputStreamSegment marks it as streaming, not in-memory, for merge buffer reuse
  //      --> stream is consumed while merging
  //      --> IFile.Reader.close() releases stream resources
  // - 2. InputStreamMapOutput.abort()
  //      --> closes InputStream without enqueuing

  // MergeManager’s internal memory budget for controlling fetching
  // increases at the time of creating InMemoryMapOutput
  // guard with synchronized (this), or synchronized (manager) inside MergeThread
  private long usedMemory;
  // Merge decisions depend on committed sizes (commitMemory >= mergeThreshold).
  // increases when InMemoryMapOutput is accepted as valid input
  // guard with synchronized (this), or synchronized (manager) inside MergeThread
  private long commitMemory;

  private final int ioSortFactor;

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  private final long maxSingleShuffleLimit;
  private final int memToMemMergeOutputsThreshold;
  private final long mergeThreshold;
  
  private final ExceptionReporter exceptionReporter;
  
  private final InputContext inputContext;

  private final TezCounter spilledRecordsCounter;
  private final TezCounter mergedMapOutputsCounter;
  
  private final TezCounter numMemToDiskMerges;
  private final TezCounter numDiskToDiskMerges;
  private final TezCounter additionalSpillBytesWritten;
  private final TezCounter additionalSpillBytesRead;
  
  private final CompressionCodec codec;
  
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;

  // Variables for stats
  private final SegmentStatsTracker statsInMemTotal = new SegmentStatsTracker();

  private final AtomicInteger mergeFileSequenceId = new AtomicInteger(0);

  private final boolean cleanup;

  private final boolean useFreeMemoryFetchedInput;
  private final long freeMemoryThreshold;   // minimum size of free memory for useFreeMemoryFetchedInput
  private final long freeMemoryLimit;       // free memory that can be assigned to this LogicalInput
  private final boolean compositeFetch;

  private final Map<String, String> mdcContext;

  private int getNextMergeFileSequenceId() {
    return mergeFileSequenceId.getAndIncrement();
  }

  private String getMergerId() {
    return "manager_" + getNextMergeFileSequenceId();
  }

  /**
   * Construct the MergeManager. Must call start before it becomes usable.
   */
  public MergeManager(Configuration conf,
                      FileSystem localFS,
                      InputContext inputContext,
                      TezCounter spilledRecordsCounter,
                      TezCounter mergedMapOutputsCounter,
                      ExceptionReporter exceptionReporter,
                      long assignedMemoryBytes,
                      CompressionCodec codec,
                      boolean ifileReadAheadEnabled,
                      int ifileReadAheadLength) {
    this.inputContext = inputContext;
    this.conf = conf;
    this.exceptionReporter = exceptionReporter;

    this.spilledRecordsCounter = spilledRecordsCounter;
    this.mergedMapOutputsCounter = mergedMapOutputsCounter;

    this.compositeFetch = ShuffleUtils.isTezShuffleHandler(conf);
    this.mapOutputFile = new TezTaskOutputFiles(conf,
        inputContext.getUniqueIdentifierForOutputFiles(),
        inputContext.getDagIdentifier(),
        inputContext.getExecutionContext().getEnvContainerId(),
        inputContext.getTaskVertexIndex(),
        this.compositeFetch);

    this.localFS = localFS;
    this.rfs = ((LocalFileSystem)localFS).getRaw();
    
    this.numDiskToDiskMerges = inputContext.getCounters().findCounter(TaskCounter.MERGE_NUM_DISK_TO_DISK_MERGES);
    this.numMemToDiskMerges = inputContext.getCounters().findCounter(TaskCounter.MERGE_NUM_MEM_TO_DISK_MERGES);
    this.additionalSpillBytesWritten = inputContext.getCounters().findCounter(TaskCounter.SPILL_BYTES_DISK);
    this.additionalSpillBytesRead = inputContext.getCounters().findCounter(TaskCounter.SPILL_BYTES_READ_ADDITIONAL);

    this.cleanup = conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT,
        TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT_DEFAULT);

    this.codec = codec;
    this.ifileReadAhead = ifileReadAheadEnabled;
    if (this.ifileReadAhead) {
      this.ifileReadAheadLength = ifileReadAheadLength;
    } else {
      this.ifileReadAheadLength = 0;
    }

    final float maxInMemCopyUse = conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT_DEFAULT);
    if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
      throw new IllegalArgumentException("Invalid value for " +
          TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT + ": " +
          maxInMemCopyUse);
    }
    long totalTaskMemoryBytes = inputContext.getTotalMemoryAvailableToTask();
    long memLimit = (long)(totalTaskMemoryBytes * maxInMemCopyUse);
    this.memoryLimitBytes = Math.min(assignedMemoryBytes, memLimit);

    float maxRedPer = conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_BUFFER_PERCENT_DEFAULT);
    if (maxRedPer > 1.0 || maxRedPer < 0.0) {
      throw new TezUncheckedException(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT + maxRedPer);
    }
    long maxRedBuffer = (long)(totalTaskMemoryBytes * maxRedPer);
    this.postMergeMemoryLimitBytes = Math.min(assignedMemoryBytes, maxRedBuffer);

    this.ioSortFactor = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR,
        TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR_DEFAULT);
    
    final float maxSingleShuffleMemoryLimitPercent = conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT_DEFAULT);
    if (maxSingleShuffleMemoryLimitPercent <= 0.0f
        || maxSingleShuffleMemoryLimitPercent > 1.0f) {
      throw new IllegalArgumentException("Invalid value for "
          + TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT + ": "
          + maxSingleShuffleMemoryLimitPercent);
    }

    // TODO: Cap it to MAX_VALUE until MapOutput starts supporting > 2 GB
    this.maxSingleShuffleLimit = (long) Math.min((memoryLimitBytes * maxSingleShuffleMemoryLimitPercent), Integer.MAX_VALUE);
    this.memToMemMergeOutputsThreshold = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMTOMEM_SEGMENTS, ioSortFactor);
    this.mergeThreshold = (long)(this.memoryLimitBytes * conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT_DEFAULT));

    // OrderedGroupedKVInput
    LOG.info("{} MergeManager for {}: assignedMemoryBytes={}",
        inputContext.getTaskAttemptIdStr(), inputContext.getSourceVertexName(),
        assignedMemoryBytes);

    if (isDebugEnabled) {
      LOG.debug(inputContext.getSourceVertexName() + ": MergerManager: " +
          "memoryLimitBytes=" + memoryLimitBytes + ", " +
          "maxSingleShuffleLimit=" + maxSingleShuffleLimit + ", " +
          "mergeThreshold=" + mergeThreshold + ", " +
          "ioSortFactor=" + ioSortFactor + ", " +
          "postMergeMemLimitBytes=" + postMergeMemoryLimitBytes + ", " +
          "memToMemMergeOutputsThreshold=" + memToMemMergeOutputsThreshold);
    }
    
    if (this.maxSingleShuffleLimit >= this.mergeThreshold) {
      throw new RuntimeException("Invalid configuration: "
          + "maxSingleShuffleLimit should be less than mergeThreshold"
          + "maxSingleShuffleLimit: " + this.maxSingleShuffleLimit
          + ", mergeThreshold: " + this.mergeThreshold);
    }

    boolean allowMemToMemMerge = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM_DEFAULT);
    if (allowMemToMemMerge) {
      this.memToMemMerger = new IntermediateMemoryToMemoryMerger(this, memToMemMergeOutputsThreshold);
    } else {
      this.memToMemMerger = null;
    }
    this.inMemoryMerger = new InMemoryMerger(this);
    this.onDiskMerger = new OnDiskMerger(this);

    this.useFreeMemoryFetchedInput = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_FETCHED_INPUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_FETCHED_INPUT_DEFAULT);
    this.freeMemoryThreshold = totalTaskMemoryBytes;  // TODO: factor

    final float freeMemoryFactor = conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_FREE_MEMORY_FACTOR_FOR_FETCHED_INPUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_FREE_MEMORY_FACTOR_FOR_FETCHED_INPUT_DEFAULT);
    if (freeMemoryFactor <= 0.0f) {
      throw new IllegalArgumentException("Invalid value for "
        + TezRuntimeConfiguration.TEZ_RUNTIME_FREE_MEMORY_FACTOR_FOR_FETCHED_INPUT + ": "
        + freeMemoryFactor);
    }
    this.freeMemoryLimit = (long)(totalTaskMemoryBytes * freeMemoryFactor);

    this.mdcContext = inputContext.getMdcContext();
  }

  void setupParentThread(Thread shuffleSchedulerThread) {
    if (isDebugEnabled) {
      LOG.debug("Setting merger's parent thread to " + shuffleSchedulerThread.getName());
    }
    if (this.memToMemMerger != null) {
      memToMemMerger.setParentThread(shuffleSchedulerThread);
    }
    this.inMemoryMerger.setParentThread(shuffleSchedulerThread);;
    this.onDiskMerger.setParentThread(shuffleSchedulerThread);
  }

  void configureAndStart() {
    if (this.memToMemMerger != null) {
      memToMemMerger.start();
    }
    this.inMemoryMerger.start();
    this.onDiskMerger.start();
  }

  /**
   * Exposing this to get an initial memory ask without instantiating the object.
   */
  static long getInitialMemoryRequirement(Configuration conf, long totalTaskMemoryBytes) {
    float maxInMemCopyUse = conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT_DEFAULT);
    if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
      throw new IllegalArgumentException(
          TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT + ": " + maxInMemCopyUse);
    }
    final long memLimit = (long)(totalTaskMemoryBytes * maxInMemCopyUse);

    float maxRedPer = conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_BUFFER_PERCENT_DEFAULT);
    if (maxRedPer > 1.0 || maxRedPer < 0.0) {
      throw new TezUncheckedException(
        TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT + ": " + maxRedPer);
    }
    final long maxRedBuffer = (long) (totalTaskMemoryBytes * maxRedPer);

    if (isDebugEnabled) {
      LOG.debug("Initial Memory required for SHUFFLE_BUFFER=" + memLimit +
          " based on INPUT_BUFFER_FACTOR=" + maxInMemCopyUse + ",  for final merged output=" +
          maxRedBuffer + ", using factor: " + maxRedPer);
    }

    return Math.max(maxRedBuffer, memLimit);
  }

  public void waitForInMemoryMerge() throws InterruptedException {
    inMemoryMerger.waitForMerge();

    /**
     * Memory released during merge process could have been used by active fetchers and if they
     * are too fast, 'commitMemory & usedMemory' could have grown beyond allowed threshold. Since
     * merge was already in progress, this would not have kicked off another merge and fetchers
     * could get into indefinite wait state later. To address this, trigger another merge process
     * if needed and wait for it to complete (to release committedMemory & usedMemory).
     */
    boolean triggerAdditionalMerge = false;
    synchronized (this) {
      if (this.commitMemory >= mergeThreshold) {
        startMemToDiskMerge();
        triggerAdditionalMerge = true;
      }
    }
    if (triggerAdditionalMerge) {
      inMemoryMerger.waitForMerge();
    }
  }

  public synchronized void waitForShuffleToMergeMemory() throws InterruptedException {
    long startTime = System.currentTimeMillis();
    while (this.usedMemory > memoryLimitBytes) {
      wait();
    }
    if (isDebugEnabled) {
      LOG.debug("Waited for " + (System.currentTimeMillis() - startTime) + " for memory to become available");
    }
  }

  final private MapOutput stallShuffle = MapOutput.createWaitMapOutput(null);

  @Override
  public MapOutput reserve(
      InputAttemptIdentifier srcAttemptIdentifier,
      long actualSize,
      long compressedLength,
      int fetcher) throws IOException {
    if (actualSize > maxSingleShuffleLimit) {
      if (useFreeMemoryFetchedInput) {
        synchronized (this) {
          MapOutput result = getMemoryMapOutputInternal(
              srcAttemptIdentifier, 0L, actualSize, true);
          if (result != null) {
            return result;
          }
        }
      }
      return getDiskMapOutput(compressedLength, srcAttemptIdentifier, fetcher);
    }
    
    // Stall shuffle if we are above the memory limit

    // It is possible that all threads could just be stalling and not make
    // progress at all. This could happen when:
    //
    // requested size is causing the used memory to go above limit &&
    // requested size < maxSingleShuffleLimit &&
    // current used size < mergeThreshold (merge will not get triggered)
    //
    // To avoid this from happening, we allow exactly one thread to go past
    // the memory limit. We check (usedMemory > memoryLimit) and not
    // (usedMemory + actualSize > memoryLimit). When this thread is done
    // fetching, this will automatically trigger a merge thereby unlocking
    // all the stalled threads

    synchronized (this) {
      if (this.usedMemory > memoryLimitBytes) {
        if (!useFreeMemoryFetchedInput || !hasFreeMemoryForSize(actualSize)) {
          // this ContainerWorker is busy serving Tasks, so do not borrow
          return stallShuffle;
        }

        MapOutput result = getMemoryMapOutputInternal(
            srcAttemptIdentifier, 0L, actualSize, false);
        if (result != null) {
          return result;
        }
        return stallShuffle;
      } else {
        // Allow the in-memory shuffle to progress
        MapOutput result = getMemoryMapOutputInternal(
            srcAttemptIdentifier, actualSize, actualSize, false);
        if (result != null) {
          return result;
        }
        // TODO: can we return stallShuffle without stalling all Fetchers?
        return getDiskMapOutput(compressedLength, srcAttemptIdentifier, fetcher);
      }
    }
  }

  // Invariant: inside this.synchronized{}
  private boolean hasFreeMemoryForSize(long actualSize) {
    long currentFreeMemory = Runtime.getRuntime().freeMemory();
    return currentFreeMemory >= freeMemoryThreshold && this.usedMemory + actualSize <= freeMemoryLimit;
  }

  // Invariant: inside this.synchronized{}
  private MapOutput getMemoryMapOutputInternal(
      InputAttemptIdentifier srcAttemptIdentifier,
      long usedMemoryForMergeManager, long actualSize,
      boolean checkFreeMemory) {
    if (!checkFreeMemory || hasFreeMemoryForSize(actualSize)) {
      try {
        // usedMemoryForMergeManager = 0 because this MemoryMapOutput should not contribute to usedMemory
        MapOutput result = unconditionalReserve(
            srcAttemptIdentifier, usedMemoryForMergeManager, actualSize, true);
        if (isDebugEnabled) {
          LOG.debug("Created MemoryMapOutput: {}, {}", this.usedMemory, actualSize);
        }
        return result;
      } catch (OutOfMemoryError oom) {
        LOG.error("Failed to created MemoryMapOutput: {}, {}", this.usedMemory, actualSize, oom);
      }
    }
    return null;
  }

  private MapOutput getDiskMapOutput(
      long compressedLength, InputAttemptIdentifier srcAttemptIdentifier, int fetcher) throws IOException {
    if (isDebugEnabled) {
      LOG.debug("Creating DiskMapOutput: {}", compressedLength);
    }
    return MapOutput.createDiskMapOutput(srcAttemptIdentifier, this, compressedLength, conf,
        fetcher, true, mapOutputFile);
  }

  /**
   * Unconditional Reserve is used by the Memory-to-Memory thread
   */
  // InMemoryMapOutput must call unreserve(), either
  //   directly from InMemoryOutput.abort() or
  //   indirectly via releaseCommittedMemory() after closeInMemoryFile() is called from InMemoryMapOutput.commit().
  private synchronized MapOutput unconditionalReserve(
      InputAttemptIdentifier srcAttemptIdentifier,
      long usedMemoryForMergeManager,
      long requestedSize,
      boolean primaryMapOutput) {
    // createMemoryMapOutput() may throw OOM, so increase usedMemory only if successful
    MapOutput result = MapOutput.createMemoryMapOutput(
        srcAttemptIdentifier, this, usedMemoryForMergeManager, requestedSize, primaryMapOutput);
    this.usedMemory += usedMemoryForMergeManager;
    return result;
  }

  @Override
  public synchronized void unreserve(long size) {
    assert usedMemory >= size;
    this.usedMemory -= size;
    if (isDebugEnabled) {
      LOG.debug("Notifying unreserve : size=" + size + ", commitMemory=" + this.commitMemory + ", usedMemory=" + this.usedMemory
          + ", mergeThreshold=" + mergeThreshold);
    }
    notifyAll();
  }

  @Override
  public synchronized void releaseCommittedMemory(long commitSize, long usedMemoryForMergeManager) {
    assert usedMemoryForMergeManager == commitSize || usedMemoryForMergeManager == 0L;
    assert commitMemory >= commitSize;
    this.commitMemory -= commitSize;
    unreserve(usedMemoryForMergeManager);
  }

  @Override
  public synchronized void closeInMemoryFile(MapOutput mapOutput) { 
    inMemoryMapOutputs.add(mapOutput);
    trackAndLogCloseInMemoryFile(mapOutput);

    this.commitMemory += mapOutput.getSizeForMergeMemoryAccounting();

    if (this.commitMemory >= mergeThreshold) {
      startMemToDiskMerge();
    }

    // This should likely run a Combiner.
    if (memToMemMerger != null) {
      synchronized (memToMemMerger) {
        if (!memToMemMerger.isInProgress() && inMemoryMapOutputs.size() >= memToMemMergeOutputsThreshold) {
          memToMemMerger.startMerge(inMemoryMapOutputs);
        }
      }
    }
  }

  private void trackAndLogCloseInMemoryFile(MapOutput mapOutput) {
    statsInMemTotal.updateStats(mapOutput.getSizeForMergeMemoryAccounting());

    if (isDebugEnabled) {
      LOG.debug("closeInMemoryFile -> map-output of size: " + mapOutput.getSizeForMergeMemoryAccounting()
          + ", inMemoryMapOutputs.size() -> " + inMemoryMapOutputs.size()
          + ", commitMemory -> " + this.commitMemory + ", usedMemory ->" +
          this.usedMemory + ", mapOutput=" + mapOutput);
    }
  }

  private void startMemToDiskMerge() {
    synchronized (inMemoryMerger) {
      if (!inMemoryMerger.isInProgress()) {
        LOG.info("{}: Starting inMemoryMerger's merge since commitMemory={} > mergeThreshold={}. Current usedMemory={}",
            inputContext.getSourceVertexName(), this.commitMemory, mergeThreshold, this.usedMemory);
        inMemoryMapOutputs.addAll(inMemoryMergedMapOutputs);
        inMemoryMergedMapOutputs.clear();
        inMemoryMerger.startMerge(inMemoryMapOutputs);
      }
    }
  }
  
  public synchronized void closeInMemoryMergedFile(MapOutput mapOutput) {
    inMemoryMergedMapOutputs.add(mapOutput);
    if (isDebugEnabled) {
      // This log could be moved to INFO level for a while, after mem-to-mem
      // merge is production ready.
      LOG.debug("closeInMemoryMergedFile -> size: " + mapOutput.getSizeForMergeMemoryAccounting() +
          ", inMemoryMergedMapOutputs.size() -> " +
          inMemoryMergedMapOutputs.size());
    }

    this.commitMemory += mapOutput.getSizeForMergeMemoryAccounting();

    if (this.commitMemory >= mergeThreshold) {
      startMemToDiskMerge();
    }
  }

  @Override
  public FileSystem getLocalFileSystem() {
    return localFS;
  }

  @Override
  public synchronized void closeOnDiskFile(FileChunk file) {
    // including only path & offset for valdiations.
    for (FileChunk fileChunk : onDiskMapOutputs) {
      if (fileChunk.getPath().equals(file.getPath())) {
        // ensure offsets are not the same.
        Preconditions.checkArgument(fileChunk.getOffset() != file.getOffset(),
            "Can't have a file with same path and offset. OldFilePath={}, OldFileOffset={}, newFilePath={}, newFileOffset={}",
          fileChunk.getPath(), fileChunk.getOffset(), file.getPath(), file.getOffset());
      }
    }

    onDiskMapOutputs.add(file);
    logCloseOnDiskFile(file);

    synchronized (onDiskMerger) {
      if (!onDiskMerger.isInProgress() &&
          onDiskMapOutputs.size() >= (2 * ioSortFactor - 1)) {
        onDiskMerger.startMerge(onDiskMapOutputs);
      }
    }
  }

  private void logCloseOnDiskFile(FileChunk file) {
    if (isDebugEnabled) {
      LOG.debug("close onDiskFile=" + file.getPath() + ", len=" + file.getLength() +
          ", onDisMapOutputs=" + onDiskMapOutputs.size());
    }
  }

  public TezRawKeyValueIterator close(boolean tryFinalMerge) throws Throwable {
    if (!isShutdown.getAndSet(true)) {
      // Wait for on-going merges to complete
      if (memToMemMerger != null) {
        memToMemMerger.close();
      }
      inMemoryMerger.close();
      onDiskMerger.close();

      List<MapOutput> memory = new ArrayList<MapOutput>(inMemoryMergedMapOutputs);
      inMemoryMergedMapOutputs.clear();
      memory.addAll(inMemoryMapOutputs);
      inMemoryMapOutputs.clear();
      List<FileChunk> disk = new ArrayList<FileChunk>(onDiskMapOutputs);
      onDiskMapOutputs.clear();

      if (isDebugEnabled) {
        if (statsInMemTotal.count > 0) {
          LOG.debug(
              "TotalInMemFetchStats: count={}, totalSize={}, min={}, max={}, avg={}",
              statsInMemTotal.count, statsInMemTotal.size,
              statsInMemTotal.minSize, statsInMemTotal.maxSize,
              (statsInMemTotal.size / (float) statsInMemTotal.count));
        }
      }

      // Don't attempt a final merge if close is invoked as a result of a previous
      // shuffle exception / error.
      if (tryFinalMerge) {
        try {
          TezRawKeyValueIterator kvIter = finalMerge(conf, rfs, memory, disk);
          return kvIter;
        } catch (InterruptedException e) {
          // Clean up the disk segments
          if (cleanup) {
            cleanup(localFS, disk);
            cleanup(localFS, onDiskMapOutputs);
          }
          Thread.currentThread().interrupt(); //reset interrupt status
          throw e;
        }
      }
    }
    return null;
  }

  public boolean isShutdown() {
    return isShutdown.get();
  }

  static void cleanup(FileSystem fs, Collection<FileChunk> fileChunkList) {
    for (FileChunk fileChunk : fileChunkList) {
      cleanup(fs, fileChunk.getPath());
    }
  }

  static void cleanup(FileSystem fs, Path path) {
    if (path == null) {
      return;
    }

    try {
      fs.delete(path, true);
    } catch (IOException e) {
      LOG.warn("Error in deleting {}", path);
    }
  }

  /**
   * Merges multiple in-memory segment to another in-memory segment
   */
  private class IntermediateMemoryToMemoryMerger 
  extends MergeThread<MapOutput> {

    public IntermediateMemoryToMemoryMerger(MergeManager manager,
                                            int mergeFactor) {
      super(manager, mergeFactor, exceptionReporter);
      setName("MemToMemMerger [" + inputContext.getSourceVertexName() + "_" + inputContext.getUniqueIdentifier() + "]");
      setDaemon(true);
    }

    @Override
    protected Map<String, String> getMdcContext() {
      return mdcContext;
    }

    @Override
    public void merge(List<MapOutput> inputs) throws IOException, InterruptedException {
      if (inputs == null || inputs.isEmpty()) {
        return;
      }

      InputAttemptIdentifier dummyMapId = inputs.get(0).getAttemptIdentifier();
      List<Segment> inMemorySegments = new ArrayList<Segment>();

      MapOutput mergedMapOutputs = null;

      long mergeOutputSize = 0l;
      //Lock manager so that fetcher threads can not change the mem size
      synchronized (manager) {

        Iterator<MapOutput> it = inputs.iterator();
        MapOutput lastAddedMapOutput = null;
        while (it.hasNext() && !Thread.currentThread().isInterrupted()) {
          MapOutput mo = it.next();
          // We have to use mo.getSizeForMergeMemoryAccounting(), not mo.getUsedMemoryForMergeManager(), because
          // we will create a buffer big enough to hold the sum of the actual sizes of all selected inputs.
          // Adding manager.getUsedMemory() is okay because
          // the guard is about whether we can safely charge the new merged buffer under the budget.
          if ((mergeOutputSize + mo.getSizeForMergeMemoryAccounting() + manager.getUsedMemory()) > memoryLimitBytes) {
            // Search for smaller segments that can fit into existing mem
            if (isDebugEnabled) {
              LOG.debug("Size is greater than usedMemory. "
                  + "mergeOutputSize=" + mergeOutputSize
                  + ", moSize=" + mo.getSizeForMergeMemoryAccounting()
                  + ", usedMemory=" + manager.getUsedMemory()
                  + ", memoryLimit=" + memoryLimitBytes);
            }
          } else {
            mergeOutputSize += mo.getSizeForMergeMemoryAccounting();
            inMemorySegments.add(createMapOutputSegment(mo));
            lastAddedMapOutput = mo;
            it.remove();
            if (isDebugEnabled) {
              LOG.debug("Added segment for merging. mergeOutputSize=" + mergeOutputSize);
            }
          }
        }

        // Add any unused MapOutput back
        inMemoryMapOutputs.addAll(inputs);

        //Exit early, if 0 or 1 segment is available
        if (inMemorySegments.size() <= 1) {
          if (lastAddedMapOutput != null) {
            inMemoryMapOutputs.add(lastAddedMapOutput);
          }
          return;
        }

        try {
          // usedMemoryForMergeManager == mergeOutputSize
          mergedMapOutputs = unconditionalReserve(dummyMapId, mergeOutputSize, mergeOutputSize, false);
        } catch (OutOfMemoryError err) {
          throw new IOException("Cannot perform merging in MemoryToMemoryMerger - do not use Memory-to-Memory merging", err);
        }
      }

      int noInMemorySegments = inMemorySegments.size();

      IFile.WriterAppendDataInputBuffer writer = new InMemoryWriter(mergedMapOutputs.getMemory());

      if (isDebugEnabled) {
        LOG.debug("{}: Initiating Memory-to-Memory merge with {} segments of total-size: {}",
            inputContext.getSourceVertexName(), noInMemorySegments, mergeOutputSize);
      }

      if (Thread.currentThread().isInterrupted()) {
        return; // early exit
      }

      // Nothing will be materialized to disk because the sort factor is being
      // set to the number of in-memory segments.
      // TODO Is this doing any combination ?
      TezRawKeyValueIterator rIter = TezMerger.merge(conf, rfs, null, inMemorySegments,
            inMemorySegments.size(), 0,
            mapOutputFile, getMergerId(),
            false, null, null, null, true, inputContext);
      TezMerger.writeFile(rIter, writer,
          TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);
      writer.close();

      if (isDebugEnabled) {
        LOG.debug("{} Memory-to-Memory merge of the {} files in-memory complete with mergeOutputSize={}",
            inputContext.getSourceVertexName(), noInMemorySegments, mergeOutputSize);
      }

      // Note the output of the merge
      closeInMemoryMergedFile(mergedMapOutputs);
    }

    @Override
    public void cleanup(List<MapOutput> inputs, boolean deleteData)
        throws IOException, InterruptedException {
      //No OP
    }
  }
  
  /**
   * Merges multiple in-memory segment to a disk segment
   */
  private class InMemoryMerger extends MergeThread<MapOutput> {

    volatile InputAttemptIdentifier srcTaskIdentifier;
    volatile Path outputPath;

    private final byte[] writeBuffer;

    public InMemoryMerger(MergeManager manager) {
      super(manager, Integer.MAX_VALUE, exceptionReporter);
      setName("MemtoDiskMerger [" + inputContext.getSourceVertexName() + "_" + inputContext.getUniqueIdentifier()  + "]");
      setDaemon(true);
      writeBuffer = IFile.allocateWriteBuffer();
    }

    @Override
    protected Map<String, String> getMdcContext() {
      return mdcContext;
    }

    @Override
    public void merge(List<MapOutput> inputs) throws IOException, InterruptedException {
      if (inputs == null || inputs.isEmpty()) {
        return;
      }

      numMemToDiskMerges.increment(1);

      // name this output file same as the name of the first file that is
      // there in the current list of inmem files (this is guaranteed to
      // be absent on the disk currently. So we don't overwrite a prev.
      // created spill). Also we need to create the output file now since
      // it is not guaranteed that this file will be present after merge
      // is called (we delete empty files as soon as we see them
      // in the merge method)

      // figure out the mapId
      srcTaskIdentifier = inputs.get(0).getAttemptIdentifier();

      List<Segment> inMemorySegments = new ArrayList<Segment>();
      long mergeOutputSize = createInMemorySegments(inputs, inMemorySegments,0);
      int noInMemorySegments = inMemorySegments.size();

      // TODO Maybe track serialized vs deserialized bytes.
      
      // All disk writes done by this merge are overhead - due to the lack of
      // adequate memory to keep all segments in memory.
      outputPath = mapOutputFile.getInputFileForWrite(
          srcTaskIdentifier.getInputIdentifier(), srcTaskIdentifier.getSpillEventId(),
          mergeOutputSize).suffix(Constants.MERGED_OUTPUT_PREFIX);

      WriterDataInputBuffer writer = null;
      long outFileLen = 0;
      try {
        writer = new WriterDataInputBuffer(rfs, outputPath, codec, null, null,
            true, writeBuffer, inputContext);

        TezRawKeyValueIterator rIter = null;
        LOG.info("Initiating in-memory merge with {} segments", noInMemorySegments);

        // Nothing actually materialized to disk - controlled by setting sort-factor to #segments.
        rIter = TezMerger.merge(conf, rfs, null,
            inMemorySegments, inMemorySegments.size(), 0,
            mapOutputFile, getMergerId(),
            false, spilledRecordsCounter, null,
            additionalSpillBytesRead, true, inputContext);
        // spilledRecordsCounter is tracking the number of keys that will be
        // read from each of the segments being merged - which is essentially
        // what will be written to disk.

        TezMerger.writeFile(rIter, writer,
            TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);
        writer.close();
        additionalSpillBytesWritten.increment(writer.getCompressedLength());
        writer = null;

        outFileLen = localFS.getFileStatus(outputPath).getLen();
        LOG.info("{} Merge of the {} files in-memory complete. Local file is {} of size {}",
            inputContext.getUniqueIdentifier(), noInMemorySegments, outputPath, outFileLen);
      } catch (IOException e) {
        //make sure that we delete the ondisk file that we created 
        //earlier when we invoked cloneFileAttributes
        localFS.delete(outputPath, true);
        throw e;
      } finally {
        if (writer != null) {
          writer.close();
        }
      }

      // Note the output of the merge
      closeOnDiskFile(new FileChunk(outputPath, 0, outFileLen));
    }

    @Override
    public void cleanup(List<MapOutput> inputs, boolean deleteData) {
      if (deleteData) {
        // Additional check at task level
        if (cleanup) {
          LOG.info("Try deleting stale data: {}", outputPath);
          MergeManager.cleanup(localFS, outputPath);
        }
      }
    }
  }

  /**
   * Merges multiple on-disk segments
   */
  class OnDiskMerger extends MergeThread<FileChunk> {

    volatile Path outputPath;

    private final byte[] writeBuffer;

    public OnDiskMerger(MergeManager manager) {
      super(manager, ioSortFactor, exceptionReporter);
      setName("DiskToDiskMerger [" +  inputContext.getSourceVertexName() + "_" + inputContext.getUniqueIdentifier() + "]");
      setDaemon(true);
      writeBuffer = IFile.allocateWriteBuffer();
    }

    @Override
    protected Map<String, String> getMdcContext() {
      return mdcContext;
    }

    @Override
    public void merge(List<FileChunk> inputs) throws IOException, InterruptedException {
      // sanity check
      if (inputs == null || inputs.isEmpty()) {
        LOG.info("No ondisk files to merge...");
        return;
      }
      numDiskToDiskMerges.increment(1);

      LOG.info("OnDiskMerger: We have {} map outputs on disk. Triggering merge...", inputs.size());

      long approxOutputSize = 0;
      List<Segment> inputSegments = new ArrayList<Segment>(inputs.size());

      // 1. Prepare the list of files to be merged.
      for (FileChunk fileChunk : inputs) {
        final long offset = fileChunk.getOffset();
        final long size = fileChunk.getLength();
        final boolean preserve = fileChunk.isLocalFile();
        if (isDebugEnabled) {
          LOG.debug("InputAttemptIdentifier=" + fileChunk.getInputAttemptIdentifier()
              + ", len=" + fileChunk.getLength() + ", offset=" + fileChunk.getOffset()
              + ", path=" + fileChunk.getPath());
        }
        final Path file = fileChunk.getPath();
        approxOutputSize += size;
        DiskSegment segment = new DiskSegment(rfs, file, offset, size, codec, ifileReadAhead,
            ifileReadAheadLength, preserve, null, inputContext);
        inputSegments.add(segment);
      }

      // add the checksum length
      approxOutputSize += (long)ChecksumFileSystem.getApproxChkSumLength(approxOutputSize);

      // 2. Start the on-disk merge process
      FileChunk file0 = inputs.get(0);
      String namePart;
      if (file0.isLocalFile()) {
        // This is setup the same way a type DISK MapOutput is setup when fetching.
        namePart = mapOutputFile.getSpillFileName(
            file0.getInputAttemptIdentifier().getInputIdentifier(),
            file0.getInputAttemptIdentifier().getSpillEventId());
      } else {
        namePart = file0.getPath().getName().toString();
      }

      outputPath = mapOutputFile.getMergedFileForWrite(
          namePart, approxOutputSize, getNextMergeFileSequenceId());

      WriterDataInputBuffer writer = new WriterDataInputBuffer(rfs, outputPath, codec, null, null,
          true, writeBuffer, inputContext);
      try {
        TezRawKeyValueIterator iter = TezMerger.merge(conf, rfs,
            null, inputSegments, ioSortFactor, 0,
            mapOutputFile, getMergerId(),
            true, spilledRecordsCounter, null,
            mergedMapOutputsCounter, true, inputContext);

        // TODO Maybe differentiate between data written because of Merges and
        // the finalMerge (i.e. final mem available may be different from initial merge mem)
        TezMerger.writeFile(iter, writer,
            TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);
        writer.close();
        additionalSpillBytesWritten.increment(writer.getCompressedLength());
      } catch (IOException e) {
        localFS.delete(outputPath, true);
        throw e;
      }

      final long outputLen = localFS.getFileStatus(outputPath).getLen();
      closeOnDiskFile(new FileChunk(outputPath, 0, outputLen));

      LOG.info("{} Finished merging {} map output files on disk of total-size {}. Local output file is {} of size {}",
          inputContext.getSourceVertexName(), inputs.size(), approxOutputSize, outputPath, outputLen);
    }

    @Override
    public void cleanup(List<FileChunk> inputs, boolean deleteData) throws IOException, InterruptedException {
      if (deleteData) {
        // Additional check at task level
        if (cleanup) {
          LOG.info("Try deleting stale chunks and data: {}", outputPath);
          MergeManager.cleanup(localFS, inputs);
          MergeManager.cleanup(localFS, outputPath);
        }
      }
    }
  }
  
  private long createInMemorySegments(List<MapOutput> inMemoryMapOutputs,
                                      List<Segment> inMemorySegments, 
                                      long leaveBytes) throws IOException {
    long totalSize = 0L;
    // We could use fullSize could come from the RamManager, but files can be
    // closed but not yet present in inMemoryMapOutputs
    long fullSize = 0L;
    for (MapOutput mo : inMemoryMapOutputs) {
      fullSize += mo.getSizeForMergeMemoryAccounting();
    }
    int inMemoryMapOutputsOffset = 0;
    while((fullSize > leaveBytes) && !Thread.currentThread().isInterrupted()) {
      MapOutput mo = inMemoryMapOutputs.get(inMemoryMapOutputsOffset++);
      long size = mo.getSizeForMergeMemoryAccounting();
      totalSize += size;
      fullSize -= size;
      inMemorySegments.add(createMapOutputSegment(mo));
    }
    // Bulk remove removed in-memory map outputs efficiently
    inMemoryMapOutputs.subList(0, inMemoryMapOutputsOffset).clear();
    return totalSize;
  }

  private Segment createMapOutputSegment(MapOutput mapOutput) throws IOException {
    IFile.KeyValueReaderDataInputBuffer reader = createMapOutputReader(mapOutput);
    TezCounter mapOutputsCounter = mapOutput.isPrimaryMapOutput() ? mergedMapOutputsCounter : null;
    if (mapOutput.getType() == ShuffleClient.Type.LOCAL_BYTE_CACHE) {
      return new InputStreamSegment(reader, mapOutputsCounter);
    }
    return new Segment(reader, mapOutputsCounter);
  }

  private IFile.KeyValueReaderDataInputBuffer createMapOutputReader(MapOutput mapOutput) throws IOException {
    assert mapOutput.getType() == ShuffleClient.Type.MEMORY || mapOutput.getType() == ShuffleClient.Type.LOCAL_BYTE_CACHE;
    if (mapOutput.getType() == ShuffleClient.Type.LOCAL_BYTE_CACHE) {
      java.io.InputStream inputStream = mapOutput.getInputStream();
      final long commitSize = mapOutput.getSizeForMergeMemoryAccounting();
      final long readerLength = mapOutput.getReaderLength();
      return new IFile.Reader(
          inputStream, readerLength, codec,
          null, null, ifileReadAhead, ifileReadAheadLength, inputContext, null) {
        @Override
        public void close() throws IOException {
          try {
            super.close();
          } finally {
            releaseCommittedMemory(commitSize, 0L);
          }
        }
      };
    }
    byte[] data = mapOutput.getMemory();
    return new InMemoryReader(
        MergeManager.this, mapOutput.getAttemptIdentifier(), data, 0, data.length,
        (int) mapOutput.getUsedMemoryForMergeManager(), null);
  }

  static class RawKVIteratorReader implements IFile.KeyValueReaderDataInputBuffer {

    private final TezRawKeyValueIterator kvIter;
    private final long size;
    private int lastNextResult = TezRawKeyValueIterator.NO_MORE_KEY_VALUE;

    public RawKVIteratorReader(TezRawKeyValueIterator kvIter, long size) {
      this.kvIter = kvIter;
      this.size = size;
    }

    @Override
    public IFile.Reader.KeyState readRawKey(RawDataBuffer key) throws IOException {
      lastNextResult = kvIter.next();
      if (lastNextResult == TezRawKeyValueIterator.NO_MORE_KEY_VALUE) {
        return IFile.Reader.KeyState.NO_KEY;
      }

      final RawDataBuffer kb = kvIter.getKey();
      final int kp = kb.getPosition();
      final int klen = kb.getLength();
      key.reset(kb.getData(), kp, klen);
      return kvIter.isSameKey() ? IFile.Reader.KeyState.SAME_KEY : IFile.Reader.KeyState.NEW_KEY;
    }

    @Override
    public void nextRawValue(RawDataBuffer value) throws IOException {
      final RawDataBuffer vb = kvIter.getValue();
      final int vp = vb.getPosition();
      final int vlen = vb.getLength();
      value.reset(vb.getData(), vp, vlen);
    }

    @Override
    public boolean isCurrentRecordStable() {
      return lastNextResult == TezRawKeyValueIterator.NEXT_KEY_VALUE_STABLE;
    }

    @Override
    public void close() throws IOException {
      kvIter.close();
    }

    @Override public long getLength() {
      return size;
    }
  }

  private TezRawKeyValueIterator finalMerge(Configuration job, FileSystem fs,
      List<MapOutput> inMemoryMapOutputs,
      List<FileChunk> onDiskMapOutputs)
    throws IOException, InterruptedException {

    logFinalMergeStart(inMemoryMapOutputs, onDiskMapOutputs);

    // segments required to vacate memory
    List<Segment> memDiskSegments = new ArrayList<Segment>();
    long inMemToDiskBytes = 0;
    if (!inMemoryMapOutputs.isEmpty()) {
      int srcTaskId = inMemoryMapOutputs.get(0).getAttemptIdentifier().getInputIdentifier();
      inMemToDiskBytes = createInMemorySegments(inMemoryMapOutputs, memDiskSegments, this.postMergeMemoryLimitBytes);
      final int numMemDiskSegments = memDiskSegments.size();
      if (numMemDiskSegments > 0 && ioSortFactor > onDiskMapOutputs.size()) {
        
        // If we reach here, it implies that we have less than io.sort.factor
        // disk segments and this will be incremented by 1 (result of the 
        // memory segments merge). Since this total would still be 
        // <= io.sort.factor, we will not do any more intermediate merges,
        // the merge of all these disk segments would be directly fed to the reduce method.
        
        // must spill to disk, but can't retain in-mem for intermediate merge
        // Cannot use spill id in final merge as it would clobber with other files, hence using Integer.MAX_VALUE
        final Path outputPath = mapOutputFile.getInputFileForWrite(
            srcTaskId, Integer.MAX_VALUE, inMemToDiskBytes).suffix(Constants.MERGED_OUTPUT_PREFIX);
        final TezRawKeyValueIterator rIter = TezMerger.merge(job, fs,
            null, memDiskSegments, numMemDiskSegments, 0,
            mapOutputFile, getMergerId(),
            false, spilledRecordsCounter, null,
            additionalSpillBytesRead, true, inputContext);
        final byte[] writeBuffer = IFile.allocateWriteBuffer();
        final WriterDataInputBuffer writer = new WriterDataInputBuffer(fs, outputPath, codec, null, null,
            true, writeBuffer, inputContext);
        try {
          TezMerger.writeFile(rIter, writer,
              TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);
        } catch (IOException e) {
          if (null != outputPath) {
            try {
              fs.delete(outputPath, true);
            } catch (IOException ie) {
              // NOTHING
            }
          }
          throw e;
        } finally {
          if (null != writer) {
            writer.close();
            additionalSpillBytesWritten.increment(writer.getCompressedLength());
          }
        }

        final FileStatus fStatus = localFS.getFileStatus(outputPath);
        // add to list of final disk outputs.
        onDiskMapOutputs.add(new FileChunk(outputPath, 0, fStatus.getLen()));

        if (isDebugEnabled) {
          LOG.debug("MemMerged: Merged " + numMemDiskSegments + "segments, size=" +
              inMemToDiskBytes + " to " + outputPath);
        }

        inMemToDiskBytes = 0;
        memDiskSegments.clear();
      } else if (inMemToDiskBytes != 0) {
        if (isDebugEnabled) {
          LOG.debug("DelayedMemMerge: Keeping " + numMemDiskSegments + " segments, " +
              inMemToDiskBytes + " bytes in memory for " +
              "intermediate, on-disk merge");
        }
      }
    }

    // segments on disk
    List<Segment> diskSegments = new ArrayList<Segment>();
    long onDiskBytes = inMemToDiskBytes;
    FileChunk[] onDisk = onDiskMapOutputs.toArray(new FileChunk[onDiskMapOutputs.size()]);
    for (FileChunk fileChunk : onDisk) {
      final long fileLength = fileChunk.getLength();
      onDiskBytes += fileLength;
      if (isDebugEnabled) {
        LOG.debug("Disk file=" + fileChunk.getPath() + ", len=" + fileLength +
            ", isLocal=" + fileChunk.isLocalFile());
      }

      final Path file = fileChunk.getPath();
      TezCounter counter =
          file.toString().endsWith(Constants.MERGED_OUTPUT_PREFIX) ? null : mergedMapOutputsCounter;

      final long fileOffset = fileChunk.getOffset();
      final boolean preserve = fileChunk.isLocalFile();
      diskSegments.add(new DiskSegment(fs, file, fileOffset, fileLength, codec, ifileReadAhead,
                                   ifileReadAheadLength, preserve, counter, inputContext));
    }
    if (isDebugEnabled) {
      LOG.debug("DiskSeg: Merging " + onDisk.length + " files, " +
          onDiskBytes + " bytes from disk");
    }
    Collections.sort(diskSegments, new Comparator<Segment>() {
      public int compare(Segment o1, Segment o2) {
        if (o1.getLength() == o2.getLength()) {
          return 0;
        }
        return o1.getLength() < o2.getLength() ? -1 : 1;
      }
    });

    // build final list of segments from merged backed by disk + in-mem
    List<Segment> finalSegments = new ArrayList<Segment>();
    long inMemBytes = createInMemorySegments(inMemoryMapOutputs, finalSegments, 0);
    if (isDebugEnabled) {
      LOG.debug("MemSeg: Merging " + finalSegments.size() + " segments, " +
          inMemBytes + " bytes from memory into reduce");
    }

    if (0 != onDiskBytes) {
      final int numInMemSegments = memDiskSegments.size();
      diskSegments.addAll(0, memDiskSegments);
      memDiskSegments.clear();
      TezRawKeyValueIterator diskMerge = TezMerger.merge(job, fs,
          codec, diskSegments, ioSortFactor, numInMemSegments,
          mapOutputFile, getMergerId(),
          false, spilledRecordsCounter, null,
          additionalSpillBytesRead, true, inputContext);
      diskSegments.clear();
      if (finalSegments.isEmpty()) {
        return diskMerge;
      }
      finalSegments.add(new Segment(
            new RawKVIteratorReader(diskMerge, onDiskBytes), null));
    }
    // This is doing nothing but creating an iterator over the segments.
    return TezMerger.merge(job, fs, codec, finalSegments,
        finalSegments.size(), 0,
        mapOutputFile, getMergerId(), false,
        spilledRecordsCounter, null, additionalSpillBytesRead, false,
        inputContext);
  }

  private void logFinalMergeStart(List<MapOutput> inMemoryMapOutputs,
                                  List<FileChunk> onDiskMapOutputs) {
    long inMemSegmentSize = 0;
    for (MapOutput inMemoryMapOutput : inMemoryMapOutputs) {
      inMemSegmentSize += inMemoryMapOutput.getSizeForMergeMemoryAccounting();

      if (isDebugEnabled) {
        LOG.debug("finalMerge: inMemoryOutput=" + inMemoryMapOutput + ", size=" +
            inMemoryMapOutput.getSizeForMergeMemoryAccounting());
      }
    }
    long onDiskSegmentSize = 0;
    for (FileChunk onDiskMapOutput : onDiskMapOutputs) {
      onDiskSegmentSize += onDiskMapOutput.getLength();

      if (isDebugEnabled) {
        LOG.debug("finalMerge: onDiskMapOutput=" + onDiskMapOutput.getPath() +
            ", size=" + onDiskMapOutput.getLength());
      }
    }

    LOG.info(
        "finalMerge with #inMemoryOutputs={}, size={} and #onDiskOutputs={}, size={}",
        inMemoryMapOutputs.size(), inMemSegmentSize, onDiskMapOutputs.size(),
        onDiskSegmentSize);
  }

  // always called inside synchronized (MergeManager) {}
  long getUsedMemory() {
    return this.usedMemory;
  }

  void waitForMemToMemMerge() throws InterruptedException {
    memToMemMerger.waitForMerge();
  }

  private static class SegmentStatsTracker {
    private long size;
    private int count;
    private long minSize;
    private long maxSize;

    SegmentStatsTracker() {
      reset();
    }

    void updateStats(long segSize) {
      size += segSize;
      count++;
      minSize = (segSize < minSize ? segSize : minSize);
      maxSize = (segSize > maxSize ? segSize : maxSize);
    }

    void reset() {
      size = 0L;
      count = 0;
      minSize = Long.MAX_VALUE;
      maxSize = Long.MIN_VALUE;
    }
  }
}
