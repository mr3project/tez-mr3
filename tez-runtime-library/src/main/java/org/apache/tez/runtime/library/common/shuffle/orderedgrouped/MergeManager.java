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
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.FileChunk;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progressable;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.combine.Combiner;
import org.apache.tez.runtime.library.common.serializer.SerializationContext;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger.DiskSegment;
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

  private final Configuration conf;
  private final FileSystem localFS;
  private final FileSystem rfs;
  private final LocalDirAllocator localDirAllocator;
  
  private final TezTaskOutputFiles mapOutputFile;
  private final Progressable progressable = new Progressable() {
    @Override
    public void progress() {
    }
  };
  private final Combiner combiner;  
  
  final Set<MapOutput> inMemoryMergedMapOutputs =
    new TreeSet<MapOutput>(new MapOutput.MapOutputComparator());
  private final IntermediateMemoryToMemoryMerger memToMemMerger;

  final Set<MapOutput> inMemoryMapOutputs =
    new TreeSet<MapOutput>(new MapOutput.MapOutputComparator());
  private final InMemoryMerger inMemoryMerger;

  final Set<FileChunk> onDiskMapOutputs = new TreeSet<FileChunk>();
  final OnDiskMerger onDiskMerger;
  
  private final long memoryLimit;
  final long postMergeMemLimit;
  private long usedMemory;
  private long commitMemory;
  private final int ioSortFactor;
  private final long maxSingleShuffleLimit;

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  private final int memToMemMergeOutputsThreshold; 
  private final long mergeThreshold;
  
  private final ExceptionReporter exceptionReporter;
  
  private final InputContext inputContext;

  private final TezCounter spilledRecordsCounter;
  private final TezCounter mergedMapOutputsCounter;
  
  private final TezCounter numMemToDiskMerges;
  private final TezCounter numDiskToDiskMerges;
  private final TezCounter additionalBytesWritten;
  private final TezCounter additionalBytesRead;
  
  private final CompressionCodec codec;
  
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;

  // Variables for stats
  private final SegmentStatsTracker statsInMemTotal = new SegmentStatsTracker();

  private final AtomicInteger mergeFileSequenceId = new AtomicInteger(0);

  private final boolean cleanup;

  private final SerializationContext serializationContext;

  private final boolean useFreeMemoryFetchedInput;
  private final long freeMemoryThreshold;

  /**
   * Construct the MergeManager. Must call start before it becomes usable.
   */
  public MergeManager(Configuration conf,
                      FileSystem localFS,
                      LocalDirAllocator localDirAllocator,  
                      InputContext inputContext,
                      Combiner combiner,
                      TezCounter spilledRecordsCounter,
                      TezCounter mergedMapOutputsCounter,
                      ExceptionReporter exceptionReporter,
                      long memoryAssigned,
                      CompressionCodec codec,
                      boolean ifileReadAheadEnabled,
                      int ifileReadAheadLength) {
    this.inputContext = inputContext;
    this.conf = conf;
    this.localDirAllocator = localDirAllocator;
    this.exceptionReporter = exceptionReporter;

    this.combiner = combiner;

    this.spilledRecordsCounter = spilledRecordsCounter;
    this.mergedMapOutputsCounter = mergedMapOutputsCounter;

    boolean compositeFetch = ShuffleUtils.isTezShuffleHandler(conf);
    this.mapOutputFile = new TezTaskOutputFiles(conf,
        inputContext.getUniqueIdentifier(),
        inputContext.getDagIdentifier(),
        inputContext.getExecutionContext().getContainerId(),
        inputContext.getTaskVertexIndex(),
        compositeFetch);

    this.localFS = localFS;
    this.rfs = ((LocalFileSystem)localFS).getRaw();
    
    this.numDiskToDiskMerges = inputContext.getCounters().findCounter(TaskCounter.NUM_DISK_TO_DISK_MERGES);
    this.numMemToDiskMerges = inputContext.getCounters().findCounter(TaskCounter.NUM_MEM_TO_DISK_MERGES);
    this.additionalBytesWritten = inputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_WRITTEN);
    this.additionalBytesRead = inputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_READ);

    this.cleanup = conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT,
        TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT_DEFAULT);

    this.codec = codec;
    this.ifileReadAhead = ifileReadAheadEnabled;
    if (this.ifileReadAhead) {
      this.ifileReadAheadLength = ifileReadAheadLength;
    } else {
      this.ifileReadAheadLength = 0;
    }

    // Figure out initial memory req start
    final float maxInMemCopyUse = conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT_DEFAULT);
    if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
      throw new IllegalArgumentException("Invalid value for " +
          TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT + ": " +
          maxInMemCopyUse);
    }

    // Allow unit tests to fix Runtime memory
    long memLimit = (long)(inputContext.getTotalMemoryAvailableToTask() * maxInMemCopyUse);

    float maxRedPer = conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_BUFFER_PERCENT_DEFAULT);
    if (maxRedPer > 1.0 || maxRedPer < 0.0) {
      throw new TezUncheckedException(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT + maxRedPer);
    }

    long maxRedBuffer = (long) (inputContext.getTotalMemoryAvailableToTask() * maxRedPer);
    // Figure out initial memory req end
    
    if (memoryAssigned < memLimit) {
      this.memoryLimit = memoryAssigned;
    } else {
      this.memoryLimit = memLimit;
    }
    
    if (memoryAssigned < maxRedBuffer) {
      this.postMergeMemLimit = memoryAssigned;
    } else {
      this.postMergeMemLimit = maxRedBuffer;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          inputContext.getSourceVertexName() + ": " + "InitialRequest: ShuffleMem=" + memLimit +
              ", postMergeMem=" + maxRedBuffer
              + ", RuntimeTotalAvailable=" + memoryAssigned +
              ". Updated to: ShuffleMem="
              + this.memoryLimit + ", postMergeMem: " + this.postMergeMemLimit);
    }

    this.ioSortFactor = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR,
        TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR_DEFAULT);
    
    final float singleShuffleMemoryLimitPercent = conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT_DEFAULT);
    if (singleShuffleMemoryLimitPercent <= 0.0f
        || singleShuffleMemoryLimitPercent > 1.0f) {
      throw new IllegalArgumentException("Invalid value for "
          + TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT + ": "
          + singleShuffleMemoryLimitPercent);
    }

    //TODO: Cap it to MAX_VALUE until MapOutput starts supporting > 2 GB
    this.maxSingleShuffleLimit = (long) Math.min((memoryLimit * singleShuffleMemoryLimitPercent), Integer.MAX_VALUE);
    this.memToMemMergeOutputsThreshold = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMTOMEM_SEGMENTS, ioSortFactor);
    this.mergeThreshold = (long)(this.memoryLimit * conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT_DEFAULT));
    if (LOG.isDebugEnabled()) {
      LOG.debug(inputContext.getSourceVertexName() + ": MergerManager: memoryLimit=" + memoryLimit + ", " +
               "maxSingleShuffleLimit=" + maxSingleShuffleLimit + ", " +
               "mergeThreshold=" + mergeThreshold + ", " +
               "ioSortFactor=" + ioSortFactor + ", " +
               "postMergeMem=" + postMergeMemLimit + ", " +
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

    this.serializationContext = new SerializationContext(conf);

    this.useFreeMemoryFetchedInput = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_FETCHED_INPUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_FETCHED_INPUT_DEFAULT);
    this.freeMemoryThreshold = inputContext.getTotalMemoryAvailableToTask();
    // TODO: introduce a factor for freeMemoryThreshold (e.g. 0.5)
  }

  void setupParentThread(Thread shuffleSchedulerThread) {
    LOG.info("Setting merger's parent thread to " + shuffleSchedulerThread.getName());
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
  static long getInitialMemoryRequirement(Configuration conf, long maxAvailableTaskMemory) {
    final float maxInMemCopyUse =
        conf.getFloat(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT_DEFAULT);
      if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
        throw new IllegalArgumentException("Invalid value for " +
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT + ": " +
            maxInMemCopyUse);
      }

      // Allow unit tests to fix Runtime memory
      long memLimit = (long)(maxAvailableTaskMemory * maxInMemCopyUse);
      
      float maxRedPer = conf.getFloat(
          TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT,
          TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_BUFFER_PERCENT_DEFAULT);
      if (maxRedPer > 1.0 || maxRedPer < 0.0) {
        throw new TezUncheckedException(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT + maxRedPer);
      }
      long maxRedBuffer = (long) (maxAvailableTaskMemory * maxRedPer);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Initial Memory required for SHUFFLE_BUFFER=" + memLimit +
            " based on INPUT_BUFFER_FACTOR=" + maxInMemCopyUse + ",  for final merged output=" +
            maxRedBuffer + ", using factor: " + maxRedPer);
      }

      long reqMem = Math.max(maxRedBuffer, memLimit);
      return reqMem;
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
      if (commitMemory >= mergeThreshold) {
        startMemToDiskMerge();
        triggerAdditionalMerge = true;
      }
    }
    if (triggerAdditionalMerge) {
      inMemoryMerger.waitForMerge();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Additional in-memory merge triggered");
      }
    }
  }

  private boolean canShuffleToMemory(long requestedSize) {
    return (requestedSize < maxSingleShuffleLimit);
  }

  public synchronized void waitForShuffleToMergeMemory() throws InterruptedException {
    long startTime = System.currentTimeMillis();
    while(usedMemory > memoryLimit) {
      wait();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Waited for " + (System.currentTimeMillis() - startTime) + " for memory to become available");
    }
  }

  final private MapOutput stallShuffle = MapOutput.createWaitMapOutput(null);

  @Override
  public MapOutput reserve(
      InputAttemptIdentifier srcAttemptIdentifier,
      long requestedSize,
      long compressedLength,
      int fetcher) throws IOException {
    if (!canShuffleToMemory(requestedSize)) {
      LOG.info("Creating DiskMapOutput for {}: {} > maxSingleShuffleLimit", srcAttemptIdentifier, requestedSize);
      return MapOutput.createDiskMapOutput(srcAttemptIdentifier, this, compressedLength, conf,
          fetcher, true, mapOutputFile);
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
    // (usedMemory + requestedSize > memoryLimit). When this thread is done
    // fetching, this will automatically trigger a merge thereby unlocking
    // all the stalled threads

    synchronized (this) {
      if (usedMemory > memoryLimit) {
        if (!useFreeMemoryFetchedInput) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(srcAttemptIdentifier + ": Stalling shuffle since usedMemory (" + usedMemory
                + ") is greater than memoryLimit (" + memoryLimit + ")." +
                " CommitMemory is (" + commitMemory + ")");
          }
          return stallShuffle;
        }
        // Check if we can find free memory in the current ContainerWorker
        long currentFreeMemory = Runtime.getRuntime().freeMemory();
        if (currentFreeMemory < freeMemoryThreshold){
          // this ContainerWorker is busy serving Tasks, so do not borrow
          return stallShuffle;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Creating MemoryMapOutput in free memory: {}, {}, CommitMemory={}",
              usedMemory, currentFreeMemory, commitMemory);
        }
      } else {
        // Allow the in-memory shuffle to progress
        if (LOG.isDebugEnabled()) {
          LOG.debug("Creating MemoryMapOutput: {}, {}, CommitMemory={}",
              usedMemory, memoryLimit, commitMemory);
        }
      }
      return unconditionalReserve(srcAttemptIdentifier, requestedSize, true);
    }
  }

  /**
   * Unconditional Reserve is used by the Memory-to-Memory thread
   */
  private synchronized MapOutput unconditionalReserve(
      InputAttemptIdentifier srcAttemptIdentifier,
      long requestedSize,
      boolean primaryMapOutput) {
    usedMemory += requestedSize;
    return MapOutput.createMemoryMapOutput(srcAttemptIdentifier, this, (int) requestedSize, primaryMapOutput);
  }

  @Override
  public synchronized void unreserve(long size) {
    usedMemory -= size;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Notifying unreserve : size=" + size + ", commitMemory=" + commitMemory + ", usedMemory=" + usedMemory
          + ", mergeThreshold=" + mergeThreshold);
    }
    notifyAll();
  }

  @Override
  public synchronized void releaseCommittedMemory(long size) {
    commitMemory -= size;
    unreserve(size);
  }


  @Override
  public synchronized void closeInMemoryFile(MapOutput mapOutput) { 
    inMemoryMapOutputs.add(mapOutput);
    trackAndLogCloseInMemoryFile(mapOutput);

    commitMemory += mapOutput.getSize();

    if (commitMemory >= mergeThreshold) {
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
    statsInMemTotal.updateStats(mapOutput.getSize());

    if (LOG.isDebugEnabled()) {
      LOG.debug("closeInMemoryFile -> map-output of size: " + mapOutput.getSize()
          + ", inMemoryMapOutputs.size() -> " + inMemoryMapOutputs.size()
          + ", commitMemory -> " + commitMemory + ", usedMemory ->" +
          usedMemory + ", mapOutput=" +
          mapOutput);
    }
  }

  private void startMemToDiskMerge() {
    synchronized (inMemoryMerger) {
      if (!inMemoryMerger.isInProgress()) {
        LOG.info("{}: Starting inMemoryMerger's merge since commitMemory={} > mergeThreshold={}. Current usedMemory={}",
            inputContext.getSourceVertexName(), commitMemory, mergeThreshold, usedMemory);
        inMemoryMapOutputs.addAll(inMemoryMergedMapOutputs);
        inMemoryMergedMapOutputs.clear();
        inMemoryMerger.startMerge(inMemoryMapOutputs);
      }
    }
  }
  
  public synchronized void closeInMemoryMergedFile(MapOutput mapOutput) {
    inMemoryMergedMapOutputs.add(mapOutput);
    if (LOG.isDebugEnabled()) {
      // This log could be moved to INFO level for a while, after mem-to-mem
      // merge is production ready.
      LOG.debug("closeInMemoryMergedFile -> size: " + mapOutput.getSize() +
          ", inMemoryMergedMapOutputs.size() -> " +
          inMemoryMergedMapOutputs.size());
    }

    commitMemory += mapOutput.getSize();

    if (commitMemory >= mergeThreshold) {
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
    if (LOG.isDebugEnabled()) {
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

      if (statsInMemTotal.count > 0) {
        LOG.info(
            "TotalInMemFetchStats: count={}, totalSize={}, min={}, max={}, avg={}",
            statsInMemTotal.count, statsInMemTotal.size,
            statsInMemTotal.minSize, statsInMemTotal.maxSize,
            (statsInMemTotal.size / (float) statsInMemTotal.count));
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
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting " + path);
      }
      fs.delete(path, true);
    } catch (IOException e) {
      LOG.info("Error in deleting " + path);
    }
  }

  void runCombineProcessor(TezRawKeyValueIterator kvIter, Writer writer)
      throws IOException, InterruptedException {
    combiner.combine(kvIter, writer);
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
    public void merge(List<MapOutput> inputs) throws IOException, InterruptedException {
      if (inputs == null || inputs.size() == 0) {
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
        while(it.hasNext() && !Thread.currentThread().isInterrupted()) {
          MapOutput mo = it.next();
          if ((mergeOutputSize + mo.getSize() + manager.getUsedMemory()) > memoryLimit) {
            //Search for smaller segments that can fit into existing mem
            if (LOG.isDebugEnabled()) {
              LOG.debug("Size is greater than usedMemory. "
                  + "mergeOutputSize=" + mergeOutputSize
                  + ", moSize=" + mo.getSize()
                  + ", usedMemory=" + manager.getUsedMemory()
                  + ", memoryLimit=" + memoryLimit);
            }
          } else {
            mergeOutputSize += mo.getSize();
            IFile.Reader reader = new InMemoryReader(MergeManager.this,
                mo.getAttemptIdentifier(), mo.getMemory(), 0, mo.getMemory().length);
            inMemorySegments.add(new Segment(reader,
                (mo.isPrimaryMapOutput() ? mergedMapOutputsCounter : null)));
            lastAddedMapOutput = mo;
            it.remove();
            if (LOG.isDebugEnabled()) {
              LOG.debug("Added segment for merging. mergeOutputSize=" + mergeOutputSize);
            }
          }
        }

        //Add any unused MapOutput back
        inMemoryMapOutputs.addAll(inputs);

        //Exit early, if 0 or 1 segment is available
        if (inMemorySegments.size() <= 1) {
          if (lastAddedMapOutput != null) {
            inMemoryMapOutputs.add(lastAddedMapOutput);
          }
          return;
        }

        mergedMapOutputs = unconditionalReserve(dummyMapId, mergeOutputSize, false);
      }

      int noInMemorySegments = inMemorySegments.size();

      IFile.WriterAppend writer = new InMemoryWriter(mergedMapOutputs.getMemory());

      LOG.info("{}: Initiating Memory-to-Memory merge with {} segments of total-size: {}",
          inputContext.getSourceVertexName(), noInMemorySegments, mergeOutputSize);

      if (Thread.currentThread().isInterrupted()) {
        return; // early exit
      }

      // Nothing will be materialized to disk because the sort factor is being
      // set to the number of in memory segments.
      // TODO Is this doing any combination ?
      TezRawKeyValueIterator rIter = 
        TezMerger.merge(conf, rfs,
                       serializationContext,
                       inMemorySegments, inMemorySegments.size(),
                       new Path(inputContext.getUniqueIdentifier()),
                       (RawComparator)ConfigUtils.getIntermediateInputKeyComparator(conf),
                       progressable, null, null, null, null, inputContext);
      TezMerger.writeFile(rIter, writer, progressable, TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);
      writer.close();

      LOG.info("{} Memory-to-Memory merge of the {} files in-memory complete with mergeOutputSize={}",
          inputContext.getSourceVertexName(), noInMemorySegments, mergeOutputSize);

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
    volatile Path tmpDir;

    private final byte[] writeBuffer;

    public InMemoryMerger(MergeManager manager) {
      super(manager, Integer.MAX_VALUE, exceptionReporter);
      setName("MemtoDiskMerger [" + inputContext.getSourceVertexName() + "_" + inputContext.getUniqueIdentifier()  + "]");
      setDaemon(true);
      writeBuffer = IFile.allocateWriteBuffer();
    }
    
    @Override
    public void merge(List<MapOutput> inputs) throws IOException, InterruptedException {
      if (inputs == null || inputs.size() == 0) {
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

      Writer writer = null;
      long outFileLen = 0;
      try {
        writer = new Writer(serializationContext.getKeySerialization(),
            serializationContext.getValSerialization(), rfs, outputPath,
            serializationContext.getKeyClass(), serializationContext.getValueClass(), codec,
            null, null, writeBuffer);

        TezRawKeyValueIterator rIter = null;
        LOG.info("Initiating in-memory merge with {} segments", noInMemorySegments);

        tmpDir = new Path(inputContext.getUniqueIdentifier());
        // Nothing actually materialized to disk - controlled by setting sort-factor to #segments.
        rIter = TezMerger.merge(conf, rfs,
            serializationContext,
            inMemorySegments, inMemorySegments.size(),
            tmpDir, (RawComparator)ConfigUtils.getIntermediateInputKeyComparator(conf),
            progressable, spilledRecordsCounter, null, additionalBytesRead, null, inputContext);
        // spilledRecordsCounter is tracking the number of keys that will be
        // read from each of the segments being merged - which is essentially
        // what will be written to disk.

        if (null == combiner) {
          TezMerger.writeFile(rIter, writer, progressable, TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);
        } else {
          runCombineProcessor(rIter, writer);
        }
        writer.close();
        additionalBytesWritten.increment(writer.getCompressedLength());
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
    public void cleanup(List<MapOutput> inputs, boolean deleteData)
        throws IOException, InterruptedException {
      if (deleteData) {
        //Additional check at task level
        if (cleanup) {
          LOG.info("Try deleting stale data");
          MergeManager.cleanup(localFS, outputPath);
          MergeManager.cleanup(localFS, tmpDir);
        }
      }
    }
  }

  /**
   * Merges multiple on-disk segments
   */
  class OnDiskMerger extends MergeThread<FileChunk> {

    volatile Path outputPath;
    volatile Path tmpDir;

    private final byte[] writeBuffer;

    public OnDiskMerger(MergeManager manager) {
      super(manager, ioSortFactor, exceptionReporter);
      setName("DiskToDiskMerger [" +  inputContext.getSourceVertexName() + "_" + inputContext.getUniqueIdentifier() + "]");
      setDaemon(true);
      writeBuffer = IFile.allocateWriteBuffer();
    }
    
    @Override
    public void merge(List<FileChunk> inputs) throws IOException, InterruptedException {
      // sanity check
      if (inputs == null || inputs.isEmpty()) {
        LOG.info("No ondisk files to merge...");
        return;
      }
      numDiskToDiskMerges.increment(1);

      long approxOutputSize = 0;
      int bytesPerSum = 
        conf.getInt("io.bytes.per.checksum", 512);
      
      LOG.info("OnDiskMerger: We have {} map outputs on disk. Triggering merge...", inputs.size());

      List<Segment> inputSegments = new ArrayList<Segment>(inputs.size());

      // 1. Prepare the list of files to be merged.
      for (FileChunk fileChunk : inputs) {
        final long offset = fileChunk.getOffset();
        final long size = fileChunk.getLength();
        final boolean preserve = fileChunk.isLocalFile();
        if (LOG.isDebugEnabled()) {
          LOG.debug("InputAttemptIdentifier=" + fileChunk.getInputAttemptIdentifier()
              + ", len=" + fileChunk.getLength() + ", offset=" + fileChunk.getOffset()
              + ", path=" + fileChunk.getPath());
        }
        final Path file = fileChunk.getPath();
        approxOutputSize += size;
        DiskSegment segment = new DiskSegment(rfs, file, offset, size, codec, ifileReadAhead,
            ifileReadAheadLength, preserve, inputContext);
        inputSegments.add(segment);
      }

      // add the checksum length
      approxOutputSize += 
        ChecksumFileSystem.getChecksumLength(approxOutputSize, bytesPerSum);

      // 2. Start the on-disk merge process
      FileChunk file0 = inputs.get(0);
      String namePart;
      String outputPathString;
      if (file0.isLocalFile()) {
        // This is setup the same way a type DISK MapOutput is setup when fetching.
        namePart = mapOutputFile.getSpillFileName(
            file0.getInputAttemptIdentifier().getInputIdentifier(),
            file0.getInputAttemptIdentifier().getSpillEventId());
      } else {
        namePart = file0.getPath().getName().toString();
      }

      // namePart includes the suffix of the file. We need to remove it.
      namePart = FilenameUtils.removeExtension(namePart);
      outputPathString = mapOutputFile.getDagOutputDir(namePart);
      outputPath = localDirAllocator.getLocalPathForWrite(outputPathString, approxOutputSize, conf);
      outputPath = outputPath.suffix(Constants.MERGED_OUTPUT_PREFIX + mergeFileSequenceId.getAndIncrement());

      Writer writer = new Writer(serializationContext.getKeySerialization(),
          serializationContext.getValSerialization(), rfs, outputPath,
          serializationContext.getKeyClass(), serializationContext.getValueClass(), codec, null,
          null, writeBuffer);
      tmpDir = new Path(inputContext.getUniqueIdentifier());
      try {
        TezRawKeyValueIterator iter = TezMerger.merge(conf, rfs,
            serializationContext,
            inputSegments,
            ioSortFactor, tmpDir,
            (RawComparator)ConfigUtils.getIntermediateInputKeyComparator(conf),
            progressable, true, spilledRecordsCounter, null,
            mergedMapOutputsCounter, null, inputContext);

        // TODO Maybe differentiate between data written because of Merges and
        // the finalMerge (i.e. final mem available may be different from initial merge mem)
        TezMerger.writeFile(iter, writer, progressable, TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);
        writer.close();
        additionalBytesWritten.increment(writer.getCompressedLength());
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
    public void cleanup(List<FileChunk> inputs, boolean deleteData) throws IOException,
        InterruptedException {
      if (deleteData) {
        //Additional check at task level
        if (cleanup) {
          LOG.info("Try deleting stale data");
          MergeManager.cleanup(localFS, inputs);
          MergeManager.cleanup(localFS, outputPath);
          MergeManager.cleanup(localFS, tmpDir);
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
      fullSize += mo.getSize();
    }
    int inMemoryMapOutputsOffset = 0;
    while((fullSize > leaveBytes) && !Thread.currentThread().isInterrupted()) {
      MapOutput mo = inMemoryMapOutputs.get(inMemoryMapOutputsOffset++);
      byte[] data = mo.getMemory();
      long size = data.length;
      totalSize += size;
      fullSize -= size;
      IFile.Reader reader = new InMemoryReader(MergeManager.this, 
                                                   mo.getAttemptIdentifier(),
                                                   data, 0, (int)size);
      inMemorySegments.add(new Segment(reader,
                                            (mo.isPrimaryMapOutput() ? 
                                            mergedMapOutputsCounter : null)));
    }
    // Bulk remove removed in-memory map outputs efficiently
    inMemoryMapOutputs.subList(0, inMemoryMapOutputsOffset).clear();
    return totalSize;
  }

  class RawKVIteratorReader extends IFile.Reader {

    private final TezRawKeyValueIterator kvIter;
    private final long size;

    public RawKVIteratorReader(TezRawKeyValueIterator kvIter, long size)
        throws IOException {
      super(null, size, null, spilledRecordsCounter, null, ifileReadAhead,
          ifileReadAheadLength, inputContext);
      this.kvIter = kvIter;
      this.size = size;
    }

    @Override
    public KeyState readRawKey(DataInputBuffer key) throws IOException {
      if (kvIter.next()) {
        final DataInputBuffer kb = kvIter.getKey();
        final int kp = kb.getPosition();
        final int klen = kb.getLength() - kp;
        key.reset(kb.getData(), kp, klen);
        bytesRead += klen;
        return kvIter.isSameKey() ? KeyState.SAME_KEY : KeyState.NEW_KEY;
      }
      return KeyState.NO_KEY;
    }

    public void nextRawValue(DataInputBuffer value) throws IOException {
      final DataInputBuffer vb = kvIter.getValue();
      final int vp = vb.getPosition();
      final int vlen = vb.getLength() - vp;
      value.reset(vb.getData(), vp, vlen);
      bytesRead += vlen;
    }

    public long getPosition() throws IOException {
      return bytesRead;
    }

    public void close() throws IOException {
      kvIter.close();
    }

    @Override public long getLength() {
      return size;
    }
  }

  private TezRawKeyValueIterator finalMerge(Configuration job, FileSystem fs,
                                       List<MapOutput> inMemoryMapOutputs,
                                       List<FileChunk> onDiskMapOutputs
                                       ) throws IOException, InterruptedException {

    logFinalMergeStart(inMemoryMapOutputs, onDiskMapOutputs);
    StringBuilder finalMergeLog = new StringBuilder();
    
    // merge config params
    SerializationContext serContext = new SerializationContext(job);
    final Path tmpDir = new Path(inputContext.getUniqueIdentifier());
    final RawComparator comparator =
      (RawComparator)ConfigUtils.getIntermediateInputKeyComparator(job);

    // segments required to vacate memory
    List<Segment> memDiskSegments = new ArrayList<Segment>();
    long inMemToDiskBytes = 0;
    if (!inMemoryMapOutputs.isEmpty()) {
      int srcTaskId = inMemoryMapOutputs.get(0).getAttemptIdentifier().getInputIdentifier();
      inMemToDiskBytes = createInMemorySegments(inMemoryMapOutputs, memDiskSegments, this.postMergeMemLimit);
      final int numMemDiskSegments = memDiskSegments.size();
      if (numMemDiskSegments > 0 && ioSortFactor > onDiskMapOutputs.size()) {
        
        // If we reach here, it implies that we have less than io.sort.factor
        // disk segments and this will be incremented by 1 (result of the 
        // memory segments merge). Since this total would still be 
        // <= io.sort.factor, we will not do any more intermediate merges,
        // the merge of all these disk segments would be directly fed to the
        // reduce method
        
        // must spill to disk, but can't retain in-mem for intermediate merge
        // Can not use spill id in final merge as it would clobber with other files, hence using
        // Integer.MAX_VALUE
        final Path outputPath = 
          mapOutputFile.getInputFileForWrite(srcTaskId, Integer.MAX_VALUE,
              inMemToDiskBytes).suffix(Constants.MERGED_OUTPUT_PREFIX);
        final TezRawKeyValueIterator rIter = TezMerger.merge(job, fs, serContext,
            memDiskSegments, numMemDiskSegments, tmpDir, comparator, progressable,
            spilledRecordsCounter, null, additionalBytesRead, null, inputContext);
        final byte[] writeBuffer = IFile.allocateWriteBuffer();
        final Writer writer = new Writer(serContext.getKeySerialization(),
            serContext.getValSerialization(), fs, outputPath, serContext.getKeyClass(),
            serContext.getValueClass(), codec, null, null, writeBuffer);
        try {
          TezMerger.writeFile(rIter, writer, progressable, TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);
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
            additionalBytesWritten.increment(writer.getCompressedLength());
          }
        }

        final FileStatus fStatus = localFS.getFileStatus(outputPath);
        // add to list of final disk outputs.
        onDiskMapOutputs.add(new FileChunk(outputPath, 0, fStatus.getLen()));

        if (LOG.isInfoEnabled()) {
          finalMergeLog.append("MemMerged: " + numMemDiskSegments + ", " + inMemToDiskBytes);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Merged " + numMemDiskSegments + "segments, size=" +
                inMemToDiskBytes + " to " + outputPath);
          }
        }

        inMemToDiskBytes = 0;
        memDiskSegments.clear();
      } else if (inMemToDiskBytes != 0) {
        if (LOG.isInfoEnabled()) {
          finalMergeLog.append("DelayedMemMerge: " + numMemDiskSegments + ", " + inMemToDiskBytes);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Keeping " + numMemDiskSegments + " segments, " +
                inMemToDiskBytes + " bytes in memory for " +
                "intermediate, on-disk merge");
          }
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
      if (LOG.isDebugEnabled()) {
        LOG.debug("Disk file=" + fileChunk.getPath() + ", len=" + fileLength +
            ", isLocal=" +
            fileChunk.isLocalFile());
      }

      final Path file = fileChunk.getPath();
      TezCounter counter =
          file.toString().endsWith(Constants.MERGED_OUTPUT_PREFIX) ? null : mergedMapOutputsCounter;

      final long fileOffset = fileChunk.getOffset();
      final boolean preserve = fileChunk.isLocalFile();
      diskSegments.add(new DiskSegment(fs, file, fileOffset, fileLength, codec, ifileReadAhead,
                                   ifileReadAheadLength, preserve, counter, inputContext));
    }
    if (LOG.isInfoEnabled()) {
      finalMergeLog.append(". DiskSeg: " + onDisk.length + ", " + onDiskBytes);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Merging " + onDisk.length + " files, " +
            onDiskBytes + " bytes from disk");
      }
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
    if (LOG.isInfoEnabled()) {
      finalMergeLog.append(". MemSeg: " + finalSegments.size() + ", " + inMemBytes);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Merging " + finalSegments.size() + " segments, " +
            inMemBytes + " bytes from memory into reduce");
      }
    }

    if (0 != onDiskBytes) {
      final int numInMemSegments = memDiskSegments.size();
      diskSegments.addAll(0, memDiskSegments);
      memDiskSegments.clear();
      TezRawKeyValueIterator diskMerge = TezMerger.merge(
          job, fs, serContext, codec, diskSegments,
          ioSortFactor, numInMemSegments, tmpDir, comparator,
          progressable, false, spilledRecordsCounter, null, additionalBytesRead, null, inputContext);
      diskSegments.clear();
      if (0 == finalSegments.size()) {
        return diskMerge;
      }
      finalSegments.add(new Segment(
            new RawKVIteratorReader(diskMerge, onDiskBytes), null));
    }
    if (LOG.isInfoEnabled()) {
      LOG.info(finalMergeLog.toString());
    }
    // This is doing nothing but creating an iterator over the segments.
    return TezMerger.merge(job, fs, serContext, codec,
        finalSegments, finalSegments.size(), tmpDir,
        comparator, progressable, spilledRecordsCounter, null,
        additionalBytesRead, null, inputContext);
  }

  private void logFinalMergeStart(List<MapOutput> inMemoryMapOutputs,
                                  List<FileChunk> onDiskMapOutputs) {
    long inMemSegmentSize = 0;
    for (MapOutput inMemoryMapOutput : inMemoryMapOutputs) {
      inMemSegmentSize += inMemoryMapOutput.getSize();

      if (LOG.isDebugEnabled()) {
        LOG.debug("finalMerge: inMemoryOutput=" + inMemoryMapOutput + ", size=" +
            inMemoryMapOutput.getSize());
      }
    }
    long onDiskSegmentSize = 0;
    for (FileChunk onDiskMapOutput : onDiskMapOutputs) {
      onDiskSegmentSize += onDiskMapOutput.getLength();

      if (LOG.isDebugEnabled()) {
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
    return usedMemory;
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
