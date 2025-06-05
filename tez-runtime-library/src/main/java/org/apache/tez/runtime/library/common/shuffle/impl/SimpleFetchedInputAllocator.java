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

package org.apache.tez.runtime.library.common.shuffle.impl;

import java.io.IOException;

import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;
import org.apache.tez.runtime.library.common.shuffle.DiskFetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputCallback;
import org.apache.tez.runtime.library.common.shuffle.MemoryFetchedInput;

/**
 * Usage: Create instance, setInitialMemoryAvailable(long), configureAndStart()
 *
 */
public class SimpleFetchedInputAllocator implements FetchedInputAllocator,
    FetchedInputCallback {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleFetchedInputAllocator.class);
  
  private final Configuration conf;

  private final TezTaskOutputFiles fileNameAllocator;

  // Configuration parameters
  private final long memoryLimit;
  private final long maxSingleMemoryShuffle;

  private final String srcNameTrimmed;
  
  private volatile long usedMemory = 0;

  private final boolean useFreeMemoryFetchedInput;
  private final long freeMemoryThreshold;

  public SimpleFetchedInputAllocator(String srcNameTrimmed,
                                     String uniqueIdentifier, int dagID,
                                     Configuration conf,
                                     long maxTaskAvailableMemory,
                                     long memoryAssigned,
                                     String containerId, int vertexId) {
    this.srcNameTrimmed = srcNameTrimmed;
    this.conf = conf;    
    this.fileNameAllocator = new TezTaskOutputFiles(conf, uniqueIdentifier, dagID, containerId, vertexId,
        ShuffleUtils.isTezShuffleHandler(conf));

    this.memoryLimit = memoryAssigned;

    final float maxSingleShuffleMemoryPercent = conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT_DEFAULT);
    if (maxSingleShuffleMemoryPercent <= 0.0f) {
      throw new IllegalArgumentException("Invalid value for "
          + TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT + ": "
          + maxSingleShuffleMemoryPercent);
    }
    // TODO: currently we must cap to MAX_VALUE because MemoryFetchedInput cannot handle > 2 GB
    this.maxSingleMemoryShuffle = (long) Math.min((memoryLimit * maxSingleShuffleMemoryPercent),
        Integer.MAX_VALUE);

    this.useFreeMemoryFetchedInput = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_FETCHED_INPUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_FETCHED_INPUT_DEFAULT);
    this.freeMemoryThreshold = maxTaskAvailableMemory;
    // TODO: introduce a factor for freeMemoryThreshold (e.g. 0.5)

    LOG.info("{}: memoryLimit={}, maxSingleMemoryShuffle={}",
        srcNameTrimmed, this.memoryLimit, this.maxSingleMemoryShuffle);
  }

  public static long getInitialMemoryReq(Configuration conf, long maxAvailableTaskMemory) {
    final float maxInMemCopyUse = conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT_DEFAULT);
    if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
      throw new IllegalArgumentException("Invalid value for "
          + TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT + ": "
          + maxInMemCopyUse);
    }
    long memReq = (long)(Math.min(maxAvailableTaskMemory, Integer.MAX_VALUE) * maxInMemCopyUse);
    return memReq;
  }

  @Override
  public synchronized FetchedInput allocate(long actualSize, long compressedSize,
      InputAttemptIdentifier inputAttemptIdentifier) throws IOException {
    if (actualSize > maxSingleMemoryShuffle) {
      LOG.info("Creating DiskFetchedInput: {} > maxSingleMemoryShuffle", actualSize);
      return new DiskFetchedInput(compressedSize,
          inputAttemptIdentifier, this, conf, fileNameAllocator);
    }
    if (this.usedMemory + actualSize > this.memoryLimit) {
      // This Task has used up all its memory (memoryLimit).
      if (!useFreeMemoryFetchedInput) {
        LOG.info("Creating DiskFetchedInput: {} + {} > memoryLimit", this.usedMemory, actualSize);
        return new DiskFetchedInput(compressedSize,
            inputAttemptIdentifier, this, conf, fileNameAllocator);
      }
      // Check if we can find free memory in the current ContainerWorker.
      long currentFreeMemory = Runtime.getRuntime().freeMemory();
      if (currentFreeMemory < freeMemoryThreshold) {
        // this ContainerWorker is busy serving Tasks, so do not borrow
        LOG.info("Creating DiskFetchedInput: {}, {} < freeMemoryThreshold", actualSize, currentFreeMemory);
        return new DiskFetchedInput(compressedSize,
            inputAttemptIdentifier, this, conf,
            fileNameAllocator);
      }
      this.usedMemory += actualSize;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating MemoryFetchedInput in free memory: {}, {}, {}", this.usedMemory, actualSize, currentFreeMemory);
      }
    } else {
      this.usedMemory += actualSize;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating MemoryFetchedInput: {}, {}", this.usedMemory, actualSize);
      }
    }
    return new MemoryFetchedInput(actualSize, inputAttemptIdentifier, this);
  }

  @Override
  public synchronized void fetchComplete(FetchedInput fetchedInput) {
    switch (fetchedInput.getType()) {
    // Not tracking anything here.
    case DISK:
    case DISK_DIRECT:
    case MEMORY:
      break;
    default:
      throw new TezUncheckedException("InputType: " + fetchedInput.getType()
          + " not expected for Broadcast fetch");
    }
  }

  @Override
  public synchronized void fetchFailed(FetchedInput fetchedInput) {
    cleanup(fetchedInput);
  }

  @Override
  public synchronized void freeResources(FetchedInput fetchedInput) {
    cleanup(fetchedInput);
  }

  private void cleanup(FetchedInput fetchedInput) {
    switch (fetchedInput.getType()) {
    case DISK:
      break;
    case MEMORY:
      unreserve(((MemoryFetchedInput) fetchedInput).getSize());
      break;
    default:
      throw new TezUncheckedException("InputType: " + fetchedInput.getType()
          + " not expected for Broadcast fetch");
    }
  }

  private synchronized void unreserve(long size) {
    this.usedMemory -= size;
    if (LOG.isDebugEnabled()) {
      LOG.debug(srcNameTrimmed + ": " + "Used memory after freeing " + size  + " : " + usedMemory);
    }
  }

}
