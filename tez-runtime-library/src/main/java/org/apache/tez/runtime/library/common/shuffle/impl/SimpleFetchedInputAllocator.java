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
import java.util.concurrent.atomic.AtomicLong;

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
public class SimpleFetchedInputAllocator implements FetchedInputAllocator, FetchedInputCallback {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleFetchedInputAllocator.class);
  
  private final Configuration conf;

  private final TezTaskOutputFiles fileNameAllocator;

  private final long memoryLimit;   // memory assigned to this LogicalInput
  private final long maxSingleMemoryShuffle;

  private final String srcNameTrimmed;
  
  private AtomicLong usedMemory = new AtomicLong(0L);

  private final boolean useFreeMemoryFetchedInput;
  private final long freeMemoryThreshold;   // minimum size of free memory for useFreeMemoryFetchedInput
  private final long freeMemoryLimit;       // free memory that can be assigned to this LogicalInput

  private final boolean shuffleMemoryStreaming;

  public SimpleFetchedInputAllocator(String srcNameTrimmed,
                                     String uniqueIdentifier, int dagID,
                                     Configuration conf,
                                     long maxTaskAvailableMemory,
                                     long memoryAssigned,
                                     String containerId, int vertexId,
                                     boolean compositeFetch) {
    this.srcNameTrimmed = srcNameTrimmed;
    this.conf = conf;    
    this.fileNameAllocator = new TezTaskOutputFiles(
        conf, uniqueIdentifier, dagID, containerId, vertexId, compositeFetch);

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
    this.maxSingleMemoryShuffle = (long) Math.min((memoryLimit * maxSingleShuffleMemoryPercent), Integer.MAX_VALUE);

    this.useFreeMemoryFetchedInput = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_FETCHED_INPUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_FETCHED_INPUT_DEFAULT);
    this.freeMemoryThreshold = maxTaskAvailableMemory;  // TODO: factor

    final float freeMemoryFactor = conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_FREE_MEMORY_FACTOR_FOR_FETCHED_INPUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_FREE_MEMORY_FACTOR_FOR_FETCHED_INPUT_DEFAULT);
    if (freeMemoryFactor <= 0.0f) {
      throw new IllegalArgumentException("Invalid value for "
          + TezRuntimeConfiguration.TEZ_RUNTIME_FREE_MEMORY_FACTOR_FOR_FETCHED_INPUT + ": "
          + freeMemoryFactor);
    }
    this.freeMemoryLimit = (long)(maxTaskAvailableMemory * freeMemoryFactor);

    this.shuffleMemoryStreaming = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_UNORDERED_MEMORY_STREAMING,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_UNORDERED_MEMORY_STREAMING_DEFAULT);

    LOG.info("{}: memoryLimit={}, maxSingleMemoryShuffle={}, freeMemoryLimit={}, shuffleMemoryStreaming={}",
        srcNameTrimmed, memoryLimit, maxSingleMemoryShuffle, freeMemoryLimit, shuffleMemoryStreaming);
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
    return (long)(Math.min(maxAvailableTaskMemory, Integer.MAX_VALUE) * maxInMemCopyUse);
  }

  final private FetchedInput stallShuffle = FetchedInput.createWaitFetchedInput(null);

  @Override
  public synchronized FetchedInput allocate(long actualSize, long compressedSize,
      InputAttemptIdentifier inputAttemptIdentifier,
      boolean isFromShufflePayload, boolean isFetchFromLocal) throws IOException {
    if (actualSize > maxSingleMemoryShuffle) {
      if (useFreeMemoryFetchedInput) {
        MemoryFetchedInput result = getMemoryFetchedInput(actualSize, inputAttemptIdentifier, true);
        if (result != null) {
          return result;
        }
      }
      return getDiskFetchedInput(compressedSize, inputAttemptIdentifier);
    }

    if (!isFromShufflePayload && usedMemory.get() + actualSize > memoryLimit) {
      // This Task has used up all its memory (memoryLimit).
      // check if we can borrow from free memory in the current ContainerWorker
      // Even when we have enough free memory, do not use more memory than freeMemoryLimit for storing MemoryFetchedInput.
      if (!useFreeMemoryFetchedInput || !hasFreeMemoryForSize(actualSize)) {
        if (shuffleMemoryStreaming) {
          return stallShuffle;
        }
        return getDiskFetchedInput(compressedSize, inputAttemptIdentifier);
      }
    }

    // If useFreeMemoryFetchedInput == true, we have:
    //   usedMemory.get() + actualSize <= memoryLimit || hasFreeMemoryForSize(actualSize)
    // Hence, do not call checkFreeMemoryForSize() again.
    MemoryFetchedInput result = getMemoryFetchedInput(actualSize, inputAttemptIdentifier, false);
    if (result != null) {
      return result;
    }
    return getDiskFetchedInput(compressedSize, inputAttemptIdentifier);
  }

  private DiskFetchedInput getDiskFetchedInput(long compressedSize, InputAttemptIdentifier inputAttemptIdentifier)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating DiskFetchedInput: {}", compressedSize);
    }
    return new DiskFetchedInput(compressedSize,
      inputAttemptIdentifier, this, conf, fileNameAllocator);
  }

  private boolean hasFreeMemoryForSize(long actualSize) {
    long currentFreeMemory = Runtime.getRuntime().freeMemory();
    return currentFreeMemory >= freeMemoryThreshold && usedMemory.get() + actualSize <= freeMemoryLimit;
  }

  private MemoryFetchedInput getMemoryFetchedInput(long actualSize, InputAttemptIdentifier inputAttemptIdentifier,
      boolean checkFreeMemory) {
    if (!checkFreeMemory || hasFreeMemoryForSize(actualSize)) {
      try {
        MemoryFetchedInput result = new MemoryFetchedInput(actualSize, inputAttemptIdentifier, this);
        this.usedMemory.addAndGet(actualSize);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Created MemoryFetchedInput: {}, {}", this.usedMemory.get(), actualSize);
        }
        return result;
      } catch (OutOfMemoryError oom) {
        LOG.error("Failed to create MemoryFetchedInput, fall through: {}, {}", this.usedMemory.get(), actualSize, oom);
      }
    }
    return null;
  }

  @Override
  public synchronized void fetchComplete(FetchedInput fetchedInput) {
    switch (fetchedInput.getType()) {
    case DISK:
    case DISK_DIRECT:
    case LOCAL_BYTE_CACHE:
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
    case MEMORY:
      unreserve(((MemoryFetchedInput) fetchedInput).getSize());
      break;
    case DISK:
    case DISK_DIRECT:
    case LOCAL_BYTE_CACHE:
      break;
    default:
      throw new TezUncheckedException("InputType: " + fetchedInput.getType()
          + " not expected for Broadcast fetch");
    }
  }

  private synchronized void unreserve(long size) {
    this.usedMemory.addAndGet(-size);
    if (LOG.isDebugEnabled()) {
      LOG.debug(srcNameTrimmed + ": " + "Used memory after freeing " + size  + " : " + this.usedMemory.get());
    }
  }
}
