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

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.runtime.api.FetcherConfig;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.FetchResult;
import org.apache.tez.runtime.library.common.shuffle.Fetcher;
import org.apache.tez.runtime.library.common.shuffle.InputHost;
import org.apache.tez.runtime.library.common.shuffle.ShuffleClient;
import org.apache.tez.runtime.library.common.shuffle.ShuffleServer;
import org.apache.tez.runtime.library.common.shuffle.ShuffleServer.PathPartition;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.api.ShuffleHandlerError;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MapOutput.Type;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleScheduler.ShuffleErrorCounterGroup;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.exceptions.FetcherReadTimeoutException;
import org.apache.tez.runtime.library.utils.CodecUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetcherOrderedGrouped extends Fetcher<MapOutput> {

  private static final Logger LOG = LoggerFactory.getLogger(FetcherOrderedGrouped.class);
  private static final boolean isDebugEnabled = LOG.isDebugEnabled();

  private static final InputAttemptIdentifier[] EMPTY_ATTEMPT_ID_ARRAY = new InputAttemptIdentifier[0];

  private static class MapOutputStat {
    final InputAttemptIdentifier srcAttemptId;
    final long decompressedLength;
    final long compressedLength;
    final int forReduce;

    MapOutputStat(InputAttemptIdentifier srcAttemptId, long decompressedLength, long compressedLength,
        int forReduce) {
      this.srcAttemptId = srcAttemptId;
      this.decompressedLength = decompressedLength;
      this.compressedLength = compressedLength;
      this.forReduce = forReduce;
    }

    @Override
    public String toString() {
      return "id: " + srcAttemptId +
          ", decompressed length: " + decompressedLength +
          ", compressed length: " + compressedLength +
          ", reduce: " + forReduce;
    }
  }

  private final int fetcherIdentifier;
  private final String logIdentifier;
  private final ShuffleScheduler shuffleScheduler;
  private final Long shuffleSchedulerId;

  private final FetchedInputAllocatorOrderedGrouped allocator;
  private final ExceptionReporter exceptionReporter;
  private final ShuffleErrorCounterGroup shuffleErrorCounterGroup;

  private volatile boolean stopped = false;
  private final Object cleanupLock = new Object();

  public FetcherOrderedGrouped(
      ShuffleServer fetcherCallback,
      Configuration conf,
      InputHost inputHost,
      InputHost.PartitionToInputs pendingInputsSeq,
      FetcherConfig fetcherConfig,
      TaskContext taskContext,
      int attempt,
      ShuffleScheduler shuffleScheduler) {
    super(fetcherCallback, conf, inputHost, fetcherConfig, taskContext, pendingInputsSeq, attempt);

    this.shuffleScheduler = shuffleScheduler;
    this.shuffleSchedulerId = shuffleScheduler.getShuffleClientId();

    this.fetcherIdentifier = fetcherIdGen.incrementAndGet();
    this.logIdentifier = attempt == 0 ?
        shuffleScheduler.getLogIdentifier() + "-O-" + minPartition:
        shuffleScheduler.getLogIdentifier() + "-O-" + minPartition + "-" + attempt;

    this.allocator = shuffleScheduler.getAllocator();
    this.exceptionReporter = shuffleScheduler.getExceptionReporter();
    this.shuffleErrorCounterGroup = shuffleScheduler.getShuffleErrorCounterGroup();
  }

  public FetcherOrderedGrouped createClone() {
    return new FetcherOrderedGrouped(
        fetcherCallback, conf, inputHost, pendingInputsSeq,
        fetcherConfig, taskContext, this.attempt + 1, shuffleScheduler);
  }

  public ShuffleClient<MapOutput> getShuffleClient() {
    return shuffleScheduler;
  }

  public boolean useSingleShuffleClientId(Long targetShuffleSchedulerId) {
    return shuffleSchedulerId.equals(targetShuffleSchedulerId);
  }

  public String getFetcherIdentifier() {
    return logIdentifier;
  }

  public FetchResult call() {
    assert !pendingInputsSeq.getInputs().isEmpty();

    startMillis = System.currentTimeMillis();
    buildPathToAttemptMap();

    Map<InputAttemptIdentifier, InputHost.PartitionRange> pendingInputs = null;
    try {
      codec = codecHolder.get();
      if (codec == null) {
        // clone codecConf because Decompressor uses locks on the Configuration object
        Configuration codecConf = new Configuration(fetcherConfig.codecConf);
        CompressionCodec newCodec = CodecUtils.getCodec(codecConf);
        codec = newCodec;
        codecHolder.set(newCodec);
      }

      pendingInputs = fetchNext();
    } catch (InterruptedException ie) {
      // TODO: might not be respected when fetcher is in progress / server is busy. TEZ-711
      // set the status back
      Thread.currentThread().interrupt();
    } catch (Throwable t) {
      exceptionReporter.reportException(t);
      // Shuffle knows how to deal with failures post shutdown via the onFailure hook
    }

    if (pendingInputs != null) {
      return new FetchResult(shuffleSchedulerId, inputHost.getHostPort(), pendingInputs);
    } else {
      return null;
    }
  }

  // called from ShuffleServer or at the end of call()
  public void shutdown(boolean disconnect) {
    if (!stopped) {
      if (isDebugEnabled) {
        LOG.debug("Fetcher stopped for host " + host);
      }
      stopped = true;
      // An interrupt will come in while shutting down the thread.
      cleanupCurrentConnection(disconnect);
    }
  }

  private Map<InputAttemptIdentifier, InputHost.PartitionRange> fetchNext() throws InterruptedException {
    boolean useLocalDiskFetch;
    boolean useFreeMemoryWriterOutput = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT_DEFAULT);
    boolean useFreeMemoryFetchedInput = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_FETCHED_INPUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_FETCHED_INPUT_DEFAULT);
    boolean shuffleMemToMem = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM_DEFAULT);
    if (useFreeMemoryWriterOutput || useFreeMemoryFetchedInput || shuffleMemToMem) {
      // useFreeMemoryWriterOutput == true: required
      // useFreeMemoryFetchedInput == true: recommended for performance
      // shuffleMemToMem == true: recommended for performance
      useLocalDiskFetch = false;
    } else {
      if (fetcherConfig.localDiskFetchOrderedEnabled &&
          host.equals(fetcherConfig.localHostName)) {
        if (fetcherConfig.compositeFetch) {
          // inspect 'first' to find the container where all inputs originate from
          InputAttemptIdentifier first = pendingInputsSeq.getInputs().get(0);
          // true if inputs originate from the current ContainerWorker
          useLocalDiskFetch = first.getPathComponent().startsWith(
              taskContext.getExecutionContext().getContainerId());
        } else {
          useLocalDiskFetch = true;
        }
      } else {
        useLocalDiskFetch = false;
      }
    }

    List<InputAttemptIdentifier> failedFetches = null;
    Map<InputAttemptIdentifier, InputHost.PartitionRange> pendingInputs = null;

    try {
      fetcherCallback.waitForMergeManager(shuffleSchedulerId);
      if (useLocalDiskFetch) {
        failedFetches = setupLocalDiskFetch();
      } else {
        pendingInputs = copyFromHost();
      }
    } finally {
      if (failedFetches != null && !failedFetches.isEmpty()) {
        failedFetches.forEach(input ->
          fetcherCallback.fetchFailed(shuffleSchedulerId, input, true, false,
              inputHost, getPartitionRange(), this));
      }
      if (!useLocalDiskFetch) {
        // This is a minor optimization that cleans up the current connection.
        // shutdown() will call cleanupCurrentConnection() again, but will have no effect.
        cleanupCurrentConnection(false);  // false to reuse connection
      }
    }

    return pendingInputs;
  }

  // return pendingInputs[]
  private Map<InputAttemptIdentifier, InputHost.PartitionRange> copyFromHost() {
    // reset retryStartTime for a new host
    retryStartTime = 0;

    if (isDebugEnabled) {
      LOG.debug("Fetcher " + fetcherIdentifier + " going to fetch from " + host
          + ", partition range: " + minPartition + "-" + maxPartition);
    }

    Map<InputAttemptIdentifier, InputHost.PartitionRange> pendingInputs = setupConnection(0);
    if (pendingInputs != null) {  // stopped or failure
      if (stopped) {
        // perhaps cleanupCurrentConnection() was already called in shutdown()
        cleanupCurrentConnection(false);  // disconnect = false because we can reuse HTTPConnection
        return null;
      }
      return pendingInputs;
    }

    // Loop through available map-outputs and fetch them
    // On any error, failedInputs is not null and we exit
    // after putting back the remaining maps to the
    // yet_to_be_fetched list and marking the failed tasks.
    int index = 0;  // points to the next input to be consumed
    InputAttemptIdentifier[] failedInputs = null;
    while (index < numInputs) {
      InputAttemptIdentifier inputAttemptIdentifier = pendingInputsSeq.getInputs().get(index);

      // fail immediately after first failure because we don't know how much to
      // skip for this error in the input stream. So we cannot move on to the
      // remaining outputs. YARN-1773. Will get to them in the next retry.
      try {
        failedInputs = copyMapOutput(input, inputAttemptIdentifier);
        if (failedInputs != null) {
          break;
        }
        index++;
      } catch (FetcherReadTimeoutException e) {
        cleanupCurrentConnection(true);   // disconnect = true because of error
        if (stopped) {
          // perhaps cleanupCurrentConnection(false) was already called in shutdown(),
          // but we just called cleanupCurrentConnection(true) anyway
          if (isDebugEnabled) {
            LOG.debug("Not re-establishing connection since Fetcher has been stopped");
          }
          // do not put back remaining inputs because stopped
          return null;
        }
        // Connect with retry
        Map<InputAttemptIdentifier, InputHost.PartitionRange> pendingInputsNew = setupConnection(index);
        if (pendingInputsNew != null) {   // stopped or failure
          if (stopped) {
            // perhaps cleanupCurrentConnection() was already called in shutdown()
            cleanupCurrentConnection(false);  // disconnect = false because we can reuse HTTPConnection
            if (isDebugEnabled) {
              LOG.debug("Not reporting connection re-establishment failure since fetcher is stopped");
            }
            return null;
          }
          return pendingInputsNew;
        }
      }
    }

    Map<InputAttemptIdentifier, InputHost.PartitionRange> pendingInputsFinal;
    if (index == numInputs) {   // success
      assert failedInputs == null;
      // no need to return pendingInputs[] because this is success
      pendingInputsFinal = null;
    } else {
      assert failedInputs != null;
      pendingInputsFinal = buildInputMapFromIndex(index);

      if (failedInputs.length > 0) {
        if (stopped) {
          if (isDebugEnabled) {
            LOG.debug("Ignoring copyMapOutput failures for tasks: " + Arrays.toString(failedInputs) +
              " since Fetcher has been stopped");
          }
          // perhaps cleanupCurrentConnection() was already called in shutdown()
          cleanupCurrentConnection(false);  // disconnect = false because we can reuse HTTPConnection
          return null;
        } else {
          LOG.warn("copyMapOutput failed for tasks: " + Arrays.toString(failedInputs));
          for (InputAttemptIdentifier failedInput : failedInputs) {
            // readError == false and connectError == false, so we only report fetch failure
            fetcherCallback.fetchFailed(shuffleSchedulerId, failedInput, true, false,
                inputHost, getPartitionRange(), this);

            // Try to remove failedInput from pendingInputsFinal[] because it reports fetch failure to AM.
            // If failedInput == srcAttemptId (instead of inputAttemptIdentifier) in copyMapOutput(),
            // failedInput may not be removed from pendingInputsFinal[], e.g.:
            //   - inputAttemptIdentifier = CompositeInputAttemptIdentifier[inputAttemptIdentifier=1000, count=5]
            //   - srcAttemptId = InputAttemptIdentifier[inputAttemptIdentifier=1004]
            // In this case, the same inputAttemptIdentifier is tried again at the next iteration.
            // This is safe because eventually the task is re-executed and new DMEs arrive, making existing DMEs obsolete.
            if (pendingInputsFinal != null) {
              pendingInputsFinal.remove(failedInput);
            }
          }
        }
      }
    }

    // no need to call cleanupCurrentConnection(false) because of finally{} in fetchNext()
    return pendingInputsFinal;
  }

  // return null if success
  // return non-null pendingInputs[] if stopped or failure
  //   - should check 'stopped' after returning
  //   - 'interrupted' is treated as 'failure', as in the original Tez implementation
  //   - pendingInputs[] should be put back in the queue
  //   - pendingInputs[] can be empty
  //   - shuffleSchedulerServer.fetchFailed() has been called for failedInputs[]
  private Map<InputAttemptIdentifier, InputHost.PartitionRange> setupConnection(int currentIndex) {
    assert currentIndex < pendingInputsSeq.getInputs().size();

    boolean connectSucceeded = false;
    try {
      String finalHost;
      boolean sslShuffle = fetcherConfig.httpConnectionParams.isSslShuffle();
      if (sslShuffle) {
        // TODO: cache in host
        finalHost = InetAddress.getByName(host).getHostName();
      } else {
        finalHost = host;
      }

      InputHost.PartitionRange range = pendingInputsSeq.getPartitionRange();
      StringBuilder baseURI = ShuffleUtils.constructBaseURIForShuffleHandler(finalHost,
          port, range, applicationId, shuffleScheduler.getDagIdentifier(),
          fetcherConfig.httpConnectionParams.isSslShuffle());

      Collection<InputAttemptIdentifier> inputsForPathComponents =
        pendingInputsSeq.getInputs().subList(currentIndex, pendingInputsSeq.getInputs().size());
      // inputsForPathComponents[] is a View, so do not update it
      URL url = ShuffleUtils.constructInputURL(baseURI.toString(), inputsForPathComponents,
          fetcherConfig.httpConnectionParams.isKeepAlive());

      httpConnection = ShuffleUtils.getHttpConnection(url, fetcherConfig.httpConnectionParams,
          logIdentifier, fetcherConfig.jobTokenSecretMgr);
      connectSucceeded = httpConnection.connect();
    } catch (IOException | InterruptedException ie) {
      if (ie instanceof InterruptedException) {
        Thread.currentThread().interrupt();   // reset status
      }
      if (stopped) {
        if (isDebugEnabled) {
          LOG.debug("Not reporting fetch failure, since an Exception was caught after shutdown");
        }
        return new HashMap<>();
      }
      shuffleErrorCounterGroup.ioErrs.increment(1);
      LOG.warn("{}: Failed to connect from {} to {} with index = {}", logIdentifier, fetcherConfig.localHostName,
        host, currentIndex, ie);
      shuffleErrorCounterGroup.connectionErrs.increment(1);

      if (fetcherConfig.connectionFailAllInput) {
        // no pending inputs && only failed inputs
        InputAttemptIdentifier[] failedFetches = buildInputSeqFromIndex(currentIndex);
        for (InputAttemptIdentifier failedFetch : failedFetches) {
          fetcherCallback.fetchFailed(shuffleSchedulerId, failedFetch, false, true,
              inputHost, getPartitionRange(), this);
        }
        return new HashMap<>();   // non-null, so failure; empty, so no pendingInputs[]
      } else {
        // pending inputs == all remaining, except failedInput == InputAttemptIdentifier at currentIndex
        InputAttemptIdentifier failedFetch =  pendingInputsSeq.getInputs().get(currentIndex);
        fetcherCallback.fetchFailed(shuffleSchedulerId, failedFetch, false, true,
            inputHost, getPartitionRange(), this);

        Map<InputAttemptIdentifier, InputHost.PartitionRange> pendingInputs = buildInputMapFromIndex(currentIndex);
        pendingInputs.remove(failedFetch);
        return pendingInputs;   // non-null, so failure; non-empty pendingInputs[]
      }
    }

    if (stopped) {
      if (isDebugEnabled) {
        LOG.debug("Detected fetcher has been shutdown after connection establishment. Returning");
      }
      return new HashMap<>();
    }

    try {
      input = httpConnection.getInputStream();  // incurs a network transmission
      setStage(STAGE_FIRST_FETCHED);
      if (getState() == STATE_STUCK) {
        fetcherCallback.wakeupLoop();
      }
      httpConnection.validate();
    } catch (IOException | InterruptedException ie) {
      if (ie instanceof InterruptedException) {
        Thread.currentThread().interrupt();   // reset status
      }
      if (stopped) {
        // Not reporting fetch failure, since an Exception was caught after shutdown
        return new HashMap<>();
      }
      shuffleErrorCounterGroup.ioErrs.increment(1);
      if (!connectSucceeded) {
        LOG.warn("{}: Failed to connect from {} to {} with index = {}: {}",
            logIdentifier, fetcherConfig.localHostName, host, currentIndex,
            ie.getClass().getName() + "/" + ie.getMessage());
        shuffleErrorCounterGroup.connectionErrs.increment(1);
      } else {
        LOG.warn("{}: Failed to verify reply after connecting from {} to {} with index = {}, informing ShuffleScheduler: {}",
            logIdentifier, fetcherConfig.localHostName, host, currentIndex,
            ie.getClass().getName() + "/" + ie.getMessage());
      }

      // similarly to FetcherUnordered, penalize only the first map and add the rest
      InputAttemptIdentifier failedFetch = pendingInputsSeq.getInputs().get(currentIndex);
      fetcherCallback.fetchFailed(shuffleSchedulerId, failedFetch, connectSucceeded, !connectSucceeded,
          inputHost, getPartitionRange(), this);

      Map<InputAttemptIdentifier, InputHost.PartitionRange> pendingInputs = buildInputMapFromIndex(currentIndex);
      pendingInputs.remove(failedFetch);
      return pendingInputs;
    }

    return null;
  }

  // return failedInputs[]
  private InputAttemptIdentifier[] copyMapOutput(
      DataInputStream input,
      InputAttemptIdentifier inputAttemptIdentifier) throws FetcherReadTimeoutException {
    MapOutput mapOutput = null;
    InputAttemptIdentifier srcAttemptId = null;
    long decompressedLength = 0;
    long compressedLength = 0;
    try {
      long startTime = System.currentTimeMillis();
      int partitionCount = 1;   // single partition only when using Hadoop shuffle service

      if (fetcherConfig.compositeFetch) {
        // Multiple partitions are fetched
        partitionCount = input.readInt();
      }
      ArrayList<MapOutputStat> mapOutputStats = new ArrayList<>(partitionCount);
      for (int mapOutputIndex = 0; mapOutputIndex < partitionCount; mapOutputIndex++) {
        MapOutputStat mapOutputStat = null;
        try {
          // Read the shuffle header
          ShuffleHeader header = new ShuffleHeader(fetcherConfig.compositeFetch);
          // TODO Review: Multiple header reads in case of status WAIT ?
          header.readFields(input);
          if (!header.mapId.startsWith(InputAttemptIdentifier.PATH_PREFIX_MR3) && !header.mapId.startsWith(InputAttemptIdentifier.PATH_PREFIX)) {
            if (!stopped) {
              shuffleErrorCounterGroup.badIdErrs.increment(1);
              if (header.mapId.startsWith(ShuffleHandlerError.DISK_ERROR_EXCEPTION.toString())) {
                LOG.warn("{}: ShuffleHandler error - {}, while fetching {}",
                    logIdentifier, header.mapId, inputAttemptIdentifier);
                // TODO: Why is this necessary? We return [inputAttemptIdentifier] anyway.
                fetcherCallback.informAM(shuffleSchedulerId, inputAttemptIdentifier);
              } else {
                LOG.warn("{}: Invalid map id: {}, expected to start with {} / {}, partition: {}",
                    logIdentifier, header.mapId,
                    InputAttemptIdentifier.PATH_PREFIX_MR3, InputAttemptIdentifier.PATH_PREFIX, header.forReduce);
              }
              return new InputAttemptIdentifier[]{ inputAttemptIdentifier };
            } else {
              if (isDebugEnabled) {
                LOG.debug("Already shutdown. Ignoring invalid map id error");
              }
              return EMPTY_ATTEMPT_ID_ARRAY;
            }
          }

          if (header.getCompressedLength() == 0) {
            // Empty partitions are already accounted for
            continue;
          }

          mapOutputStat = new MapOutputStat(
              pathToAttemptMap.get(new PathPartition(header.mapId, header.forReduce)),
              header.uncompressedLength,
              header.compressedLength,
              header.forReduce);
          mapOutputStats.add(mapOutputStat);
        } catch (IllegalArgumentException e) {
          if (!stopped) {
            shuffleErrorCounterGroup.badIdErrs.increment(1);
            LOG.warn("{}: Invalid map id", logIdentifier, e);
            // Don't know which one was bad, so consider this one bad and don't read
            // the remaining because we don't know where to start reading from. YARN-1773
            return new InputAttemptIdentifier[]{ inputAttemptIdentifier };
          } else {
            if (isDebugEnabled) {
              LOG.debug("Already shutdown. Ignoring invalid map id error. Exception: " +
                  e.getClass().getName() + ", Message: " + e.getMessage());
            }
            return EMPTY_ATTEMPT_ID_ARRAY;
          }
        }

        // Do some basic sanity verification
        if (!verifySanity(mapOutputStat.compressedLength, mapOutputStat.decompressedLength,
                          mapOutputStat.forReduce, mapOutputStat.srcAttemptId)) {
          if (!stopped) {
            srcAttemptId = mapOutputStat.srcAttemptId;
            if (srcAttemptId == null) {
              srcAttemptId = inputAttemptIdentifier;
              LOG.warn("{}: Was expecting {} but got null", logIdentifier, srcAttemptId);
            }
            assert (srcAttemptId != null);
            return new InputAttemptIdentifier[]{ srcAttemptId };
          } else {
            if (isDebugEnabled) {
              LOG.debug("Already stopped. Ignoring verification failure.");
            }
            return EMPTY_ATTEMPT_ID_ARRAY;
          }
        }

        if (isDebugEnabled) {
          LOG.debug("header: " + mapOutputStat.srcAttemptId + ", len: " + mapOutputStat.compressedLength +
              ", decomp len: " + mapOutputStat.decompressedLength);
        }
      }

      for (MapOutputStat mapOutputStat : mapOutputStats) {
        // Get the location for the map output - either in-memory or on-disk
        srcAttemptId = mapOutputStat.srcAttemptId;
        decompressedLength = mapOutputStat.decompressedLength;
        compressedLength = mapOutputStat.compressedLength;
        try {
          mapOutput = allocator.reserve(srcAttemptId, decompressedLength, compressedLength, fetcherIdentifier);
        } catch (IOException e) {
          if (!stopped) {
            // Kill the reduce attempt
            shuffleErrorCounterGroup.ioErrs.increment(1);
            LOG.error("{}: Shuffle failed because of a local error", logIdentifier, e);
            exceptionReporter.reportException(e);
          } else {
            if (isDebugEnabled) {
              LOG.debug("Already stopped. Ignoring error from merger.reserve");
            }
          }
          return EMPTY_ATTEMPT_ID_ARRAY;
        }

        // Check if we can shuffle *now* ...
        if (mapOutput.getType() == Type.WAIT) {
          LOG.info("{}: MergerManager returned Status.WAIT...", logIdentifier);
          // Not an error but wait to process data.
          return EMPTY_ATTEMPT_ID_ARRAY;
        }

        // Go!
        if (isDebugEnabled) {
          LOG.debug(logIdentifier + " about to shuffle output of map " +
              mapOutput.getAttemptIdentifier() + " decomp: " +
              decompressedLength + " len: " + compressedLength + " to " + mapOutput.getType());
        }

        if (mapOutput.getType() == Type.MEMORY) {
          ShuffleUtils.shuffleToMemory(mapOutput.getMemory(), input, (int) decompressedLength,
              (int) compressedLength, codec, fetcherConfig.ifileReadAhead,
              fetcherConfig.ifileReadAheadLength, LOG,
              mapOutput.getAttemptIdentifier(), taskContext, true);
        } else if (mapOutput.getType() == Type.DISK) {
          ShuffleUtils.shuffleToDisk(mapOutput.getDisk(), host,
              input, compressedLength, decompressedLength, LOG, mapOutput.getAttemptIdentifier(),
              fetcherConfig.ifileReadAhead, fetcherConfig.ifileReadAheadLength,
              fetcherConfig.verifyDiskChecksum);
        } else {
          throw new IOException("Unknown mapOutput type while fetching shuffle data:" +
              mapOutput.getType());
        }

        // Inform the shuffle scheduler
        long endTime = System.currentTimeMillis();
        // Reset retryStartTime as map task make progress if retried before.
        retryStartTime = 0;
        fetcherCallback.fetchSucceeded(shuffleSchedulerId, host, srcAttemptId, mapOutput,
            compressedLength, decompressedLength, endTime - startTime);
      }

      // all success, now regard inputAttemptIdentifier as consumed
    } catch (IOException | InternalError ioe) {
      if (stopped) {
        if (isDebugEnabled) {
          LOG.debug("Not reporting fetch failure for exception during data copy: ["
              + ioe.getClass().getName() + ", " + ioe.getMessage() + "]");
        }
        // perhaps cleanupCurrentConnection(false) was already called in shutdown()
        cleanupCurrentConnection(true);   // true because of error
        if (mapOutput != null) {
          mapOutput.abort(); // Release resources
        }
        // Don't need to put back - since that's handled by the invoker
        return EMPTY_ATTEMPT_ID_ARRAY;
      }
      if (shouldRetry(ioe)) {
        // release mem/file handles
        if (mapOutput != null) {
          mapOutput.abort();
        }
        throw new FetcherReadTimeoutException(ioe);
      }
      shuffleErrorCounterGroup.ioErrs.increment(1);
      if (srcAttemptId == null || mapOutput == null) {
        LOG.info("{}: Failed to read map header {} ({}, {}): {}",
            logIdentifier, srcAttemptId, decompressedLength, compressedLength,
            ioe.getClass().getName() + "/" + ioe.getMessage());
        if (srcAttemptId == null) {
          // unlike Tez, do not fail all inputs starting from currentIndex -- fail only inputAttemptIdentifier
          return new InputAttemptIdentifier[]{ inputAttemptIdentifier };
        } else {
          return new InputAttemptIdentifier[]{ srcAttemptId };
        }
      }
      LOG.warn("{}: Failed to shuffle output of {} from {}", logIdentifier, srcAttemptId, host, ioe);

      // Inform the ShuffleScheduler
      mapOutput.abort();
      return new InputAttemptIdentifier[]{ srcAttemptId };
    }

    return null;
  }

  private List<InputAttemptIdentifier> setupLocalDiskFetch() {
    if (isDebugEnabled) {
      LOG.debug("Fetcher " + fetcherIdentifier + " going to fetch (local disk) from " + host
          + ", partition range: " + minPartition + "-" + maxPartition);
    }

    int partitionId = pendingInputsSeq.getPartition();
    int partitionCount = pendingInputsSeq.getPartitionCount();

    setStage(STAGE_FIRST_FETCHED);  // local reads should not interfere with stage

    int index = 0;  // points to the next input to be consumed
    List<InputAttemptIdentifier> failedFetches = new ArrayList<>();
    while (index < numInputs) {
      // Avoid fetching more if already stopped
      if (stopped) {
        return null;
      }

      InputAttemptIdentifier inputAttemptIdentifier = pendingInputsSeq.getInputs().get(index);
      String pathComponent = inputAttemptIdentifier.getPathComponent();   // already in expanded form
      TezSpillRecord spillRecord = null;  // specific to each inputAttemptIdentifier/pathComponent
      Path inputFilePath = null;

      MapOutput mapOutput = null;
      boolean hasFailures = false;
      // Fetch partition count number of map outputs (handles auto-reduce case)
      for (int k = 0; k < partitionCount; k++) {
        int reduceId = partitionId + k;
        InputAttemptIdentifier srcAttemptId = pathToAttemptMap.get(new PathPartition(pathComponent, reduceId));

        try {
          long startTime = System.currentTimeMillis();

          // pathComponent == srcAttemptId.getPathComponent(), so we compute spillRecord and inputFilePath only once
          if (spillRecord == null) {
            AbstractMap.SimpleEntry<TezSpillRecord, Path> pair = ShuffleUtils.getTezSpillRecordInputFilePath(
                taskContext, pathComponent, fetcherConfig.compositeFetch,
                shuffleScheduler.getDagIdentifier(), conf,
                fetcherConfig.localDirAllocator, fetcherConfig.localFs);
            spillRecord = pair.getKey();
            inputFilePath = pair.getValue();
          }
          TezIndexRecord indexRecord = spillRecord.getIndex(reduceId);
          if (!indexRecord.hasData()) {
            continue;
          }

          mapOutput = getMapOutputForDirectDiskFetch(srcAttemptId, inputFilePath, indexRecord);
          long endTime = System.currentTimeMillis();
          fetcherCallback.fetchSucceeded(shuffleSchedulerId, host, srcAttemptId, mapOutput,
              indexRecord.getPartLength(), indexRecord.getRawLength(), (endTime - startTime));
        } catch (IOException | InternalError e) {
          if (mapOutput != null) {
            mapOutput.abort();
          }
          if (!stopped) {
            hasFailures = true;
            shuffleErrorCounterGroup.ioErrs.increment(1);
            fetcherCallback.fetchFailed(shuffleSchedulerId, srcAttemptId, true, false,
                null, null, null);
            LOG.warn("{}: Failed to read local disk output of {} from {}", logIdentifier, srcAttemptId, host, e);
          } else {
            if (isDebugEnabled) {
              LOG.debug("Ignoring fetch error during local disk copy since fetcher has already been stopped");
            }
            return null;
          }
        }
      }

      if (hasFailures) {
        // failed to read some partition belonging to inputAttemptIdentifier inside the inner loop
        failedFetches.add(inputAttemptIdentifier);
      }

      index++;
    }

    assert index == numInputs;  // completed the loop
    return failedFetches;
  }

  private void cleanupCurrentConnection(boolean disconnect) {
    // Synchronizing on cleanupLock to ensure we don't run into a parallel close.
    // Can't synchronize on the main class itself since that would cause the shutdown request to block.
    synchronized (cleanupLock) {
      try {
        if (httpConnection != null) {
          httpConnection.cleanup(disconnect);
          // do not set httpConnection to null because shutdown() can be called at any moment and
          // we may see NPE from httpConnection.validate(), for example.
        }
      } catch (IOException e) {
        LOG.info("{}: Exception while shutting down: {}", logIdentifier, e.getMessage());
        if (isDebugEnabled) {
          LOG.debug(StringUtils.EMPTY, e);
        }
      }
    }
  }

  private MapOutput getMapOutputForDirectDiskFetch(InputAttemptIdentifier srcAttemptId, Path filename,
      TezIndexRecord indexRecord) throws IOException {
    return MapOutput.createLocalDiskMapOutput(srcAttemptId, allocator, filename,
        indexRecord.getStartOffset(), indexRecord.getPartLength(), true);
  }

  private boolean verifySanity(long compressedLength, long decompressedLength,
      int forReduce, InputAttemptIdentifier srcAttemptId) {
    if (compressedLength < 0 || decompressedLength < 0) {
      shuffleErrorCounterGroup.wrongLengthErrs.increment(1);
      LOG.warn("{}: Invalid lengths in map output header: id: {}, len: {}, decomp len: {}",
          logIdentifier, srcAttemptId, compressedLength, decompressedLength);
      return false;
    }

    // partitionId verification. Isn't available here because it is encoded into URI
    if (forReduce < minPartition || forReduce > maxPartition) {
      shuffleErrorCounterGroup.wrongReduceErrs.increment(1);
      LOG.warn("{}: Data for wrong partition map: {}, len: {}, decomp len: {} for partition {}, expected partition range: {}-{}",
          logIdentifier, srcAttemptId, compressedLength, decompressedLength, forReduce, minPartition, maxPartition);
      return false;
    }
    return true;
  }

  private boolean shouldRetry(Throwable ioe) {
    if (!(ioe instanceof SocketTimeoutException)) {
      return false;
    }
    // First time to retry.
    long currentTime = System.currentTimeMillis();
    if (retryStartTime == 0) {
      retryStartTime = currentTime;
    }

    if (currentTime - retryStartTime < fetcherConfig.httpConnectionParams.getReadTimeout()) {
      LOG.warn("{}: Shuffle output from {} failed, retry it", logIdentifier, host);
      //retry connecting to the host
      return true;
    } else {
      // timeout, prepare to be failed.
      LOG.warn("{}: Timeout for copying MapOutput with retry on host {} after {} milliseconds",
          logIdentifier, host, fetcherConfig.httpConnectionParams.getReadTimeout());
      return false;
    }
  }
}
