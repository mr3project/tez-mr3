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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.http.BaseHttpConnection;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.library.common.Constants;
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
  private static final AtomicInteger fetcherIdGen = new AtomicInteger(0);
  private final boolean isDebugEnabled = LOG.isDebugEnabled();

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

  private final ShuffleServer shuffleSchedulerServer;
  private final Configuration conf;
  private final String applicationId;
  private final ShuffleServer.FetcherConfig fetcherConfig;
  private final TaskContext taskContext;

  private final String host;
  private final int port;

  private final int fetcherIdentifier;
  private final String logIdentifier;

  private volatile boolean stopped = false;

  // Initiative value is 0, which means it hasn't retried yet.
  private long retryStartTime = 0;

  // set in assignShuffleClient()
  private ShuffleScheduler shuffleScheduler;
  private long shuffleSchedulerId;
  private int dagId;
  private FetchedInputAllocatorOrderedGrouped allocator;
  private ExceptionReporter exceptionReporter;
  private ShuffleErrorCounterGroup shuffleErrorCounterGroup;

  private volatile BaseHttpConnection httpConnection;
  private volatile DataInputStream input;

  private final Object cleanupLock = new Object();

  private CompressionCodec codec;
  private static final ThreadLocal<CompressionCodec> codecHolder = new ThreadLocal<>();

  public FetcherOrderedGrouped(
      ShuffleServer shuffleSchedulerServer,
      Configuration conf,
      InputHost inputHost,
      InputHost.PartitionToInputs pendingInputsSeq,
      ShuffleServer.FetcherConfig fetcherConfig,
      TaskContext taskContext,
      ShuffleScheduler shuffleScheduler) {
    super(pendingInputsSeq);

    this.shuffleSchedulerServer = shuffleSchedulerServer;
    this.conf = conf;
    this.applicationId = taskContext.getApplicationId().toString();
    this.fetcherConfig = fetcherConfig;
    this.taskContext = taskContext;

    this.host = inputHost.getHost();
    this.port = inputHost.getPort();

    this.fetcherIdentifier = fetcherIdGen.incrementAndGet();
    this.logIdentifier = shuffleScheduler.getLogIdentifier() + "-O-" + fetcherIdentifier;
  }

  public void assignShuffleClient(ShuffleClient<MapOutput> shuffleClient) {
    this.shuffleScheduler = (ShuffleScheduler)shuffleClient;
    this.shuffleSchedulerId = shuffleClient.getShuffleClientId();
    this.dagId = shuffleClient.getDagIdentifier();

    this.allocator = shuffleScheduler.getAllocator();
    this.exceptionReporter = shuffleScheduler.getExceptionReporter();
    this.shuffleErrorCounterGroup = shuffleScheduler.getShuffleErrorCounterGroup();
  }

  public ShuffleClient<MapOutput> getShuffleClient() {
    return shuffleScheduler;
  }

  public boolean useSingleShuffleClientId(long shuffleSchedulerId) {
    return shuffleSchedulerId == this.shuffleSchedulerId;
  }

  public String getFetcherIdentifier() {
    return logIdentifier;
  }

  public FetchResult call() {
    assert !pendingInputsSeq.getInputs().isEmpty();

    buildPathToAttemptMap();

    Map<InputAttemptIdentifier, InputHost.PartitionRange> pendingInputs = null;
    try {
      codec = codecHolder.get();
      if (codec == null) {
        // clone codecConf because Decompressor uses locks on the Configuration object
        CompressionCodec newCodec = CodecUtils.getCodec(new Configuration(fetcherConfig.codecConf));
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
      return new FetchResult(shuffleSchedulerId, host, port, pendingInputs);
    } else {
      return null;
    }
  }

  public void shutdown() {
    if (!stopped) {
      if (isDebugEnabled) {
        LOG.debug("Fetcher stopped for host " + host);
      }
      stopped = true;
      // An interrupt will come in while shutting down the thread.
      cleanupCurrentConnection(false);
    }
  }

  private Map<InputAttemptIdentifier, InputHost.PartitionRange> fetchNext() throws InterruptedException {
    List<InputAttemptIdentifier> failedFetches = null;
    Map<InputAttemptIdentifier, InputHost.PartitionRange> pendingInputs = null;
    try {
      shuffleSchedulerServer.waitForMergeManager(shuffleSchedulerId);

      if (fetcherConfig.localDiskFetchEnabled
          && host.equals(fetcherConfig.localHostName)) {
        failedFetches = setupLocalDiskFetch();
      } else {
        pendingInputs = copyFromHost();
      }
    } finally {
      // FIXME
      if (failedFetches != null && !failedFetches.isEmpty()) {
        failedFetches.stream().forEach(input ->
          shuffleSchedulerServer.fetchFailed(shuffleSchedulerId, input, true, false));
      }
      cleanupCurrentConnection(false);
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
        cleanupCurrentConnection(true);
        return null;
      }
      return pendingInputs;
    }

    // Loop through available map-outputs and fetch them
    // On any error, failedInputs is not null and we exit
    // after putting back the remaining maps to the
    // yet_to_be_fetched list and marking the failed tasks.
    int index = 0;  // points to the next input to be consumed
    boolean putBackRemainingInputsFromIndex = true;   // true if remaining inputs should be put back in queue
    InputAttemptIdentifier[] failedInputs = null;

    while (index < numInputs) {
      InputAttemptIdentifier inputAttemptIdentifier = pendingInputsSeq.getInputs().get(index);

      // fail immediately after first failure because we don't know how much to
      // skip for this error in the input stream. So we cannot move on to the
      // remaining outputs. YARN-1773. Will get to them in the next retry.
      try {
        failedInputs = copyMapOutput(input, inputAttemptIdentifier, index);
        // Cf. failedInputs[] can be buildInputSeqFromIndex(index)
        // TODO: remove the above comment
        if (failedInputs != null) {
          break;
        }
        index++;
      } catch (FetcherReadTimeoutException e) {
        // Setup connection again if disconnected
        cleanupCurrentConnection(true);
        if (stopped) {
          // do not put back remaining inputs because stopped
          if (isDebugEnabled) {
            LOG.debug("Not re-establishing connection since Fetcher has been stopped");
          }
          return null;
        }
        // Connect with retry
        Map<InputAttemptIdentifier, InputHost.PartitionRange> pendingInputsNew = setupConnection(index);
        if (pendingInputsNew != null) {   // stopped or failure
          if (stopped) {
            cleanupCurrentConnection(true);
            if (isDebugEnabled) {
              LOG.debug("Not reporting connection re-establishment failure since fetcher is stopped");
            }
            return null;
          }
          return pendingInputsNew;
        }
      }
    }

    // Q. When do we have failedInputs == null?
    //   1. success --> set putBackRemainingInputsFromIndex to false
    //   2. index % pendingInputsSeq.length != 0 after FetcherReadTimeoutException --> already set to true
    // From 2, we may have:
    //   failedInputs == null && index < numInputs

    if (index == numInputs) {   // success
      assert failedInputs == null;
      putBackRemainingInputsFromIndex = false;
    }

    // TODO: calculate buildInputMapFromIndex(index) for putBackRemainingInputsFromIndex here

    if (failedInputs != null && failedInputs.length > 0) {
      if (stopped) {
        if (isDebugEnabled) {
          LOG.debug("Ignoring copyMapOutput failures for tasks: " + Arrays.toString(failedInputs) +
              " since Fetcher has been stopped");
        }
      } else {
        LOG.warn("copyMapOutput failed for tasks " + Arrays.toString(failedInputs));
        for (InputAttemptIdentifier left : failedInputs) {
          // readError == false and connectError == false, so we only report fetch failure
          shuffleSchedulerServer.fetchFailed(shuffleSchedulerId, left, true, false);
        }
      }
    }

    cleanupCurrentConnection(false);

    if (putBackRemainingInputsFromIndex) {
      // TODO: Why not remove failedInputs[]??? Note: I.A.I vs CompositeI.A.I.
      return buildInputMapFromIndex(index);
    } else {
      return null;
    }
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

      // exception, so no pending inputs && only failed inputs
      InputAttemptIdentifier[] failedFetches = buildInputSeqFromIndex(currentIndex);
      for (InputAttemptIdentifier failedFetch : failedFetches) {
        shuffleSchedulerServer.fetchFailed(shuffleSchedulerId, failedFetch, false, true);
      }
      return new HashMap<>();   // non-null, so failure; empty, so no pendingInputs[]
    }

    if (stopped) {
      if (isDebugEnabled) {
        LOG.debug("Detected fetcher has been shutdown after connection establishment. Returning");
      }
      return new HashMap<>();
    }

    try {
      input = httpConnection.getInputStream();
      httpConnection.validate();
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
      if (!connectSucceeded) {
        LOG.warn("{}: Failed to connect from {} to {} with index = {}", logIdentifier, fetcherConfig.localHostName,
            host, currentIndex, ie);
        shuffleErrorCounterGroup.connectionErrs.increment(1);
      } else {
        LOG.warn("{}: Failed to verify reply after connecting from {} to {} with index = {}", logIdentifier,
            fetcherConfig.localHostName, host, currentIndex, ie);
      }

      // similarly to FetcherUnordered, penalize only the first map and add the rest
      InputAttemptIdentifier failedFetch = pendingInputsSeq.getInputs().get(currentIndex);
      shuffleSchedulerServer.fetchFailed(shuffleSchedulerId, failedFetch, connectSucceeded, !connectSucceeded);

      Map<InputAttemptIdentifier, InputHost.PartitionRange> pendingInputs = buildInputMapFromIndex(currentIndex);
      pendingInputs.remove(failedFetch);
      return pendingInputs;
    }

    return null;
  }

  private InputAttemptIdentifier[] copyMapOutput(DataInputStream input,
      InputAttemptIdentifier inputAttemptIdentifier, int currentIndex) throws FetcherReadTimeoutException {
    MapOutput mapOutput = null;
    InputAttemptIdentifier srcAttemptId = null;
    long decompressedLength = 0;
    long compressedLength = 0;
    try {
      long startTime = System.currentTimeMillis();
      int partitionCount = 1;

      if (fetcherConfig.compositeFetch) {
        // Multiple partitions are fetched
        partitionCount = WritableUtils.readVInt(input);
      }
      ArrayList<MapOutputStat> mapOutputStats = new ArrayList<>(partitionCount);
      for (int mapOutputIndex = 0; mapOutputIndex < partitionCount; mapOutputIndex++) {
        MapOutputStat mapOutputStat = null;
        try {
          // Read the shuffle header
          ShuffleHeader header = new ShuffleHeader();
          // TODO Review: Multiple header reads in case of status WAIT ?
          header.readFields(input);
          if (!header.mapId.startsWith(InputAttemptIdentifier.PATH_PREFIX_MR3) && !header.mapId.startsWith(InputAttemptIdentifier.PATH_PREFIX)) {
            if (!stopped) {
              shuffleErrorCounterGroup.badIdErrs.increment(1);
              if (header.mapId.startsWith(ShuffleHandlerError.DISK_ERROR_EXCEPTION.toString())) {
                LOG.warn("{}: ShuffleHandler error - {}, while fetching {}",
                    logIdentifier, header.mapId, inputAttemptIdentifier);
                // TODO: Why is this necessary? We return [inputAttemptIdentifier] anyway.
                shuffleSchedulerServer.informAM(shuffleSchedulerId, inputAttemptIdentifier);
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
          //Not an error but wait to process data.
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

        shuffleSchedulerServer.fetchSucceeded(shuffleSchedulerId, host, srcAttemptId, mapOutput,
            compressedLength, decompressedLength, endTime - startTime);
      }

      // all success, now regard inputAttemptIdentifier as consumed
    } catch (IOException | InternalError ioe) {
      if (stopped) {
        if (isDebugEnabled) {
          LOG.debug("Not reporting fetch failure for exception during data copy: ["
              + ioe.getClass().getName() + ", " + ioe.getMessage() + "]");
        }
        cleanupCurrentConnection(true);
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
        LOG.info("{}: Failed to read map header {} decomp: {}, {}",
            logIdentifier, srcAttemptId, decompressedLength, compressedLength, ioe);
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

    int index = 0;  // points to the next input to be consumed
    List<InputAttemptIdentifier> failedFetches = new ArrayList<>();
    while (index < numInputs) {
      // Avoid fetching more if already stopped
      if (stopped) {
        return null;
      }

      InputAttemptIdentifier inputAttemptIdentifier = pendingInputsSeq.getInputs().get(index);
      String pathComponent = inputAttemptIdentifier.getPathComponent();

      MapOutput mapOutput = null;
      boolean hasFailures = false;
      // Fetch partition count number of map outputs (handles auto-reduce case)
      for (int k = 0; k < partitionCount; k++) {
        int reduceId = partitionId + k;
        InputAttemptIdentifier srcAttemptId = pathToAttemptMap.get(new PathPartition(pathComponent, reduceId));

        try {
          long startTime = System.currentTimeMillis();
          Path filename = getShuffleInputFileName(pathComponent, "");
          TezIndexRecord indexRecord = getIndexRecord(pathComponent, reduceId);
          if(!indexRecord.hasData()) {
            continue;
          }
          mapOutput = getMapOutputForDirectDiskFetch(srcAttemptId, filename, indexRecord);
          long endTime = System.currentTimeMillis();
          shuffleSchedulerServer.fetchSucceeded(shuffleSchedulerId, host, srcAttemptId, mapOutput,
              indexRecord.getPartLength(), indexRecord.getRawLength(), (endTime - startTime));
        } catch (IOException | InternalError e) {
          if (mapOutput != null) {
            mapOutput.abort();
          }
          if (!stopped) {
            hasFailures = true;
            shuffleErrorCounterGroup.ioErrs.increment(1);
            shuffleSchedulerServer.fetchFailed(shuffleSchedulerId, srcAttemptId, true, false);
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
    // Synchronizing on cleanupLock to ensure we don't run into a parallel close
    // Can't synchronize on the main class itself since that would cause the
    // shutdown request to block
    synchronized (cleanupLock) {
      try {
        if (httpConnection != null) {
          httpConnection.cleanup(disconnect);
          httpConnection = null;
        }
      } catch (IOException e) {
        if (isDebugEnabled) {
          LOG.debug("Exception while shutting down fetcher " + logIdentifier, e);
        } else {
          LOG.info("Exception while shutting down fetcher {}: {}", logIdentifier, e.getMessage());
        }
      }
    }
  }

  //TODO: Refactor following to make use of methods from TezTaskOutputFiles to be consistent.
  private Path getShuffleInputFileName(String pathComponent, String suffix) throws IOException {
    LocalDirAllocator localDirAllocator = new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);
    String pathFromLocalDir =
        ShuffleUtils.adjustPathComponent(fetcherConfig.compositeFetch, dagId, pathComponent) +
            Path.SEPARATOR + Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING + suffix;

    return localDirAllocator.getLocalPathToRead(pathFromLocalDir, conf);
  }

  private TezIndexRecord getIndexRecord(String pathComponent, int partitionId) throws IOException {
    Path indexFile = getShuffleInputFileName(pathComponent,
        Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING);
    TezSpillRecord spillRecord = new TezSpillRecord(indexFile, fetcherConfig.localFs);
    return spillRecord.getIndex(partitionId);
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
