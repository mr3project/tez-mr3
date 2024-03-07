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
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
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

public class FetcherOrderedGrouped implements Fetcher<MapOutput> {

  private static final Logger LOG = LoggerFactory.getLogger(FetcherOrderedGrouped.class);
  private static final AtomicInteger nextId = new AtomicInteger(0);

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
  private final ShuffleServer.FetcherConfig fetcherConfig;
  private final TaskContext taskContext;
  private final String applicationId;

  private final String host;
  private final int port;
  private final int minPartition;
  private final int maxPartition;
  private final int partitionCount;
  private final List<InputAttemptIdentifier> srcAttempts;

  private final int fetcherIdentifier;
  private final String logIdentifier;

  private final Map<PathPartition, InputAttemptIdentifier> pathToAttemptMap;

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

  private Map<Long, InputAttemptIdentifier> srcAttemptsRemaining;

  private volatile BaseHttpConnection httpConnection;
  private volatile DataInputStream input;

  private final Object cleanupLock = new Object();

  private CompressionCodec codec;
  private static final ThreadLocal<CompressionCodec> codecHolder = new ThreadLocal<>();

  public FetcherOrderedGrouped(
      ShuffleServer shuffleSchedulerServer,
      Configuration conf,
      InputHost inputHost,
      InputHost.PartitionToInputs pendingInputs,
      ShuffleServer.FetcherConfig fetcherConfig,
      TaskContext taskContext) {
    this.shuffleSchedulerServer = shuffleSchedulerServer;
    this.conf = conf;
    this.fetcherConfig = fetcherConfig;
    this.taskContext = taskContext;
    this.applicationId = taskContext.getApplicationId().toString();

    this.host = inputHost.getHost();
    this.port = inputHost.getPort();
    this.minPartition = pendingInputs.getPartition();
    this.partitionCount = pendingInputs.getPartitionCount();
    this.maxPartition = minPartition + partitionCount - 1;
    this.srcAttempts = pendingInputs.getInputs();

    this.fetcherIdentifier = nextId.incrementAndGet();
    this.logIdentifier = "FetcherOG-" + fetcherIdentifier;

    this.pathToAttemptMap = new HashMap<PathPartition, InputAttemptIdentifier>();
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

  // always results null
  public FetchResult call() {
    try {
      if (srcAttempts.isEmpty()) {
        return null;
      }

      populatePathToAttemptMap(srcAttempts);

      codec = codecHolder.get();
      if (codec == null) {
        // clone codecConf because Decompressor uses locks on the Configuration object
        CompressionCodec newCodec = CodecUtils.getCodec(new Configuration(fetcherConfig.codecConf));
        codec = newCodec;
        codecHolder.set(newCodec);
      }

      srcAttemptsRemaining = null;
      fetchNext();
    } catch (InterruptedException ie) {
      // TODO: might not be respected when fetcher is in progress / server is busy. TEZ-711
      // set the status back
      Thread.currentThread().interrupt();
      return null;
    } catch (Throwable t) {
      exceptionReporter.reportException(t);
      // Shuffle knows how to deal with failures post shutdown via the onFailure hook
    }
    return null;
  }

  public void shutdown() {
    if (!stopped) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Fetcher stopped for host " + host);
      }
      stopped = true;
      // An interrupt will come in while shutting down the thread.
      cleanupCurrentConnection(false);
    }
  }

  private void fetchNext() throws InterruptedException, IOException {
    try {
      shuffleSchedulerServer.waitForMergeManager(shuffleSchedulerId);

      if (fetcherConfig.localDiskFetchEnabled
          && host.equals(fetcherConfig.localHostName)
          && !fetcherConfig.localFetchComparePort) {
        setupLocalDiskFetch();
      } else {
        copyFromHost();
      }
    } finally {
      // FIXME
      srcAttemptsRemaining.values().forEach((input) -> {
        shuffleSchedulerServer.fetchFailed(shuffleSchedulerId, host, input, true, false);
      });
      cleanupCurrentConnection(false);
    }
  }

  private void copyFromHost() throws IOException {
    // reset retryStartTime for a new host
    retryStartTime = 0;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Fetcher " + fetcherIdentifier + " going to fetch from " + host + " for: "
          + srcAttempts + ", partition range: " + minPartition + "-" + maxPartition);
    }
    populateRemainingMap(srcAttempts);
    // srcAttemptsRemaining[] is ordered, so we should use srcAttemptsRemaining[] to construct URL
    if (!setupConnection(srcAttemptsRemaining.values())) {
      if (stopped) {
        cleanupCurrentConnection(true);
      }
      return;
    }

    // Loop through available map-outputs and fetch them
    // On any error, faildTasks is not null and we exit
    // after putting back the remaining maps to the
    // yet_to_be_fetched list and marking the failed tasks.
    InputAttemptIdentifier[] failedTasks = null;
    while (!srcAttemptsRemaining.isEmpty() && failedTasks == null) {
      InputAttemptIdentifier inputAttemptIdentifier =
          srcAttemptsRemaining.entrySet().iterator().next().getValue();
      // fail immediately after first failure because we don't know how much to
      // skip for this error in the input stream. So we cannot move on to the
      // remaining outputs. YARN-1773. Will get to them in the next retry.
      try {
        failedTasks = copyMapOutput(input, inputAttemptIdentifier);
      } catch (FetcherReadTimeoutException e) {
        // Setup connection again if disconnected
        cleanupCurrentConnection(true);
        if (stopped) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Not re-establishing connection since Fetcher has been stopped");
          }
          return;
        }
        // Connect with retry
        if (!setupConnection(srcAttemptsRemaining.values())) {
          if (stopped) {
            cleanupCurrentConnection(true);
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "Not reporting connection re-establishment failure since fetcher is stopped");
            }
            return;
          }
          failedTasks = new InputAttemptIdentifier[] {getNextRemainingAttempt()};
          break;
        }
      }
    }

    if (failedTasks != null && failedTasks.length > 0) {
      if (stopped) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Ignoring copyMapOutput failures for tasks: " + Arrays.toString(failedTasks) +
              " since Fetcher has been stopped");
        }
      } else {
        LOG.warn("copyMapOutput failed for tasks " + Arrays.toString(failedTasks));
        for (InputAttemptIdentifier left : failedTasks) {
          // readError == false and connectError == false, so we only report fetch failure

          // Use (true, false) to stop the corresponding ShuffleScheduler.
          // shuffleSchedulerServer.fetchFailed(shuffleSchedulerId, host, left, false, false);
          shuffleSchedulerServer.fetchFailed(shuffleSchedulerId, host, left, true, false);
        }
      }
    }

    cleanupCurrentConnection(false);

    // Sanity check
    if (failedTasks == null && !srcAttemptsRemaining.isEmpty()) {
      throw new IOException("server didn't return all expected map outputs: " + srcAttemptsRemaining.size() + " left.");
    }
  }

  private boolean setupConnection(Collection<InputAttemptIdentifier> attempts) {
    boolean connectSucceeded = false;
    try {
      String finalHost;
      boolean sslShuffle = fetcherConfig.httpConnectionParams.isSslShuffle();
      if (sslShuffle) {
        finalHost = InetAddress.getByName(host).getHostName();
      } else {
        finalHost = host;
      }

      InputHost.PartitionRange range = new InputHost.PartitionRange(minPartition, partitionCount);
      List<InputHost.PartitionRange> ranges = new ArrayList<>();
      ranges.add(range);
      StringBuilder baseURI = ShuffleUtils.constructBaseURIForShuffleHandler(finalHost,
          port, ranges, applicationId, dagId, sslShuffle);

      URL url = ShuffleUtils.constructInputURL(baseURI.toString(), attempts, fetcherConfig.httpConnectionParams.isKeepAlive());
      httpConnection = ShuffleUtils.getHttpConnection(url, fetcherConfig.httpConnectionParams,
          logIdentifier, fetcherConfig.jobTokenSecretMgr);
      connectSucceeded = httpConnection.connect();

      if (stopped) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Detected fetcher has been shutdown after connection establishment. Returning");
        }
        return false;
      }
      input = httpConnection.getInputStream();
      httpConnection.validate();
      return true;
    } catch (IOException | InterruptedException ie) {
      if (ie instanceof InterruptedException) {
        Thread.currentThread().interrupt(); //reset status
      }
      if (stopped) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Not reporting fetch failure, since an Exception was caught after shutdown");
        }
        return false;
      }
      shuffleErrorCounterGroup.ioErrs.increment(1);
      if (!connectSucceeded) {
        LOG.warn(String.format("Failed to connect from %s to %s with %d inputs", fetcherConfig.localHostName,
            host, srcAttemptsRemaining.size()), ie);
        shuffleErrorCounterGroup.connectionErrs.increment(1);
      } else {
        LOG.warn(String.format(
            "Failed to verify reply after connecting from %s to %s with %d inputs pending",
            fetcherConfig.localHostName, host, srcAttemptsRemaining.size()), ie);
      }

      // At this point, either the connection failed, or the initial header verification failed.
      // The error does not relate to any specific Input. Report all of them as failed.
      // This ends up indirectly penalizing the host (multiple failures reported on the single host)
      for (InputAttemptIdentifier left : srcAttemptsRemaining.values()) {
        // Need to be handling temporary glitches ..
        // Report read error to the AM to trigger source failure heuristics
        shuffleSchedulerServer.fetchFailed(shuffleSchedulerId, host, left,
            connectSucceeded, !connectSucceeded);
      }
      return false;
    }
  }

  private InputAttemptIdentifier[] copyMapOutput(DataInputStream input,
      InputAttemptIdentifier inputAttemptIdentifier) throws FetcherReadTimeoutException {
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
          //Read the shuffle header
          ShuffleHeader header = new ShuffleHeader();
          // TODO Review: Multiple header reads in case of status WAIT ?
          header.readFields(input);
          if (!header.mapId.startsWith(InputAttemptIdentifier.PATH_PREFIX_MR3) && !header.mapId.startsWith(InputAttemptIdentifier.PATH_PREFIX)) {
            if (!stopped) {
              shuffleErrorCounterGroup.badIdErrs.increment(1);
              if (header.mapId.startsWith(ShuffleHandlerError.DISK_ERROR_EXCEPTION.toString())) {
                LOG.warn("ShuffleHandler error - " + header.mapId + ", while fetching " + inputAttemptIdentifier);
                shuffleSchedulerServer.informAM(shuffleSchedulerId, inputAttemptIdentifier);
              } else {
                LOG.warn("Invalid map id: " + header.mapId + ", expected to start with " +
                    InputAttemptIdentifier.PATH_PREFIX_MR3 + "/" + InputAttemptIdentifier.PATH_PREFIX + ", partition: " + header.forReduce);
              }
              return new InputAttemptIdentifier[]{getNextRemainingAttempt()};
            } else {
              if (LOG.isDebugEnabled()) {
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
            LOG.warn("Invalid map id ", e);
            // Don't know which one was bad, so consider this one bad and dont read
            // the remaining because we dont know where to start reading from. YARN-1773
            return new InputAttemptIdentifier[]{getNextRemainingAttempt()};
          } else {
            if (LOG.isDebugEnabled()) {
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
              srcAttemptId = getNextRemainingAttempt();
              LOG.warn("Was expecting " + srcAttemptId + " but got null");
            }
            assert (srcAttemptId != null);
            return new InputAttemptIdentifier[]{srcAttemptId};
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Already stopped. Ignoring verification failure.");
            }
            return EMPTY_ATTEMPT_ID_ARRAY;
          }
        }

        if (LOG.isDebugEnabled()) {
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
            LOG.error(logIdentifier + " : Shuffle failed : caused by local error", e);
            exceptionReporter.reportException(e);
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Already stopped. Ignoring error from merger.reserve");
            }
          }
          return EMPTY_ATTEMPT_ID_ARRAY;
        }

        // Check if we can shuffle *now* ...
        if (mapOutput.getType() == Type.WAIT) {
          LOG.info("fetcher# {} - MergerManager returned Status.WAIT ...", fetcherIdentifier);
          //Not an error but wait to process data.
          return EMPTY_ATTEMPT_ID_ARRAY;
        }

        // Go!
        if (LOG.isDebugEnabled()) {
          LOG.debug("fetcher#" + fetcherIdentifier + " about to shuffle output of map " +
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
      srcAttemptsRemaining.remove(inputAttemptIdentifier.getUniqueId());
    } catch (IOException | InternalError ioe) {
      if (stopped) {
        if (LOG.isDebugEnabled()) {
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
        //release mem/file handles
        if (mapOutput != null) {
          mapOutput.abort();
        }
        throw new FetcherReadTimeoutException(ioe);
      }
      shuffleErrorCounterGroup.ioErrs.increment(1);
      if (srcAttemptId == null || mapOutput == null) {
        LOG.info("fetcher#{} failed to read map header{} decomp: {}, {}",
          fetcherIdentifier, srcAttemptId, decompressedLength, compressedLength, ioe);
        if (srcAttemptId == null) {
          return srcAttemptsRemaining.values().toArray(new InputAttemptIdentifier[srcAttemptsRemaining.values().size()]);
        } else {
          return new InputAttemptIdentifier[]{srcAttemptId};
        }
      }
      LOG.warn("Failed to shuffle output of {} from {}", srcAttemptId, host, ioe);

      // Inform the ShuffleScheduler
      mapOutput.abort();
      return new InputAttemptIdentifier[] {srcAttemptId};
    }
    return null;
  }


  private void setupLocalDiskFetch() {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Fetcher " + fetcherIdentifier + " going to fetch (local disk) from " + host + " for: "
          + srcAttempts + ", partition range: " + minPartition + "-" + maxPartition);
    }

    // List of maps to be fetched yet
    populateRemainingMap(srcAttempts);

    final Iterator<InputAttemptIdentifier> iter = srcAttemptsRemaining.values().iterator();
    while (iter.hasNext()) {
      // Avoid fetching more if already stopped
      if (stopped) {
        return;
      }
      InputAttemptIdentifier srcAttemptId = iter.next();
      MapOutput mapOutput = null;
      boolean hasFailures = false;
      // Fetch partition count number of map outputs (handles auto-reduce case)
      for (int reduceId = minPartition; reduceId <= maxPartition; reduceId++) {
        try {
          long startTime = System.currentTimeMillis();
          srcAttemptId = pathToAttemptMap.get(new PathPartition(srcAttemptId.getPathComponent(), reduceId));
          Path filename = getShuffleInputFileName(srcAttemptId.getPathComponent(), "");
          TezIndexRecord indexRecord = getIndexRecord(srcAttemptId.getPathComponent(), reduceId);
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
            shuffleSchedulerServer.fetchFailed(shuffleSchedulerId, host, srcAttemptId, true, false);
            LOG.warn("Failed to read local disk output of " + srcAttemptId + " from " + host, e);
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "Ignoring fetch error during local disk copy since fetcher has already been stopped");
            }
            return;
          }

        }
      }
      if (!hasFailures) {
        iter.remove();
      }
    }
  }

  private void populateRemainingMap(List<InputAttemptIdentifier> origlist) {
    if (srcAttemptsRemaining == null) {
      srcAttemptsRemaining = new LinkedHashMap<Long, InputAttemptIdentifier>(origlist.size());
    }
    for (InputAttemptIdentifier id : origlist) {
      srcAttemptsRemaining.put(id.getUniqueId(), id);
    }
  }

  private InputAttemptIdentifier getNextRemainingAttempt() {
    if (srcAttemptsRemaining.size() > 0) {
      return srcAttemptsRemaining.values().iterator().next();
    } else {
      return null;
    }
  }

  private void populatePathToAttemptMap(List<InputAttemptIdentifier> srcAttempts) {
    for (InputAttemptIdentifier in : srcAttempts) {
      if (in instanceof CompositeInputAttemptIdentifier) {
        CompositeInputAttemptIdentifier cin = (CompositeInputAttemptIdentifier)in;
        for (int i = 0; i < cin.getInputIdentifierCount(); i++) {
          pathToAttemptMap.put(new PathPartition(cin.getPathComponent(), minPartition + i), cin.expand(i));
        }
      } else {
        pathToAttemptMap.put(new PathPartition(in.getPathComponent(), 0), in);
      }
    }
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
        if (LOG.isDebugEnabled()) {
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
      LOG.warn(logIdentifier + " invalid lengths in map output header: id: " +
          srcAttemptId + " len: " + compressedLength + ", decomp len: " +
          decompressedLength);
      return false;
    }

    // partitionId verification. Isn't available here because it is encoded into URI
    if (forReduce < minPartition || forReduce > maxPartition) {
      shuffleErrorCounterGroup.wrongReduceErrs.increment(1);
      LOG.warn(logIdentifier + " data for the wrong partition map: " + srcAttemptId + " len: "
          + compressedLength + " decomp len: " + decompressedLength + " for partition " + forReduce
          + ", expected partition range: " + minPartition + "-" + maxPartition);
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
      LOG.warn("Shuffle output from " + host + " failed, retry it.");
      //retry connecting to the host
      return true;
    } else {
      // timeout, prepare to be failed.
      LOG.warn("Timeout for copying MapOutput with retry on host " + host
          + "after " + fetcherConfig.httpConnectionParams.getReadTimeout() + "milliseconds.");
      return false;
    }
  }
}
