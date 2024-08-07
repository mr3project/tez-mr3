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

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.http.BaseHttpConnection;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.library.common.shuffle.DiskFetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchResult;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputCallback;
import org.apache.tez.runtime.library.common.shuffle.Fetcher;
import org.apache.tez.runtime.library.common.shuffle.InputHost;
import org.apache.tez.runtime.library.common.shuffle.LocalDiskFetchedInput;
import org.apache.tez.runtime.library.common.shuffle.MemoryFetchedInput;
import org.apache.tez.runtime.library.common.shuffle.ShuffleClient;
import org.apache.tez.runtime.library.common.shuffle.ShuffleServer;
import org.apache.tez.runtime.library.common.shuffle.ShuffleServer.PathPartition;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.api.ShuffleHandlerError;
import org.apache.tez.runtime.library.utils.CodecUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleHeader;  // TODO: relocate
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.exceptions.FetcherReadTimeoutException;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput.Type;

/**
 * Responsible for fetching inputs served by the ShuffleHandler for a single
 * host. Construct using {@link FetcherBuilder}
 */
public class FetcherUnordered extends Fetcher<FetchedInput> {

  private static final Logger LOG = LoggerFactory.getLogger(FetcherUnordered.class);
  private static final AtomicInteger fetcherIdGen = new AtomicInteger(0);
  private final boolean isDebugEnabled = LOG.isDebugEnabled();

  private final ShuffleServer fetcherCallback;
  private final Configuration conf;
  private final ApplicationId appId;
  private final ShuffleServer.FetcherConfig fetcherConfig;
  private final TaskContext taskContext;

  private final String host;
  private final int port;

  private final int fetcherIdentifier;
  private final String logIdentifier;

  private final AtomicBoolean isShutDown = new AtomicBoolean(false);

  // Initiative value is 0, which means it hasn't retried yet.
  private long retryStartTime = 0;

  // set in assignShuffleClient()
  // never updated after assignShuffleClient(), so effectively immutable
  private ShuffleManager shuffleManager;
  private long shuffleManagerId;

  private BaseHttpConnection httpConnection;
  private volatile DataInputStream input;

  private CompressionCodec codec;
  private static final ThreadLocal<CompressionCodec> codecHolder = new ThreadLocal<>();

  public FetcherUnordered(ShuffleServer fetcherCallback,
                          Configuration conf, ApplicationId appId,
                          InputHost inputHost,
                          InputHost.PartitionToInputs pendingInputsSeq,
                          ShuffleServer.FetcherConfig fetcherConfig,
                          TaskContext taskContext,
                          ShuffleManager shuffleManager) {
    super(pendingInputsSeq);

    this.fetcherCallback = fetcherCallback;
    this.conf = conf;
    this.appId = appId;
    this.fetcherConfig = fetcherConfig;
    this.taskContext = taskContext;

    this.fetcherIdentifier = fetcherIdGen.getAndIncrement();
    this.logIdentifier = shuffleManager.getLogIdentifier() + "-U-" + fetcherIdentifier;

    this.host = inputHost.getHost();
    this.port = inputHost.getPort();
  }

  public void assignShuffleClient(ShuffleClient<FetchedInput> shuffleClient) {
    this.shuffleManager = (ShuffleManager)shuffleClient;
    this.shuffleManagerId = shuffleClient.getShuffleClientId();
  }

  public ShuffleClient<FetchedInput> getShuffleClient() {
    return shuffleManager;
  }

  public boolean useSingleShuffleClientId(long targetShuffleManagerId) {
    return shuffleManagerId == targetShuffleManagerId;
  }

  public String getFetcherIdentifier() {
    return logIdentifier;
  }

  @Override
  public FetchResult call() throws Exception {
    assert !pendingInputsSeq.getInputs().isEmpty();

    buildPathToAttemptMap();

    codec = codecHolder.get();
    if (codec == null) {
      // clone codecConf because Decompressor uses locks on the Configuration object
      Configuration codecConf = new Configuration(fetcherConfig.codecConf);
      CompressionCodec newCodec = CodecUtils.getCodec(codecConf);
      codec = newCodec;
      codecHolder.set(newCodec);
    }

    HostFetchResult hostFetchResult;
    // ignore ShuffleServer.localShufflePorts[] which is not initialized
    if (fetcherConfig.localDiskFetchEnabled &&
        host.equals(fetcherConfig.localHostName)) {
      hostFetchResult = doLocalDiskFetch();
    } else{
      hostFetchResult = doHttpFetch();
    }

    if (hostFetchResult.failedInputs != null && hostFetchResult.failedInputs.length > 0) {
      if (!isShutDown.get()) {
        LOG.warn("{}: doLocalDisk/HttpFetch() failed for tasks {}",
            logIdentifier, Arrays.toString(hostFetchResult.failedInputs));

        // never add back those InputAttemptIdentifiers that are sent to AM with InputReadError.
        Map<InputAttemptIdentifier, InputHost.PartitionRange> pendingInputs =
            hostFetchResult.fetchResult.getPendingInputs();
        for (InputAttemptIdentifier failed : hostFetchResult.failedInputs) {
          fetcherCallback.fetchFailed(shuffleManagerId, failed, false, hostFetchResult.connectFailed);
          if (pendingInputs != null) {
            pendingInputs.remove(failed);
          }
        }
      } else {
        if (isDebugEnabled) {
          LOG.debug("Ignoring failed fetch reports for " + hostFetchResult.failedInputs.length +
              " inputs since the fetcher has already been stopped");
        }
      }
    }

    shutdown();

    // skip sanity check because we check the invariant in HostFetchResult()

    return hostFetchResult.fetchResult;
  }

  // currentIndex is only for providing pathComponents to be used in URL.
  // success or interrupted --> return null
  // fail --> return non-null
  private HostFetchResult setupConnection(int currentIndex) {
    assert currentIndex < pendingInputsSeq.getInputs().size();

    try {
      String finalHost;
      HttpConnectionParams httpConnectionParams = fetcherConfig.httpConnectionParams;
      if (httpConnectionParams.isSslShuffle()) {
        finalHost = InetAddress.getByName(host).getHostName();
      } else {
        finalHost = host;
      }

      InputHost.PartitionRange range = pendingInputsSeq.getPartitionRange();
      StringBuilder baseURI = ShuffleUtils.constructBaseURIForShuffleHandler(finalHost,
          port, range, appId.toString(), shuffleManager.getDagIdentifier(), httpConnectionParams.isSslShuffle());

      Collection<InputAttemptIdentifier> inputsForPathComponents =
        pendingInputsSeq.getInputs().subList(currentIndex, pendingInputsSeq.getInputs().size());
      // inputsForPathComponents[] is a View, so do not update it
      URL url = ShuffleUtils.constructInputURL(baseURI.toString(), inputsForPathComponents,
          httpConnectionParams.isKeepAlive());

      httpConnection = ShuffleUtils.getHttpConnection(url, httpConnectionParams,
          logIdentifier, fetcherConfig.jobTokenSecretMgr);
      httpConnection.connect();
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      // If connect did not succeed, just mark all the maps as failed.
      InputAttemptIdentifier[] failedFetches;
      Map<InputAttemptIdentifier, InputHost.PartitionRange> pendingInputs;
      if (isShutDown.get()) {
        if (isDebugEnabled) {
          LOG.debug("Not reporting fetch failure during connection establishment, since an Exception was caught after shutdown." +
              e.getClass().getName() + ", Message: " + e.getMessage());
        }
        failedFetches = null;
        pendingInputs = null;
        // do not call getResultWithNoPendingInputsNoFailedInputBecauseAlreadyShutdown() because connectFailed == true
      } else {
        if (fetcherConfig.connectionFailAllInput) {
          // no pending inputs && only failed inputs
          failedFetches = buildInputSeqFromIndex(currentIndex);
          pendingInputs = null;
        } else {
          // all pending inputs, except failedInput == InputAttemptIdentifier at currentIndex
          InputAttemptIdentifier failedFetch =  pendingInputsSeq.getInputs().get(currentIndex);
          failedFetches = new InputAttemptIdentifier[]{ failedFetch };

          pendingInputs = buildInputMapFromIndex(currentIndex);
          // pendingInputs.remove() is optional because call() removes failedFetches[] from pendingInputs[]
          pendingInputs.remove(failedFetch);
        }
      }
      return new HostFetchResult(new FetchResult(
          shuffleManagerId, host, port, pendingInputs),
          failedFetches, true);
    }

    if (isShutDown.get()) {
      // shutdown would have no effect if in the process of establishing the connection.
      shutdownInternal(false);  // everything is okay, but isShutDown == true, so disconnect = false (???)
      if (isDebugEnabled) {
        LOG.debug("Detected fetcher has been shutdown after connection establishment. Returning");
      }
      return getResultWithNoPendingInputsNoFailedInputBecauseAlreadyShutdown();
    }

    try {
      input = httpConnection.getInputStream();
      httpConnection.validate();
      // validateConnectionResponse(msgToEncode, encHash);
    } catch (IOException e) {
      if (isShutDown.get()) {
        if (isDebugEnabled) {
          LOG.debug("Not reporting fetch failure during connection establishment, since an Exception was caught after shutdown." +
              e.getClass().getName() + ", Message: " + e.getMessage());
        }
        return getResultWithNoPendingInputsNoFailedInputBecauseAlreadyShutdown();
      } else {
        LOG.warn("{}: Fetch Failure while connecting from {} to: {}:{}, Informing ShuffleManager",
            logIdentifier, fetcherConfig.localHostName, host, port, e);
        // If we got a read error at this stage, it implies there was a problem with the first map,
        // typically lost map. So, penalize only that map and add the rest.
        // In this way, we can rerun one source Task at a time and avoid rerunning many source Tasks at once.
        // Cf. gla2024.4.8.pptx: What if a ContainerWorker becomes unreachable temporarily?
        // no need to apply TEZ-4174 because we send InputReadError regardless of connectFailed (see ShuffleManager.fetchFailed())
        InputAttemptIdentifier[] failedFetches =
            new InputAttemptIdentifier[]{ pendingInputsSeq.getInputs().get(currentIndex) };
        // It is okay to set pendingInputs[] to include all remaining IAIs, including failedFetches[0],
        // because call() removes failedInputs[0] from pendingInputs[].
        return new HostFetchResult(new FetchResult(
            shuffleManagerId, host, port, buildInputMapFromIndex(currentIndex)),
            failedFetches, false);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();   // reset status
      return null;
    }

    return null;
  }

  private HostFetchResult doHttpFetch() {
    HostFetchResult connectionsWithRetryResult = setupConnection(0);
    if (connectionsWithRetryResult != null) {
      // no InputAttemptIdentifier has been consumed, so return here
      return connectionsWithRetryResult;
    }
    // By this point, the connection is set up and the response has been validated.

    // Handle any shutdown which may have been invoked.
    if (isShutDown.get()) {
      // shutdown would have no effect if in the process of establishing the connection.
      shutdownInternal(false);  // everything is okay, but isShutDown == true, so disconnect = false (???)
      if (isDebugEnabled) {
        LOG.debug("Detected fetcher has been shutdown after opening stream. Returning");
      }
      return getResultWithNoPendingInputsNoFailedInputBecauseAlreadyShutdown();
    }
    // After this point, closing the stream and connection should cause SocketException,
    // which will be ignored since shutdown has been invoked.

    // Loop through available map-outputs and fetch them
    // On any error, failedInputs is not null and we exit
    // after putting back the remaining maps to the
    // yet_to_be_fetched list and marking the failed tasks.
    InputAttemptIdentifier[] failedInputs = null;

    int index = 0;  // points to the next input to be consumed
    while (index < numInputs) {
      InputAttemptIdentifier inputAttemptIdentifier = pendingInputsSeq.getInputs().get(index);

      if (isShutDown.get()) {
        shutdownInternal(true);
        if (isDebugEnabled) {
          LOG.debug("Fetcher already shutdown. Aborting queued fetches for " + (numInputs - index) + " inputs");
        }
        return getResultWithNoPendingInputsNoFailedInputBecauseAlreadyShutdown();
      }

      try {
        // fetchInputs() either:
        //   1. successfully read inputAttemptIdentifier at index, returning null
        //   2. fails to read inputAttemptIdentifier, returning non-null
        failedInputs = fetchInputs(input, inputAttemptIdentifier, index);
        // failedInputs can be:
        //   1. all remaining inputs starting from index
        //   2. inputAttemptIdentifier
        //   3. an individual InputAttemptIdentifier belonging to inputAttemptIdentifier
        //   4. null, error, isShutDown == true
        //   5. null, success, increment index
        if (failedInputs != null) {
          break;
        }
        index++;
      } catch (FetcherReadTimeoutException e) {
        // failed to read inputAttemptIdentifier at index, and retry

        // clean up connection
        shutdownInternal(true);
        if (isShutDown.get()) {
          if (isDebugEnabled) {
            LOG.debug("Fetcher already shutdown. Aborting reconnection and queued fetches for " + (numInputs - index) + " inputs");
          }
          return getResultWithNoPendingInputsNoFailedInputBecauseAlreadyShutdown();
        }

        // try again to connect
        assert failedInputs == null;
        connectionsWithRetryResult = setupConnection(index);
        if (connectionsWithRetryResult != null) {
          // setupConnection() failed, so we should use connectionsWithRetryResult.failedInputs
          // connectionsWithRetryResult.failedInputs can be null if isShutDown == true
          failedInputs = connectionsWithRetryResult.failedInputs;
          // some InputAttemptIdentifiers may have been consumed, so terminate the loop
          break;
        }
        // re-connection okay, so resume the loop with the same index
      }
    }

    // failedInputs[] is valid

    // do not consider the following case because we checked 'failedInputs != null' just after checking isShutDown:
    //   failedInputs != null && failedInputs.length > 0 && isShutDown.get()
    if (index < numInputs) {
      if (isShutDown.get()) {
        if (isDebugEnabled) {
          LOG.debug("Fetcher already shutdown. Aborting reconnection and queued fetches for " + (numInputs - index) + " inputs");
        }
        return getResultWithNoPendingInputsNoFailedInputBecauseAlreadyShutdown();
      }
      return new HostFetchResult(new FetchResult(
          shuffleManagerId, host, port, buildInputMapFromIndex(index)),
          failedInputs, false);
    } else {
      assert failedInputs == null;
      return new HostFetchResult(new FetchResult(
          shuffleManagerId, host, port, null),
          null, false);
    }
  }

  private HostFetchResult getResultWithNoPendingInputsNoFailedInputBecauseAlreadyShutdown() {
    return new HostFetchResult(new FetchResult(
        shuffleManagerId, host, port, null),
        null, false);
  }

  private HostFetchResult doLocalDiskFetch() {
    int partitionId = pendingInputsSeq.getPartition();
    int partitionCount = pendingInputsSeq.getPartitionCount();

    int index = 0;  // points to the next input to be consumed
    List<InputAttemptIdentifier> failedFetches = new ArrayList<>();
    while (index < numInputs) {
      if (isShutDown.get()) {
        if (isDebugEnabled) {
          LOG.debug("Already shutdown. Skipping fetch for " + (numInputs - index) + " inputs");
        }
        return getResultWithNoPendingInputsNoFailedInputBecauseAlreadyShutdown();
      }

      InputAttemptIdentifier inputAttemptIdentifier = pendingInputsSeq.getInputs().get(index);
      String pathComponent = inputAttemptIdentifier.getPathComponent();

      boolean hasFailures = false;  // set to true if errors occur inside the inner loop
      for (int k = 0; k < partitionCount; k++) {
        int reduceId = partitionId + k;
        InputAttemptIdentifier srcAttemptId = pathToAttemptMap.get(new PathPartition(pathComponent, reduceId));
        long startTime = System.currentTimeMillis();

        FetchedInput fetchedInput = null;
        try {
          // for missing files, this will throw an exception
          TezIndexRecord idxRecord = getTezIndexRecord(pathComponent, reduceId);
          fetchedInput = getLocalDiskFetchedInput(srcAttemptId, idxRecord);
          long endTime = System.currentTimeMillis();
          fetcherCallback.fetchSucceeded(shuffleManagerId, host, srcAttemptId, fetchedInput,
              idxRecord.getPartLength(), idxRecord.getRawLength(), (endTime - startTime));
        } catch (IOException | InternalError e) {
          hasFailures = true;
          cleanupFetchedInput(fetchedInput);
          if (isShutDown.get()) {
            if (isDebugEnabled) {
              LOG.debug("Already shutdown. Ignoring local fetch failure for " + srcAttemptId +
                  " from host " + host + " : " + e.getClass().getName() + ", message=" + e.getMessage());
            }
            return getResultWithNoPendingInputsNoFailedInputBecauseAlreadyShutdown();
          }
          LOG.warn("{}: Failed to shuffle output of {} from {} (local fetch)", logIdentifier, srcAttemptId, host, e);
        }
        // do not break out the inner loop
      }

      if (hasFailures) {
        // failed to read some partition belonging to inputAttemptIdentifier inside the inner loop
        failedFetches.add(inputAttemptIdentifier);
      }

      index++;
    }

    assert index == numInputs;
    if (!failedFetches.isEmpty()) {
      if (isShutDown.get()) {
        if (isDebugEnabled) {
          LOG.debug("Already shutdown, not reporting fetch failures for: " + failedFetches.size() +
              " remaining inputs");
        }
        return getResultWithNoPendingInputsNoFailedInputBecauseAlreadyShutdown();
      } else {
        return new HostFetchResult(new FetchResult(
            shuffleManagerId, host, port, null),
            failedFetches.toArray(new InputAttemptIdentifier[failedFetches.size()]), false);
      }
    } else {
      // nothing needs to be done to requeue remaining entries
      return new HostFetchResult(new FetchResult(
          shuffleManagerId, host, port, null),
          null, false);
    }
  }

  private LocalDiskFetchedInput getLocalDiskFetchedInput(
      InputAttemptIdentifier srcAttemptId, TezIndexRecord idxRecord)
      throws IOException {
    LocalDiskFetchedInput fetchedInput = new LocalDiskFetchedInput(idxRecord.getStartOffset(),
      idxRecord.getPartLength(), srcAttemptId,
      getShuffleInputFileName(srcAttemptId.getPathComponent(), null),
      conf,
      new FetchedInputCallback() {
        @Override
        public void fetchComplete(FetchedInput fetchedInput) {
        }

        @Override
        public void fetchFailed(FetchedInput fetchedInput) {
        }

        @Override
        public void freeResources(FetchedInput fetchedInput) {
        }
      });
    if (isDebugEnabled) {
      LOG.debug("fetcher" + " about to shuffle output of srcAttempt (direct disk)" + srcAttemptId
        + " decomp: " + idxRecord.getRawLength() + " len: " + idxRecord.getPartLength()
        + " to " + fetchedInput.getType());
    }

    return fetchedInput;
  }

  private TezIndexRecord getTezIndexRecord(String pathComponent, int partition) throws
      IOException {
    TezIndexRecord idxRecord;
    Path indexFile = getShuffleInputFileName(pathComponent,
        Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING);

    TezSpillRecord spillRecord = new TezSpillRecord(indexFile, fetcherConfig.localFs);
    idxRecord = spillRecord.getIndex(partition);
    return idxRecord;
  }

  private String getMapOutputFile(String pathComponent) {
    return ShuffleUtils.adjustPathComponent(fetcherConfig.compositeFetch, shuffleManager.getDagIdentifier(), pathComponent) +
      Path.SEPARATOR + Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING;
  }

  private Path getShuffleInputFileName(String pathComponent, String suffix)
      throws IOException {
    suffix = suffix != null ? suffix : "";

    String pathFromLocalDir = getMapOutputFile(pathComponent) + suffix;
    return fetcherConfig.localDirAllocator.getLocalPathToRead(pathFromLocalDir, conf);
  }

  static class HostFetchResult {
    private final FetchResult fetchResult;
    private final InputAttemptIdentifier[] failedInputs;
    private final boolean connectFailed;

    public HostFetchResult(FetchResult fetchResult,
                           InputAttemptIdentifier[] failedInputs,
                           boolean connectFailed) {
      this.fetchResult = fetchResult;
      this.failedInputs = failedInputs;
      this.connectFailed = connectFailed;
      assert !(failedInputs == null) || fetchResult.getPendingInputs() == null;
    }
  }

  // called from ShuffleServer or at the end of call()
  public void shutdown() {
    if (!isShutDown.getAndSet(true)) {
      if (isDebugEnabled) {
        LOG.debug("Shutting down fetcher for host: " + host);
      }
      shutdownInternal(false);
    }
  }

  // can be called multiple times
  private void shutdownInternal(boolean disconnect) {
    // Synchronizing on isShutDown to ensure we don't run into a parallel close
    // Can't synchronize on the main class itself since that would cause the
    // shutdown request to block
    synchronized (isShutDown) {
      try {
        if (httpConnection != null) {
          httpConnection.cleanup(disconnect);
        }
      } catch (IOException e) {
        LOG.info("{}: Exception while shutting down: {}", logIdentifier, e.getMessage());
        if (isDebugEnabled) {
          LOG.debug(StringUtils.EMPTY, e);
        }
      }
    }
  }

  private static class MapOutputStat {
    final InputAttemptIdentifier srcAttemptId;
    final long decompressedLength;
    final long compressedLength;
    final int forReduce;

    MapOutputStat(InputAttemptIdentifier srcAttemptId, long decompressedLength, long compressedLength, int forReduce) {
      assert srcAttemptId != null;
      this.srcAttemptId = srcAttemptId;
      this.decompressedLength = decompressedLength;
      this.compressedLength = compressedLength;
      this.forReduce = forReduce;
    }

    @Override
    public String toString() {
      return "id: " + srcAttemptId + ", decompressed length: " + decompressedLength + ", compressed length: " + compressedLength + ", reduce: " + forReduce;
    }
  }

  private InputAttemptIdentifier[] fetchInputs(
      DataInputStream input,
      InputAttemptIdentifier inputAttemptIdentifier,
      int currentIndex) throws FetcherReadTimeoutException {
    FetchedInput fetchedInput = null;
    InputAttemptIdentifier srcAttemptId = null;   // to be constructed from data fetched from ShuffleServer
    long decompressedLength = 0;
    long compressedLength = 0;
    try {
      long startTime = System.currentTimeMillis();
      int partitionCount = 1;

      // read the first part - partitionCount
      if (fetcherConfig.compositeFetch) {
        // multiple partitions are fetched
        partitionCount = WritableUtils.readVInt(input);
      }

      // read the second part - ShuffleHeader[]
      ArrayList<MapOutputStat> mapOutputStats = new ArrayList<>(partitionCount);
      for (int mapOutputIndex = 0; mapOutputIndex < partitionCount; mapOutputIndex++) {
        MapOutputStat mapOutputStat = null;
        int responsePartition = -1;
        // read the shuffle header
        String pathComponent = null;

        // build srcAttemptId and MapOutputStat
        try {
          ShuffleHeader header = new ShuffleHeader();
          header.readFields(input);
          pathComponent = header.getMapId();
          if (!pathComponent.startsWith(InputAttemptIdentifier.PATH_PREFIX_MR3) && !pathComponent.startsWith(InputAttemptIdentifier.PATH_PREFIX)) {
            if (pathComponent.startsWith(ShuffleHandlerError.DISK_ERROR_EXCEPTION.toString())) {
              LOG.warn("{}: ShuffleHandler error - {}, while fetching {}",
                  logIdentifier, pathComponent, inputAttemptIdentifier);
              // this should be treated as local fetch failure in order to send InputReadError
              return new InputAttemptIdentifier[]{ inputAttemptIdentifier };
            }
            throw new IllegalArgumentException("Invalid map id: " + header.getMapId() + ", expected to start with " +
                InputAttemptIdentifier.PATH_PREFIX_MR3 + "/" + InputAttemptIdentifier.PATH_PREFIX + ", partition: " + header.getPartition()
                + " while fetching " + inputAttemptIdentifier);
          }

          srcAttemptId = pathToAttemptMap.get(new PathPartition(pathComponent, header.getPartition()));
          if (srcAttemptId == null) {
            throw new IllegalArgumentException("Source attempt not found for map id: " + header.getMapId() +
                ", partition: " + header.getPartition() + " while fetching " + inputAttemptIdentifier);
          }

          if (header.getCompressedLength() == 0) {
            // empty partitions are already accounted for
            continue;
          }

          mapOutputStat = new MapOutputStat(srcAttemptId,
              header.getUncompressedLength(), header.getCompressedLength(), header.getPartition());
          mapOutputStats.add(mapOutputStat);
          responsePartition = header.getPartition();
        } catch (IllegalArgumentException e) {
          if (!isShutDown.get()) {
            LOG.warn("{}: Invalid src id", logIdentifier, e);
            // don't know which one was bad, so consider all of them (starting from currentIndex) as bad
            return buildInputSeqFromIndex(currentIndex);  // okay because it is IllegalArgumentException
          } else {
            if (isDebugEnabled) {
              LOG.debug("Already shutdown. Ignoring badId error with message: " + e.getMessage());
            }
            return null;
          }
        }

        // Do some basic sanity verification on MapOutputStat
        if (!verifySanity(mapOutputStat.compressedLength, mapOutputStat.decompressedLength,
                responsePartition, mapOutputStat.srcAttemptId, pathComponent)) {
          if (!isShutDown.get()) {
            srcAttemptId = mapOutputStat.srcAttemptId;
            assert srcAttemptId != null;
            return new InputAttemptIdentifier[]{ srcAttemptId };
          } else {
            if (isDebugEnabled) {
              LOG.debug("Already shutdown. Ignoring verification failure.");
            }
            return null;
          }
        }

        if (isDebugEnabled) {
          LOG.debug("header: " + mapOutputStat.srcAttemptId + ", len: " + mapOutputStat.compressedLength
              + ", decomp len: " + mapOutputStat.decompressedLength);
        }
      }

      // read the third part - payload
      for (MapOutputStat mapOutputStat : mapOutputStats) {
        // Get the location for the map output - either in-memory or on-disk
        srcAttemptId = mapOutputStat.srcAttemptId;
        decompressedLength = mapOutputStat.decompressedLength;
        compressedLength = mapOutputStat.compressedLength;
        // TODO TEZ-957. handle IOException here when Broadcast has better error checking
        {
          fetchedInput = shuffleManager.getInputManager().allocate(decompressedLength,
              compressedLength, srcAttemptId);
        }
        // No concept of WAIT at the moment.
        // // Check if we can shuffle *now* ...
        // if (fetchedInput.getType() == FetchedInput.WAIT) {
        // LOG.info("fetcher#" + id + " - MergerManager returned Status.WAIT ...");
        // // Not an error but wait to process data.
        // return EMPTY_ATTEMPT_ID_ARRAY;
        // }

        // Go!
        if (isDebugEnabled) {
          LOG.debug("fetcher" + " about to shuffle output of srcAttempt "
              + fetchedInput.getInputAttemptIdentifier() + " decomp: "
              + decompressedLength + " len: " + compressedLength + " to "
              + fetchedInput.getType());
        }

        if (fetchedInput.getType() == Type.MEMORY) {
          ShuffleUtils.shuffleToMemory(((MemoryFetchedInput) fetchedInput).getBytes(),
              input, (int) decompressedLength, (int) compressedLength, codec,
              fetcherConfig.ifileReadAhead, fetcherConfig.ifileReadAheadLength, LOG,
              fetchedInput.getInputAttemptIdentifier(), taskContext, true);
        } else if (fetchedInput.getType() == Type.DISK) {
          ShuffleUtils.shuffleToDisk(((DiskFetchedInput) fetchedInput).getOutputStream(),
              (host + ":" + port), input, compressedLength, decompressedLength, LOG,
              fetchedInput.getInputAttemptIdentifier(),
              fetcherConfig.ifileReadAhead, fetcherConfig.ifileReadAheadLength, fetcherConfig.verifyDiskChecksum);
        } else {
          throw new TezUncheckedException("Bad fetchedInput type while fetching shuffle data " +
              fetchedInput);
        }

        // Inform the shuffle scheduler
        long endTime = System.currentTimeMillis();
        // Reset retryStartTime as map task make progress if retried before.
        retryStartTime = 0;
        fetcherCallback.fetchSucceeded(shuffleManagerId, host, srcAttemptId, fetchedInput,
            compressedLength, decompressedLength, (endTime - startTime));
      }

      // all success, so return null
    } catch (IOException | InternalError ioe) {
      if (isShutDown.get()) {
        cleanupFetchedInput(fetchedInput);
        if (isDebugEnabled) {
          LOG.debug(
              "Already shutdown. Ignoring exception during fetch " + ioe.getClass().getName() +
                  ", Message: " + ioe.getMessage());
        }
        return null;
      }
      if (shouldRetry(ioe)) {
        // release mem/file handles
        cleanupFetchedInput(fetchedInput);
        throw new FetcherReadTimeoutException(ioe);
      }
      if (srcAttemptId == null || fetchedInput == null) {
        LOG.info("{}: Failed to read map header {} decomp: {}, {}",
            logIdentifier, srcAttemptId, decompressedLength, compressedLength, ioe);
        // Cleanup fetchedInput before returning.
        cleanupFetchedInput(fetchedInput);
        if (srcAttemptId == null) {
          return buildInputSeqFromIndex(currentIndex);
        } else {
          return new InputAttemptIdentifier[]{ srcAttemptId };
        }
      }
      LOG.warn("{}: Failed to shuffle output of {} from {} to {}",
          logIdentifier, srcAttemptId, host, fetcherConfig.localHostName, ioe);

      // Cleanup fetchedInput
      cleanupFetchedInput(fetchedInput);
      return new InputAttemptIdentifier[]{ srcAttemptId };
    }

    return null;
  }

  private void cleanupFetchedInput(FetchedInput fetchedInput) {
    if (fetchedInput != null) {
      try {
        fetchedInput.abort();
      } catch (IOException e) {
        LOG.info("{}: Failure to cleanup fetchedInput {}", logIdentifier, fetchedInput);
      }
    }
  }

  /**
   * Check connection needs to be re-established.
   *
   * @param srcAttemptId
   * @param ioe
   * @return true to indicate connection retry. false otherwise.
   * @throws IOException
   */
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
      LOG.warn("{}: Shuffle output failed to {}, retry it", logIdentifier, fetcherConfig.localHostName);
      // retry connecting to the host
      return true;
    } else {
      // timeout, prepare to be failed.
      LOG.warn("{}: Timeout for copying MapOutput with retry on host {} after {} milliseconds",
          logIdentifier, host, fetcherConfig.httpConnectionParams.getReadTimeout());
      return false;
    }
  }

  /**
   * Do some basic verification on the input received -- Being defensive
   * 
   * @param compressedLength
   * @param decompressedLength
   * @param fetchPartition
   * @param srcAttemptId
   * @param pathComponent
   * @return true/false, based on if the verification succeeded or not
   */
  private boolean verifySanity(long compressedLength, long decompressedLength,
      int fetchPartition, InputAttemptIdentifier srcAttemptId, String pathComponent) {
    if (compressedLength < 0 || decompressedLength < 0) {
      LOG.warn("{}: Invalid lengths in input header -> headerPathComponent: {}, " +
          "mappedSrcAttemptId: {}, len: {}, decomp len: {}",
          logIdentifier, pathComponent, srcAttemptId, compressedLength, decompressedLength);
      return false;
    }

    if (fetchPartition < this.minPartition || fetchPartition > this.maxPartition) {
      LOG.warn("{}: Data for the wrong reduce -> headerPathComponent: {}, " +
              "mappedSrcAttemptId: {}, len: {}, decomp len: {} for reduce {}",
          logIdentifier, pathComponent, srcAttemptId, compressedLength, decompressedLength,
          fetchPartition);
      return false;
    }
    return true;
  }
  
  @Override
  public int hashCode() {
    return fetcherIdentifier;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    FetcherUnordered other = (FetcherUnordered) obj;
    if (fetcherIdentifier != other.fetcherIdentifier)
      return false;
    return true;
  }
}
