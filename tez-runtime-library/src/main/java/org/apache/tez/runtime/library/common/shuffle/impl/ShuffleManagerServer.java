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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.utils.CodecUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.FetchResult;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.Fetcher;
import org.apache.tez.runtime.library.common.shuffle.Fetcher.FetcherBuilder;
import org.apache.tez.runtime.library.common.shuffle.HostPort;
import org.apache.tez.runtime.library.common.shuffle.InputHost;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ShuffleManagerServer {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleManagerServer.class);

  // parameters required by Fetchers
  public static class FetcherConfig {
    public final Configuration codecConf;
    public final boolean ifileReadAhead;
    public final int ifileReadAheadLength;
    public final int ifileBufferSize;
    public final JobTokenSecretManager jobTokenSecretMgr;
    public final HttpConnectionParams httpConnectionParams;
    public final RawLocalFileSystem localFs;
    public final LocalDirAllocator localDirAllocator;
    public final String localHostName;
    public final boolean localDiskFetchEnabled;
    public final boolean sharedFetchEnabled;   // TODO: necessary?
    public final boolean verifyDiskChecksum;
    public final boolean compositeFetch;
    public final boolean localFetchComparePort;

    public FetcherConfig(
        Configuration codecConf,
        boolean ifileReadAhead,
        int ifileReadAheadLength,
        int ifileBufferSize,
        JobTokenSecretManager jobTokenSecretMgr,
        HttpConnectionParams httpConnectionParams,
        RawLocalFileSystem localFs,
        LocalDirAllocator localDirAllocator,
        String localHostName,
        boolean localDiskFetchEnabled,
        boolean sharedFetchEnabled,
        boolean verifyDiskChecksum,
        boolean compositeFetch,
        boolean localFetchComparePort) {
      this.codecConf = codecConf;
      this.ifileReadAhead = ifileReadAhead;
      this.ifileReadAheadLength = ifileReadAheadLength;
      this.ifileBufferSize = ifileBufferSize;
      this.jobTokenSecretMgr = jobTokenSecretMgr;
      this.httpConnectionParams = httpConnectionParams;
      this.localFs = localFs;
      this.localDirAllocator = localDirAllocator;
      this.localHostName = localHostName;
      this.localDiskFetchEnabled = localDiskFetchEnabled;
      this.sharedFetchEnabled = sharedFetchEnabled;
      this.verifyDiskChecksum = verifyDiskChecksum;
      this.compositeFetch = compositeFetch;
      this.localFetchComparePort = localFetchComparePort;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("[ifileReadAhead=");
      sb.append(ifileReadAhead);
      sb.append(", ifileReadAheadLength=");
      sb.append(ifileReadAheadLength);
      sb.append(", ifileBufferSize=");
      sb.append(ifileBufferSize);
      sb.append(", httpConnectionParams=");
      sb.append(httpConnectionParams);
      sb.append(", localDiskFetchEnabled=");
      sb.append(localDiskFetchEnabled);
      sb.append(", sharedFetchEnabled=");
      sb.append(sharedFetchEnabled);
      sb.append("]");
      return sb.toString();
    }
  }

  private static volatile ShuffleManagerServer INSTANCE;

  public static ShuffleManagerServer createInstance(
      TaskContext context, Configuration conf) throws IOException {
    INSTANCE = new ShuffleManagerServer(context, conf);
    return INSTANCE;
  }

  public static ShuffleManagerServer getInstance() throws IOException {
    if (INSTANCE == null) {
      throw new IOException("ShuffleManagerServer not found");
    }
    return INSTANCE;
  }

  private final TaskContext taskContext;
  private final Configuration conf;

  private final int numFetchers;
  private final ListeningExecutorService fetcherExecutor;
  private final FetcherConfig fetcherConfig;

  // taskContext.useShuffleHandlerProcessOnK8s() == false:
  //   do not use localShufflePorts[] because taskContext.getServiceProviderMetaData(auxiliaryService)
  //   may not be valid yet (which becomes valid only after all ShuffleHandlers start)
  // taskContext.useShuffleHandlerProcessOnK8s() == true:
  //   initialize localShufflePorts[] which is constant in all ContainerWorkers
  public final int[] localShufflePorts;

  // used when choosing lockPath
  private final Path[] localDisks;

  private final int maxTaskOutputAtOnce;

  // TODO: knownSrcHosts[] is never removed from, so check for memory-leak in public clouds
  // Invariant: pendingHosts[] \subset knownSrcHosts.InputHost[]
  private final ConcurrentMap<HostPort, InputHost> knownSrcHosts;
  private final Set<Fetcher> runningFetchers;   // thread-safe because we use ConcurrentHashMap

  private final AtomicLong shuffleManagerCount = new AtomicLong(0L);
  private final ConcurrentMap<Long, ShuffleManager> shuffleManagers;

  private final BlockingQueue<InputHost> pendingHosts;
  // required to be held when manipulating pendingHosts[]
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition wakeLoop = lock.newCondition();

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  public ShuffleManagerServer(TaskContext taskContext, Configuration conf) throws IOException {
    this.taskContext = taskContext;
    this.conf = conf;

    this.numFetchers = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_UNORDERED_PARALLEL_COPIES,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_UNORDERED_PARALLEL_COPIES_DEFAULT);
    final ExecutorService fetcherRawExecutor;
    fetcherRawExecutor = Executors.newFixedThreadPool(numFetchers, new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat(taskContext.getUniqueIdentifier() + " Fetcher_B #%d").build());
    this.fetcherExecutor = MoreExecutors.listeningDecorator(fetcherRawExecutor);
    this.fetcherConfig = CodecUtils.constructFetcherConfig(conf, taskContext);

    if (taskContext.useShuffleHandlerProcessOnK8s()) {
      String auxiliaryService = conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
          TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
      ByteBuffer shuffleMetadata = taskContext.getServiceProviderMetaData(auxiliaryService);
      this.localShufflePorts = ShuffleUtils.deserializeShuffleProviderMetaData(shuffleMetadata);
    } else {
      this.localShufflePorts = null;
    }

    this.localDisks = Iterables.toArray(
        fetcherConfig.localDirAllocator.getAllLocalPathsToRead(".", conf), Path.class);
    Arrays.sort(this.localDisks);

    /**
     * Setting to very high val can lead to Http 400 error. Cap it to 75; every attempt id would
     * be approximately 48 bytes; 48 * 75 = 3600 which should give some room for other info in URL.
     */
    this.maxTaskOutputAtOnce = Math.max(1, Math.min(75, conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE_DEFAULT)));

    knownSrcHosts = new ConcurrentHashMap<HostPort, InputHost>();
    runningFetchers = Collections.newSetFromMap(new ConcurrentHashMap<Fetcher, Boolean>());
    shuffleManagers = new ConcurrentHashMap<Long, ShuffleManager>();
    pendingHosts = new LinkedBlockingQueue<InputHost>();

    LOG.info("Configuration: numFetchers={}, maxTaskOutputAtOnce={}, FetcherConfig={}",
        numFetchers, maxTaskOutputAtOnce, fetcherConfig);
  }

  public int[] getLocalShufflePorts() {
    return localShufflePorts;
  }

  public int getMaxTaskOutputAtOnce() {
    return maxTaskOutputAtOnce;
  }

  public BlockingQueue<InputHost> getPendingHosts() {
    return pendingHosts;
  }

  public void run() throws IOException {
    try {
      call();
      LOG.info("ShuffleManagerServer thread completed");
    } catch (Throwable th) {
      if (isShutdown.get()) {
        LOG.error("ShuffleManagerServer already shutdown. Ignoring error: ", th);
      } else {
        LOG.error("ShuffleManagerServer failed with error: ", th);
      }
    }
  }

  // if true, continue to scan pendingInputs[]
  // if false, every Fetcher is either full or with no partition range, so no need to scan pendingInputs[]
  private boolean existsShouldScanPendingInputs() {
    Iterator<Map.Entry<Long, ShuffleManager>> iterator = shuffleManagers.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, ShuffleManager> entry = iterator.next();
      if (entry.getValue().shouldScanPendingInputs()) {
        return true;
      }
    }
    return false;
  }

  private void call() throws Exception {
    while (!isShutdown.get()) {
      lock.lock();
      try {
        while (!isShutdown.get()
            && (runningFetchers.size() >= numFetchers ||
                pendingHosts.isEmpty() ||
                !existsShouldScanPendingInputs())) {
          wakeLoop.await(1000, TimeUnit.MILLISECONDS);
        }
      } finally {
        lock.unlock();
      }

      int maxFetchersToRun = numFetchers - runningFetchers.size();
      int count = 0;
      InputHost peekInputHost = pendingHosts.peek();
      while (count < maxFetchersToRun &&
             peekInputHost != null &&
             existsShouldScanPendingInputs()) {
        // for every ShuffleManager,
        //   1. 'numPartitionRanges > 0' remains the same until the current thread consumes existing inputs
        //   2. 'numFetchers < maxNumFetchers' remains the same until the current thread creates new Fetchers
        InputHost inputHost;
        try {
          inputHost = peekInputHost.takeFromPendingHosts(pendingHosts);
        } catch (InterruptedException e) {
          if (isShutdown.get()) {
            LOG.info("Interrupted and has been shutdown, breaking out of the loop");
            Thread.currentThread().interrupt();
            break;
          } else {
            throw e;
          }
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Processing InputHost: " + inputHost.toDetailedString());
        }

        Fetcher fetcher = constructFetcherForHost(inputHost, conf);
        // even when fetcher == null, inputHost may have inputs if 'ShuffleManager == null'
        inputHost.addToPendingHostsIfNecessary(pendingHosts);
        if (fetcher == null) {
          peekInputHost = pendingHosts.peek();
          continue;
        }

        runningFetchers.add(fetcher);
        fetcher.getShuffleManager().fetcherStarted();

        ListenableFuture<FetchResult> future = fetcherExecutor.submit(fetcher);
        Futures.addCallback(future, new FetchFutureCallback(fetcher));

        count += 1;
        peekInputHost = pendingHosts.peek();
      }
    }

    LOG.info("Shutting down ShuffleManagerServer, Interrupted: {}", Thread.currentThread().isInterrupted());
    if (!fetcherExecutor.isShutdown()) {
      fetcherExecutor.shutdownNow();
    }
  }

  private Fetcher constructFetcherForHost(InputHost inputHost, Configuration conf) {
    InputHost.PartitionToInputs pendingInputsOfOnePartitionRange =
        inputHost.clearAndGetOnePartitionRange(shuffleManagers);
    if (pendingInputsOfOnePartitionRange == null) {
      // assert { inputHost.partitionToInputs.keys.forall { s => !shuffleManagers[s].shouldScanPendingInputs() } }
      // is not valid because some Fetcher may have returned
      return null;
    }

    long shuffleManagerId = pendingInputsOfOnePartitionRange.getShuffleManagerId();
    ShuffleManager shuffleManager = shuffleManagers.get(shuffleManagerId);
    if (shuffleManager == null) {
      // this can happen if ShuffleManagerServer.unregister() is called after obtaining pendingInputsOfOnePartitionRange
      LOG.warn("ShuffleManager {} already unregistered, ignoring {}", shuffleManagerId, pendingInputsOfOnePartitionRange);
      // remaining mappings in inputHost that use shuffleManagerId are removed when this method is called again
      return null;
    }

    shuffleManager.cleanInputHostForConstructFetcher(pendingInputsOfOnePartitionRange, inputHost);

    Path lockPath = null;
    if (fetcherConfig.sharedFetchEnabled) {
      // pick a single lock disk from host/port hashcode
      final int h = Math.abs(Objects.hashCode(inputHost.getHost(), inputHost.getPort()));
      lockPath = new Path(this.localDisks[h % this.localDisks.length], "locks");
    }

    FetcherBuilder fetcherBuilder = new FetcherBuilder(this,
        conf, taskContext.getApplicationId(), fetcherConfig, lockPath, taskContext);
    fetcherBuilder.assignWork(shuffleManager, inputHost, pendingInputsOfOnePartitionRange);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Created Fetcher for host: " + inputHost.getHost()
          + ", with inputs: " + pendingInputsOfOnePartitionRange);
    }
    return fetcherBuilder.build();
  }

  public long register(ShuffleManager shuffleManager) {
    long shuffleManagerId = shuffleManagerCount.getAndIncrement();
    shuffleManagers.put(shuffleManagerId, shuffleManager);
    LOG.info("Registered ShuffleManager: {}, total={}", shuffleManagerId, shuffleManagers.size());
    return shuffleManagerId;
  }

  public void unregister(long shuffleManagerId) {
    // clear InputHost with shuffleManagerId
    for (InputHost inputHost: knownSrcHosts.values()) {
      boolean removed = inputHost.clearShuffleManagerId(shuffleManagerId);
      if (removed) {
        LOG.warn("Cleared InputHost for ShuffleManager: " + shuffleManagerId);
      }
    }

    for (Fetcher fetcher: runningFetchers) {
      if (fetcher.useSingleShuffleManagerId(shuffleManagerId)) {
        LOG.warn("Shutting down active Fetcher for ShuffleManger: {} {}",
            shuffleManagerId, fetcher.getLogIdentifier());
        fetcher.shutdown();
      }
    }

    ShuffleManager old = shuffleManagers.remove(shuffleManagerId);
    assert old != null;
    LOG.info("Unregistered ShuffleManager: " + shuffleManagerId);
  }

  public void addKnownInput(ShuffleManager shuffleManager, String hostName, int port,
                            CompositeInputAttemptIdentifier srcAttemptIdentifier, int partitionId) {
    HostPort identifier = new HostPort(hostName, port);
    InputHost host = knownSrcHosts.get(identifier);
    if (host == null) {
      host = new InputHost(identifier);
      InputHost old = knownSrcHosts.putIfAbsent(identifier, host);
      if (old != null) {
        host = old;
      }
    }

    host.addKnownInput(shuffleManager, partitionId,
        srcAttemptIdentifier.getInputIdentifierCount(), srcAttemptIdentifier, pendingHosts);
    lock.lock();
    try {
      wakeLoop.signal();
    } finally {
      lock.unlock();
    }
  }

  public void fetchSucceeded(long shuffleManagerId, String host, InputAttemptIdentifier srcAttemptIdentifier,
      FetchedInput fetchedInput, long fetchedBytes, long decompressedLength, long copyDuration)
      throws IOException {
    ShuffleManager shuffleManager = shuffleManagers.get(shuffleManagerId);
    if (shuffleManager == null) {
      LOG.warn("ShuffleManager {} already unregistered, ignoring fetchSucceeded(): {}",
          shuffleManagerId, srcAttemptIdentifier);
    } else {
      shuffleManager.fetchSucceeded(
          host, srcAttemptIdentifier, fetchedInput, fetchedBytes, decompressedLength, copyDuration);
    }
  }

  public void fetchFailed(long shuffleManagerId, String host,
      InputAttemptIdentifier srcAttemptIdentifier, boolean connectFailed) {
    ShuffleManager shuffleManager = shuffleManagers.get(shuffleManagerId);
    if (shuffleManager == null) {
      LOG.warn("ShuffleManager {} already unregistered, ignoring fetchFailed: {}",
          shuffleManagerId, srcAttemptIdentifier);
    } else {
      shuffleManager.fetchFailed(host, srcAttemptIdentifier, connectFailed);
    }
  }

  public void dagLeaving(int dagIdId) {
    // TODO: currently no action necessary because unregister() is called for every ShuffleManager
  }

  public void shutdown() {
    if (!isShutdown.getAndSet(true)) {
      // Shut down any pending fetchers
      LOG.info("Shutting down pending fetchers: {}", runningFetchers.size());
      lock.lock();
      try {
        wakeLoop.signal();
        for (Fetcher fetcher : runningFetchers) {
          try {
            fetcher.shutdown();
          } catch (Exception e) {
            LOG.warn(
                "Error while stopping fetcher during shutdown. Ignoring and continuing. Message={}",
                e.getMessage());
          }
        }
      } finally {
        lock.unlock();
      }

      if (this.fetcherExecutor != null && !this.fetcherExecutor.isShutdown()) {
        this.fetcherExecutor.shutdownNow();   // interrupt all running fetchers
      }
    }
  }

  private class FetchFutureCallback implements FutureCallback<FetchResult> {

    private final Fetcher fetcher;
    
    public FetchFutureCallback(Fetcher fetcher) {
      this.fetcher = fetcher;
    }
    
    private void doBookKeepingForFetcherComplete() {
      fetcher.getShuffleManager().fetcherFinished();
      runningFetchers.remove(fetcher);

      lock.lock();
      try {
        wakeLoop.signal();
      } finally {
        lock.unlock();
      }
    }
    
    @Override
    public void onSuccess(FetchResult result) {
      fetcher.shutdown();
      if (isShutdown.get()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Already shutdown. Ignoring event from fetcher");
        }
      } else {
        Iterable<InputAttemptIdentifier> pendingInputs = result.getPendingInputs();
        assert result.getShuffleManagerId() == fetcher.getShuffleManager().getShuffleManagerId();
        if (pendingInputs != null && pendingInputs.iterator().hasNext()) {
          HostPort identifier = new HostPort(result.getHost(), result.getPort());
          InputHost inputHost = knownSrcHosts.get(identifier);
          assert inputHost != null;
          for (InputAttemptIdentifier input : pendingInputs) {
            inputHost.addKnownInput(fetcher.getShuffleManager(),
                result.getPartition(), result.getPartitionCount(), input, pendingHosts);
          }
        }
        doBookKeepingForFetcherComplete();
      }
    }

    @Override
    public void onFailure(Throwable t) {
      // Unsuccessful - the fetcher may not have shutdown correctly. Try shutting it down.
      fetcher.shutdown();
      if (isShutdown.get()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Already shutdown. Ignoring error from fetcher: " + t);
        }
      } else {
        LOG.error("Fetcher failed with error: ", t);
        doBookKeepingForFetcherComplete();
      }
    }
  }
}
