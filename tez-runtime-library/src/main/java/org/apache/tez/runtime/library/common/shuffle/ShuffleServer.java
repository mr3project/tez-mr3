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

package org.apache.tez.runtime.library.common.shuffle;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.impl.FetcherUnordered;
import org.apache.tez.runtime.library.common.shuffle.impl.ShuffleManager;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.FetcherOrderedGrouped;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MapOutput;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleScheduler;
import org.apache.tez.runtime.library.utils.CodecUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ShuffleServer implements FetcherCallback {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServer.class);

  private static volatile ShuffleServer INSTANCE;

  public static ShuffleServer createInstance(
      TaskContext context, Configuration conf) throws IOException {
    int numFetchers = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_TOTAL_PARALLEL_COPIES,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_TOTAL_PARALLEL_COPIES_DEFAULT);
    INSTANCE = new ShuffleServer(context, conf, numFetchers, "ShuffleServer");
    return INSTANCE;
  }

  public static ShuffleServer getInstance() throws IOException {
    if (INSTANCE == null) {
      throw new IOException("ShuffleServer not found");
    }
    return INSTANCE;
  }

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
      sb.append("]");
      return sb.toString();
    }
  }

  public static class PathPartition {

    final String path;
    final int partition;

    public PathPartition(String path, int partition) {
      this.path = path;
      this.partition = partition;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((path == null) ? 0 : path.hashCode());
      result = prime * result + partition;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      PathPartition other = (PathPartition) obj;
      if (path == null) {
        if (other.path != null)
          return false;
      } else if (!path.equals(other.path))
        return false;
      if (partition != other.partition)
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "PathPartition [path=" + path + ", partition=" + partition + "]";
    }
  }

  private final TaskContext taskContext;
  private final Configuration conf;
  private final int numFetchers;
  private final String serverName;

  private final ListeningExecutorService fetcherExecutor;
  private final FetcherConfig fetcherConfig;

  // taskContext.useShuffleHandlerProcessOnK8s() == false:
  //   do not use localShufflePorts[] because taskContext.getServiceProviderMetaData(auxiliaryService)
  //   may not be valid yet (which becomes valid only after all ShuffleHandlers start)
  // taskContext.useShuffleHandlerProcessOnK8s() == true:
  //   initialize localShufflePorts[] which is constant in all ContainerWorkers
  public final int[] localShufflePorts;

  private final int maxTaskOutputAtOnce;

  // TODO: knownSrcHosts[] is never removed from, so check for memory-leak in public clouds
  // Invariant: pendingHosts[] \subset knownSrcHosts.InputHost[]
  private final ConcurrentMap<HostPort, InputHost> knownSrcHosts;
  private final Set<Fetcher> runningFetchers;   // thread-safe because we use ConcurrentHashMap

  private final AtomicLong shuffleClientCount = new AtomicLong(0L);
  protected final ConcurrentMap<Long, ShuffleClient<?>> shuffleClients;

  private final BlockingQueue<InputHost> pendingHosts;
  // required to be held when manipulating pendingHosts[]
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition wakeLoop = lock.newCondition();

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  public ShuffleServer(
      TaskContext taskContext,
      Configuration conf,
      int numFetchers,
      String serverName) throws IOException {
    this.taskContext = taskContext;
    this.conf = conf;
    this.numFetchers = numFetchers;
    this.serverName = serverName;

    final ExecutorService fetcherRawExecutor;
    fetcherRawExecutor = Executors.newFixedThreadPool(numFetchers, new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(taskContext.getUniqueIdentifier() + " Fetcher" + " #%d")
        .build());
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

    /**
     * Setting to very high val can lead to Http 400 error. Cap it to 75; every attempt id would
     * be approximately 48 bytes; 48 * 75 = 3600 which should give some room for other info in URL.
     */
    this.maxTaskOutputAtOnce = Math.max(1, Math.min(75, conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE_DEFAULT)));

    knownSrcHosts = new ConcurrentHashMap<HostPort, InputHost>();
    runningFetchers = Collections.newSetFromMap(new ConcurrentHashMap<Fetcher, Boolean>());
    shuffleClients = new ConcurrentHashMap<Long, ShuffleClient<?>>();
    pendingHosts = new LinkedBlockingQueue<InputHost>();

    LOG.info("{} Configuration: numFetchers={}, maxTaskOutputAtOnce={}, FetcherConfig={}",
        serverName, numFetchers, maxTaskOutputAtOnce, fetcherConfig);
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

  public void run() {
    try {
      call();
      LOG.info("{} thread completed", serverName);
    } catch (Throwable th) {
      if (isShutdown.get()) {
        LOG.error("{} already shutdown. Ignoring error: ", serverName, th);
      } else {
        LOG.error("{} failed with error: ", serverName, th);
      }
    }
  }

  // if true, continue to scan pendingInputs[]
  // if false, every Fetcher is either full or with no partition range, so no need to scan pendingInputs[]
  private boolean existsShouldScanPendingInputs() {
    Iterator<Map.Entry<Long, ShuffleClient<?>>> iterator = shuffleClients.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, ShuffleClient<?>> entry = iterator.next();
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
        while (!isShutdown.get() &&
            (runningFetchers.size() >= numFetchers ||
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
        // for every ShuffleClient,
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
        // even when fetcher == null, inputHost may have inputs if 'ShuffleClient == null'
        inputHost.addToPendingHostsIfNecessary(pendingHosts);
        if (fetcher == null) {
          peekInputHost = pendingHosts.peek();
          continue;
        }

        runningFetchers.add(fetcher);
        fetcher.getShuffleClient().fetcherStarted();

        ListenableFuture<FetchResult> future = fetcherExecutor.submit(fetcher);
        Futures.addCallback(future, new FetchFutureCallback(fetcher));

        count += 1;
        peekInputHost = pendingHosts.peek();
      }
    }

    LOG.info("Shutting down {}, Interrupted: {}", serverName, Thread.currentThread().isInterrupted());
    if (!fetcherExecutor.isShutdown()) {
      fetcherExecutor.shutdownNow();
    }
  }

  private Fetcher constructFetcherForHost(InputHost inputHost, Configuration conf) {
    InputHost.PartitionToInputs pendingInputs = inputHost.clearAndGetOnePartitionRange(
        shuffleClients, maxTaskOutputAtOnce);
    if (pendingInputs == null) {
      // assert { inputHost.partitionToInputs.keys.forall { s => !shuffleClients[s].shouldScanPendingInputs() } }
      // is not valid because some Fetcher may have returned
      return null;
    }

    long shuffleClientId = pendingInputs.getShuffleClientId();
    ShuffleClient<?> shuffleClient = shuffleClients.get(shuffleClientId);
    if (shuffleClient == null) {
      // this can happen if ShuffleServer.unregister() is called after obtaining pendingInputs
      LOG.warn("ShuffleClient {} already unregistered, ignoring {}", shuffleClientId, pendingInputs);
      // remaining mappings in inputHost that use shuffleClientId are removed when this method is called again
      return null;
    }

    boolean removedAnyInput = shuffleClient.cleanInputHostForConstructFetcher(pendingInputs);
    if (pendingInputs.getInputs().isEmpty()) {
      return null;
    }

    InputHost.PartitionToInputs[] pendingInputsSeq;
    if (removedAnyInput || (shuffleClient instanceof ShuffleScheduler)) {
      pendingInputsSeq = new InputHost.PartitionToInputs[1];
      pendingInputsSeq[0] = pendingInputs;
    } else {
      pendingInputsSeq = inputHost.getPendingInputsSeq(shuffleClient, pendingInputs);
    }
    assert pendingInputsSeq.length > 0;
    assert !pendingInputsSeq[0].getInputs().isEmpty();
    assert Arrays.stream(pendingInputsSeq).allMatch(p ->
        p.getInputs().size() == pendingInputsSeq[0].getInputs().size());
    assert java.util.stream.IntStream.range(0, pendingInputsSeq[0].getInputs().size()).allMatch(index ->
      Arrays.stream(pendingInputsSeq).allMatch(p ->
          p.getInputs().get(index).getPathComponent().equals(
              pendingInputsSeq[0].getInputs().get(index).getPathComponent()))
    );

    if (shuffleClient instanceof ShuffleManager) {
      FetcherUnordered fetcher = constructFetcherUnordered(
          conf, fetcherConfig, taskContext, inputHost, pendingInputsSeq);
      // do not merge assignShuffleClient() to constructFetcher() because
      // type variable T should not appear both in argument types and in return type
      fetcher.assignShuffleClient((ShuffleManager)shuffleClient);
      return fetcher;
    } else {
      assert pendingInputsSeq.length == 1;
      FetcherOrderedGrouped fetcher = constructFetcherOrdered(
        conf, fetcherConfig, taskContext, inputHost, pendingInputsSeq[0]);
      fetcher.assignShuffleClient((ShuffleScheduler)shuffleClient);
      return fetcher;
    }
  }

  public long register(ShuffleClient<?> shuffleClient) {
    long shuffleClientId = shuffleClientCount.getAndIncrement();
    shuffleClients.put(shuffleClientId, shuffleClient);
    LOG.info("Registered ShuffleClient: {}, total={}", shuffleClientId, shuffleClients.size());
    return shuffleClientId;
  }

  public void unregister(long shuffleClientId) {
    // clear InputHost with shuffleClientId
    for (InputHost inputHost: knownSrcHosts.values()) {
      boolean removed = inputHost.clearShuffleClientId(shuffleClientId);
      if (removed) {
        LOG.warn("Cleared InputHost for {}: {}", serverName, shuffleClientId);
      }
    }

    for (Fetcher fetcher: runningFetchers) {
      if (fetcher.useSingleShuffleClientId(shuffleClientId)) {
        LOG.warn("Shutting down active Fetcher for ShuffleManger: {} {}",
            shuffleClientId, fetcher.getFetcherIdentifier());
        fetcher.shutdown();
      }
    }

    ShuffleClient<?> old = shuffleClients.remove(shuffleClientId);
    assert old != null;
    LOG.info("Unregistered ShuffleClient: " + shuffleClientId);
  }

  public void addKnownInput(ShuffleClient<?> shuffleClient, String hostName, int port,
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

    host.addKnownInput(shuffleClient, partitionId,
        srcAttemptIdentifier.getInputIdentifierCount(), srcAttemptIdentifier, pendingHosts);
    lock.lock();
    try {
      wakeLoop.signal();
    } finally {
      lock.unlock();
    }
  }

  public void fetchSucceeded(long shuffleClientId, String host,
                             InputAttemptIdentifier srcAttemptIdentifier,
                             ShuffleInput fetchedInput,
                             long fetchedBytes, long decompressedLength, long copyDuration)
    throws IOException {
    ShuffleClient<?> shuffleClient = shuffleClients.get(shuffleClientId);
    if (shuffleClient == null) {
      LOG.warn("ShuffleClient {} already unregistered, ignoring fetchSucceeded(): {}",
          shuffleClientId, srcAttemptIdentifier);
    } else {
      if (shuffleClient instanceof ShuffleManager) {
        ShuffleManager sc = (ShuffleManager)shuffleClient;
        sc.fetchSucceeded(
            srcAttemptIdentifier, (FetchedInput)fetchedInput, fetchedBytes, decompressedLength, copyDuration);
      } else {
        ShuffleScheduler sc = (ShuffleScheduler)shuffleClient;
        sc.fetchSucceeded(
          srcAttemptIdentifier, (MapOutput)fetchedInput, fetchedBytes, decompressedLength, copyDuration);
      }
    }
  }

  public void fetchFailed(long shuffleClientId, String host,
                          InputAttemptIdentifier srcAttemptIdentifier,
                          boolean readFailed, boolean connectFailed) {
    ShuffleClient<?> shuffleClient = shuffleClients.get(shuffleClientId);
    if (shuffleClient == null) {
      LOG.warn("ShuffleClient {} already unregistered, ignoring fetchFailed: {}",
          shuffleClientId, srcAttemptIdentifier);
    } else {
      shuffleClient.fetchFailed(srcAttemptIdentifier, readFailed, connectFailed);
    }
  }

  public void dagLeaving(int dagIdId) {
    // TODO: currently no action necessary because unregister() is called for every ShuffleClient
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

  private FetcherUnordered constructFetcherUnordered(
      Configuration conf,
      FetcherConfig fetcherConfig,
      TaskContext taskContext,
      InputHost inputHost,
      InputHost.PartitionToInputs[] pendingInputsSeq) {
    FetcherUnordered.FetcherBuilder fetcherBuilder = new FetcherUnordered.FetcherBuilder(this,
      conf, taskContext.getApplicationId(), fetcherConfig, taskContext);
    fetcherBuilder.assignWork(inputHost, pendingInputsSeq);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Created FetcherUnordered for host: " + inputHost.getHost()
        + ", with inputs: " + pendingInputsSeq);
    }
    return fetcherBuilder.build();
  }

  private FetcherOrderedGrouped constructFetcherOrdered(
      Configuration conf,
      FetcherConfig fetcherConfig,
      TaskContext taskContext,
      InputHost inputHost,
      InputHost.PartitionToInputs pendingInputs) {
    return new FetcherOrderedGrouped(this,
      conf, inputHost, pendingInputs, fetcherConfig, taskContext);
  }

  public void informAM(long shuffleSchedulerId, InputAttemptIdentifier srcAttempt) {
    ShuffleScheduler shuffleScheduler = (ShuffleScheduler)shuffleClients.get(shuffleSchedulerId);
    if (shuffleScheduler == null) {
      LOG.warn("ShuffleScheduler {} already unregistered, ignoring informAM(): {}",
        shuffleSchedulerId, srcAttempt);
    } else {
      shuffleScheduler.informAM(srcAttempt);
    }
  }

  public void waitForMergeManager(long shuffleSchedulerId) throws InterruptedException {
    ShuffleScheduler shuffleScheduler = (ShuffleScheduler)shuffleClients.get(shuffleSchedulerId);
    if (shuffleScheduler == null) {
      throw new TezUncheckedException("Unregistered ShuffleScheduler: " + shuffleSchedulerId);
    } else {
      shuffleScheduler.waitForMergeManager();
    }
  }
  private class FetchFutureCallback implements FutureCallback<FetchResult> {

    private final Fetcher fetcher;

    public FetchFutureCallback(Fetcher fetcher) {
      this.fetcher = fetcher;
    }

    private void doBookKeepingForFetcherComplete() {
      fetcher.getShuffleClient().fetcherFinished();
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
        if (result != null) {
          // TODO: originally only for unordered
          assert result.getShuffleManagerId() == fetcher.getShuffleClient().getShuffleClientId();

          Map<InputAttemptIdentifier, InputHost.PartitionRange> pendingInputs = result.getPendingInputs();
          if (pendingInputs != null) {
            HostPort identifier = new HostPort(result.getHost(), result.getPort());
            InputHost inputHost = knownSrcHosts.get(identifier);
            assert inputHost != null;

            for (Map.Entry<InputAttemptIdentifier, InputHost.PartitionRange > input : pendingInputs.entrySet()) {
              InputHost.PartitionRange range = input.getValue();
              inputHost.addKnownInput(fetcher.getShuffleClient(),
                  range.getPartition(), range.getPartitionCount(), input.getKey(), pendingHosts);
            }
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
        // TODO: originally in ordered
        //  - exceptionReporter.reportException(t);
        doBookKeepingForFetcherComplete();
      }
    }
  }
}
