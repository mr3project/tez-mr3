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
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.FetcherConfig;
import org.apache.tez.runtime.api.FetcherConfigCommon;
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

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
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
  private final boolean isDebugEnabled = LOG.isDebugEnabled();

  public static ShuffleServer createInstance(
      TaskContext context, Configuration conf) throws IOException {
    int numFetchers = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_TOTAL_PARALLEL_COPIES,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_TOTAL_PARALLEL_COPIES_DEFAULT);
    return new ShuffleServer(context, conf, numFetchers, context.getUniqueIdentifier());
  }

  public static Configuration getCodecConf(Object instance, Configuration conf) {
    // clone because Decompressor uses locks on the Configuration object
    if (instance != null) {
      return new Configuration(((ShuffleServer)instance).fetcherConfigCommon.codecConf);
    } else {
      return new Configuration(conf);
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

  public static enum RangesScheme {
    SCHEME_FIRST,
    SCHEME_MAX
  }

  private final TaskContext taskContext;
  private final int maxNumFetchers;
  private final String serverName;

  private final ListeningExecutorService fetcherExecutor;
  private final FetcherConfigCommon fetcherConfigCommon;

  private final int maxTaskOutputAtOnce;
  private final RangesScheme rangesScheme;

  // to prevent memory-leak in knownSrcHosts[] in public clouds
  private final int maxNumInputHosts;

  // Invariant: pendingHosts[] \subset knownSrcHosts.InputHost[]
  private final ConcurrentMap<HostPort, InputHost> knownSrcHosts;

  private final AtomicLong shuffleClientCount = new AtomicLong(0L);
  protected final ConcurrentMap<Long, ShuffleClient<?>> shuffleClients;
  private final Object registerLock = new Object();

  private final BlockingQueue<InputHost> pendingHosts;

  // for loop in call()
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition wakeLoop = lock.newCondition();

  private final Set<Fetcher<?>> runningFetchers;   // thread-safe because we use ConcurrentHashMap

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  private final ExecutorService shutdownExecutor;

  private static final int LAUNCH_LOOP_WAIT_PERIOD_MILLIS = 1000;
  private static final int CHECK_STUCK_FETCHER_PERIOD_MILLIS = 250;

  public ShuffleServer(
      TaskContext taskContext,
      Configuration conf,
      int numFetchers,
      String serverName) throws IOException {
    this.taskContext = taskContext;
    this.maxNumFetchers = numFetchers;
    this.serverName = serverName;

    final ExecutorService fetcherRawExecutor;
    fetcherRawExecutor = Executors.newFixedThreadPool(numFetchers, new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("Fetcher" + " #%d")
        .build());
    this.fetcherExecutor = MoreExecutors.listeningDecorator(fetcherRawExecutor);
    this.fetcherConfigCommon = CodecUtils.constructFetcherConfigCommon(conf, taskContext);

    /**
     * Setting to very high val can lead to Http 400 error. Cap it to 75; every attempt id would
     * be approximately 48 bytes; 48 * 75 = 3600 which should give some room for other info in URL.
     */
    this.maxTaskOutputAtOnce = Math.max(1, Math.min(75, conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE_DEFAULT)));

    String scheme = conf.get(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_RANGES_SCHEME,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_RANGES_SCHEME_DEFAULT);
    this.rangesScheme = scheme.equalsIgnoreCase("max") ? RangesScheme.SCHEME_MAX : RangesScheme.SCHEME_FIRST;

    knownSrcHosts = new ConcurrentHashMap<HostPort, InputHost>();

    shuffleClients = new ConcurrentHashMap<Long, ShuffleClient<?>>();
    pendingHosts = new LinkedBlockingQueue<InputHost>();

    runningFetchers = Collections.newSetFromMap(new ConcurrentHashMap<Fetcher<?>, Boolean>());

    this.maxNumInputHosts = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_INPUT_HOSTPORTS,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_INPUT_HOSTPORTS_DEFAULT);

    this.shutdownExecutor = Executors.newSingleThreadExecutor();

    LOG.info("{} Configuration: numFetchers={}, maxTaskOutputAtOnce={}, FetcherConfigCommon={}, rangesScheme={}, maxNumInputHosts={}",
        serverName, numFetchers, maxTaskOutputAtOnce, fetcherConfigCommon, rangesScheme, maxNumInputHosts);
  }

  public int getMaxTaskOutputAtOnce() {
    return maxTaskOutputAtOnce;
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

  // variables local to call()
  private boolean shouldLaunchNewFetchers;
  private boolean existsFetcherToRetry;
  private boolean existsFetcherFromStuckToRecovered;
  private boolean existsFetcherFromStuckToSpeculative;
  private long nextCheckStuckFetcherMillis;
  private boolean shouldCheckStuckFetcher;

  private boolean getShouldLaunchNewFetchers() {
    return
      !pendingHosts.isEmpty() &&
      pendingHosts.stream().anyMatch(p ->
          p.isHostNormal() &&
          p.hasFetcherToLaunch(shuffleClients));
  }

  private void updateLoopConditions() {
    final long currentMillis = System.currentTimeMillis();

    int currentNumFetchers = runningFetchers.size();
    shouldLaunchNewFetchers =
        currentNumFetchers < maxNumFetchers &&
        getShouldLaunchNewFetchers();

    existsFetcherToRetry = runningFetchers.stream().anyMatch(f -> {
        FetcherConfig fetcherConfig = f.fetcherConfig;
        int state = f.getState();
        return
          (state == Fetcher.STATE_NORMAL || state == Fetcher.STATE_RECOVERED) &&
          (currentMillis - f.getStartMillis() >= fetcherConfig.speculativeExecutionWaitMillis);
      });

    existsFetcherFromStuckToRecovered = runningFetchers.stream().anyMatch(f ->
        f.getState() == Fetcher.STATE_STUCK &&
        f.getStage() == Fetcher.STAGE_FIRST_FETCHED);

    existsFetcherFromStuckToSpeculative = runningFetchers.stream().anyMatch(f -> {
      FetcherConfig fetcherConfig = f.fetcherConfig;
      final int STUCK_FETCHER_RELEASE_MILLIS = fetcherConfig.stuckFetcherReleaseMillis;
      return
        f.getState() == Fetcher.STATE_STUCK &&
        f.getStage() < Fetcher.STAGE_FIRST_FETCHED &&
        (currentMillis - f.getStartMillis() >= STUCK_FETCHER_RELEASE_MILLIS);
    });

    shouldCheckStuckFetcher = currentMillis > nextCheckStuckFetcherMillis;
  }

  private void call() throws Exception {
    long initialMillis = System.currentTimeMillis();
    nextCheckStuckFetcherMillis = initialMillis + CHECK_STUCK_FETCHER_PERIOD_MILLIS;
    while (!isShutdown.get()) {
      lock.lock();
      try {
        updateLoopConditions();
        while (!isShutdown.get() &&
               !shouldLaunchNewFetchers &&
               !existsFetcherToRetry &&
               !existsFetcherFromStuckToRecovered &&
               !existsFetcherFromStuckToSpeculative &&
               !shouldCheckStuckFetcher) {
          wakeLoop.await(LAUNCH_LOOP_WAIT_PERIOD_MILLIS, TimeUnit.MILLISECONDS);
          updateLoopConditions();
        }
      } finally {
        lock.unlock();
      }

      if (isDebugEnabled) {
        LOG.debug("ShuffleServer loop: existsFetcherToRetry={}, existsFetcherFromStuckToRecovered={}, existsFetcherFromStuckToSpeculative={}, shouldLaunchNewFetchers={}, shouldCheckStuckFetchers={}",
            existsFetcherToRetry, existsFetcherFromStuckToRecovered, existsFetcherFromStuckToSpeculative,
            shouldLaunchNewFetchers, shouldCheckStuckFetcher);
      }

      final long currentMillis = System.currentTimeMillis();

      if (existsFetcherToRetry) {
        // transition: from NORMAL/RECOVERED to RETRY
        runningFetchers.forEach(fetcher -> {
          FetcherConfig fetcherConfig = fetcher.fetcherConfig;
          int state = fetcher.getState();
          long elapsed = currentMillis - fetcher.getStartMillis();
          if ((state == Fetcher.STATE_NORMAL || state == Fetcher.STATE_RECOVERED) &&
              elapsed >= fetcherConfig.speculativeExecutionWaitMillis) {
            // this assert{} can be false with wrong configs:
            //   assert fetcher.getStage() == Fetcher.STAGE_FIRST_FETCHED;
            boolean result = fetcher.trySetStateRETRY();
            // result == false if fetcher.state == COMPLETED
            if (result) {
              // There is a slight chance that this fetcher is in COMPLETED.
              // This is not a problem because the resul of the new speculative fetcher is ignored.
              LOG.warn("Fetcher to RETRY: {} in {}ms", fetcher.getFetcherIdentifier(), elapsed);
              trySpeculativeFetcher(fetcher);
            }
          }
        });
      }

      if (existsFetcherFromStuckToRecovered) {
        // transition: from STUCK to RECOVERED
        runningFetchers.forEach(fetcher -> {
          if (fetcher.getState() == Fetcher.STATE_STUCK &&
              fetcher.getStage() == Fetcher.STAGE_FIRST_FETCHED) {
            boolean result = fetcher.trySetStateRECOVERED();
            if (result) {
              // This thread is responsible for calling removeHostBlocked().
              removeHostBlocked(fetcher);
              LOG.info("Fetcher STUCK to RECOVERED: {} in stage {}",
                  fetcher.getFetcherIdentifier(), Fetcher.STAGE_FIRST_FETCHED);
            }
          }
        });
      }

      if (existsFetcherFromStuckToSpeculative) {
        // try to transition: from STUCK to SPECULATIVE
        runningFetchers.forEach(fetcher -> {
          FetcherConfig fetcherConfig = fetcher.fetcherConfig;
          final int STUCK_FETCHER_RELEASE_MILLIS = fetcherConfig.stuckFetcherReleaseMillis;
          if (fetcher.getState() == Fetcher.STATE_STUCK &&
              fetcher.getStage() != Fetcher.STAGE_FIRST_FETCHED &&
              (currentMillis - fetcher.getStartMillis()) >= STUCK_FETCHER_RELEASE_MILLIS) {
            boolean result = fetcher.trySetStateSPECULATIVE();
            if (result) {
              // This thread is responsible for calling removeHostBlocked().
              removeHostBlocked(fetcher);
              LOG.warn("Fetcher STUCK to SPECULATIVE: {} in stage {}",
                  fetcher.getFetcherIdentifier(), fetcher.getStage());
              trySpeculativeFetcher(fetcher);
            }
          }
        });
      }

      if (shouldCheckStuckFetcher) {
        // try to transition: from NORMAL with stage == INITIAL to STUCK
        runningFetchers.forEach(fetcher -> {
          FetcherConfig fetcherConfig = fetcher.fetcherConfig;
          final int STUCK_FETCHER_THRESHOLD_MILLIS = fetcherConfig.stuckFetcherThresholdMillis;
          if (fetcher.getState() == Fetcher.STATE_NORMAL) {
            long elapsed = currentMillis - fetcher.getStartMillis();
            if (elapsed > STUCK_FETCHER_THRESHOLD_MILLIS &&
                fetcher.getStage() != Fetcher.STAGE_FIRST_FETCHED) {
              boolean result = fetcher.trySetStateSTUCKaddHostBlocked();
              if (result) {
                LOG.warn("Fetcher NORMAL to STUCK: {} in stage {}, {}, {}ms",
                    fetcher.getFetcherIdentifier(), fetcher.getStage(),
                    fetcher.inputHost.getHostPort(), currentMillis - fetcher.getStartMillis());
              }
            }
          }
        });

        // reset nextCheckStuckFetchers
        nextCheckStuckFetcherMillis = currentMillis + CHECK_STUCK_FETCHER_PERIOD_MILLIS;
      }

      if (shouldLaunchNewFetchers) {
        // speculative Fetcher may have been launched and some Fetcher may been finished,
        // so we cannot reuse currentNumFetchers in updateLoopConditions()
        int initialNumFetchers = runningFetchers.size();
        int maxFetchersToRun = maxNumFetchers - initialNumFetchers;

        int numNewFetchers = 0;
        InputHost peekInputHost = pendingHosts.peek();
        while (getShouldLaunchNewFetchers() &&
               numNewFetchers < maxFetchersToRun &&
               peekInputHost != null) {
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

          if (inputHost.isHostNormal()) {
            Fetcher<?> fetcher = constructFetcherForHost(inputHost);
            // even when fetcher == null, inputHost may still have inputs if 'ShuffleClient == null'
            if (fetcher != null) {
              runFetcher(fetcher);
              numNewFetchers += 1;
            }
          }
          inputHost.addToPendingHostsIfNecessary(pendingHosts);

          peekInputHost = pendingHosts.peek();
        }

        if (isDebugEnabled) {
          LOG.debug("Fetcher launched={}, runningFetchers={}", numNewFetchers, runningFetchers.size());
        }
      }

      // end of while{} loop
    }

    LOG.info("Shutting down {}, Interrupted: {}", serverName, Thread.currentThread().isInterrupted());
    if (!fetcherExecutor.isShutdown()) {
      fetcherExecutor.shutdownNow();
    }
  }

  private void trySpeculativeFetcher(Fetcher<?> fetcher) {
    // create a speculative fetcher only if its ShuffleClient is still alive
    FetcherConfig fetcherConfig = fetcher.fetcherConfig;
    if (fetcher.attempt < fetcherConfig.maxSpeculativeFetchAttempts &&
      shuffleClients.get(fetcher.getShuffleClient().getShuffleClientId()) != null) {
      Fetcher<?> speculativeFetcher = fetcher.createClone();
      runFetcher(speculativeFetcher);   // incurs concurrent modification
      LOG.warn("Retrying execution of Fetcher: {} to {}",
        fetcher.getFetcherIdentifier(), speculativeFetcher.getFetcherIdentifier());
    }
  }

  // can be called twice for the same Fetcher: from call() and doBookKeepingForFetcherComplete()
  private void removeHostBlocked(Fetcher<?> fetcher) {
    fetcher.inputHost.removeHostBlocked(fetcher);
  }

  private void runFetcher(Fetcher<?> fetcher) {
    runningFetchers.add(fetcher);
    fetcher.getShuffleClient().fetcherStarted();
    ListenableFuture<FetchResult> future = fetcherExecutor.submit(fetcher);
    Futures.addCallback(future, new FetchFutureCallback(fetcher));
  }

  private Fetcher<?> constructFetcherForHost(InputHost inputHost) {
    InputHost.PartitionToInputs pendingInputs = inputHost.clearAndGetOnePartitionRange(
        shuffleClients, maxTaskOutputAtOnce, rangesScheme);
    if (pendingInputs == null) {
      // assert { inputHost.partitionToInputs.keys.forall { s => !shuffleClients[s].shouldScanPendingInputs() } }
      // is not valid because some Fetcher may have returned
      return null;
    }

    Long shuffleClientId = pendingInputs.getShuffleClientId();
    ShuffleClient<?> shuffleClient = shuffleClients.get(shuffleClientId);
    if (shuffleClient == null) {
      // this can happen if ShuffleServer.unregister() is called after obtaining pendingInputs
      LOG.warn("ShuffleClient {} already unregistered, ignoring {}", shuffleClientId, pendingInputs);
      // remaining mappings in inputHost that use shuffleClientId are removed when this method is called again
      return null;
    }

    boolean removedAnyInput = shuffleClient.cleanInputHostForConstructFetcher(pendingInputs);
    if (pendingInputs.getInputs().isEmpty()) {
      assert removedAnyInput;
      return null;
    }

    FetcherConfig fetcherConfig = shuffleClient.getFetcherConfig();
    if (shuffleClient instanceof ShuffleManager) {
      return new FetcherUnordered(this,
          shuffleClient.conf, inputHost, pendingInputs, fetcherConfigCommon, fetcherConfig, taskContext,
          0, (ShuffleManager)shuffleClient);
    } else {
      return new FetcherOrderedGrouped(this,
          shuffleClient.conf, inputHost, pendingInputs, fetcherConfigCommon, fetcherConfig, taskContext,
          0, (ShuffleScheduler)shuffleClient);
    }
  }

  public void wakeupLoop() {
    lock.lock();
    try {
      wakeLoop.signal();
    } finally {
      lock.unlock();
    }
  }

  public Long register(ShuffleClient<?> shuffleClient) {
    Long shuffleClientId = Long.valueOf(shuffleClientCount.getAndIncrement());
    synchronized (registerLock) {
      shuffleClients.put(shuffleClientId, shuffleClient);
    }
    LOG.info("Registered ShuffleClient for {}: {}, total={}", shuffleClient.getLogIdentifier(), shuffleClientId, shuffleClients.size());
    return shuffleClientId;
  }

  public void unregister(Long shuffleClientId) {
    // clear InputHost with shuffleClientId
    for (InputHost inputHost: knownSrcHosts.values()) {
      inputHost.clearShuffleClientId(shuffleClientId);
    }
    // but some Fetcher for shuffleClientId may have been chosen in constructFetcherForHost() and get executed later

    // add()/remove() can be called while traversing
    // add() with Fetcher for shuffleClientId is okay:
    //   this Fetcher is orphaned because ShuffleClient is gone.
    //   later, it will be removed from runningFetchers[] when it is finished.
    runningFetchers.forEach(fetcher -> {
      if (fetcher.useSingleShuffleClientId(shuffleClientId)) {
        LOG.warn("Shutting down running Fetcher for ShuffleClient {}: {}",
            shuffleClientId, fetcher.getReportStatus());
        // fetcher.shutdown() can block for a long time (see HttpConnection.cleanup()).
        // Since a RuntimeTask can complete only after unregister() returns, we call it in a separate thread.
        shutdownExecutor.submit(() -> {
          fetcher.shutdown(true);   // true because this fetcher is likely stalled
        });
      }
    });

    synchronized (registerLock) {
      ShuffleClient<?> old = shuffleClients.remove(shuffleClientId);
      assert old != null;
      if (shuffleClients.isEmpty()) {
        // no new ShuffleClient can be registered, so addKnownInput() is not called
        // as a result, knownSrcHosts[] can be safely cleaned inside this block
        if (knownSrcHosts.size() > maxNumInputHosts) {
          LOG.warn("Clearing known InputHosts: current size = {}", knownSrcHosts.size());
          knownSrcHosts.clear();
        }
      }
    }

    LOG.info("Unregistered ShuffleClient: {}", shuffleClientId);
  }

  public void addKnownInput(ShuffleClient<?> shuffleClient,
                            String hostName, String containerId, int port,
                            CompositeInputAttemptIdentifier srcAttemptIdentifier, int partitionId) {
    HostPort identifier = new HostPort(hostName, containerId, port);
    InputHost host = knownSrcHosts.get(identifier);
    if (host == null) {
      host = new InputHost(identifier);
      InputHost old = knownSrcHosts.putIfAbsent(identifier, host);
      if (old != null) {
        host = old;
      }
    }

    host.addKnownInput(shuffleClient, partitionId,
        srcAttemptIdentifier.getInputIdentifierCount(), srcAttemptIdentifier, pendingHosts,
        false);
  }

  public void fetchSucceeded(Long shuffleClientId, String host,
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

  public void fetchFailed(Long shuffleClientId,
                          final CompositeInputAttemptIdentifier srcAttemptIdentifier,
                          boolean readFailed, boolean connectFailed,
                          @Nullable InputHost inputHost, InputHost.PartitionRange partitionRange,
                          @Nullable Fetcher<?> fetcher) {
    ShuffleClient<?> shuffleClient = shuffleClients.get(shuffleClientId);
    if (shuffleClient == null) {
      LOG.warn("ShuffleClient {} already unregistered, ignoring fetchFailed: {}",
          shuffleClientId, srcAttemptIdentifier);
      return;
    }

    if (inputHost != null) {
      if (inputHost.containsInput(shuffleClientId, partitionRange, srcAttemptIdentifier)) {
        // This can happen if some speculative fetcher finds 'mapOutput.getType() == Type.WAIT' and
        // enqueue itself, while another speculative fetcher fails afterwards.
        LOG.info("Do not fail {} because the same input is currently in the queue of {}",
            srcAttemptIdentifier, inputHost);
        return;
      }

      if (fetcher != null) {
        boolean existsConcurrentFetcher = runningFetchers.stream().anyMatch(f -> {
          return f != fetcher &&
            f.inputHost.getHostPort().equals(inputHost.getHostPort()) &&  // redundant, but for quick filtering
            f.containsInputAttemptIdentifier(srcAttemptIdentifier);
        });
        if (existsConcurrentFetcher) {
          LOG.info("Do not fail {} of {} because another fetcher is running",
              srcAttemptIdentifier, fetcher);
          return;
        }
      }
    }

    shuffleClient.fetchFailed(srcAttemptIdentifier, readFailed, connectFailed);
  }

  public void dagLeaving(int dagIdId) {
    // TODO: currently no action necessary because unregister() is called for every ShuffleClient
  }

  public void shutdown() {
    if (!isShutdown.getAndSet(true)) {
      wakeupLoop();
      // add()/remove() can be called while traversing (which is okay because DaemonTask is stopping)
      runningFetchers.forEach(fetcher -> { fetcher.shutdown(true); });

      if (this.fetcherExecutor != null && !this.fetcherExecutor.isShutdown()) {
        this.fetcherExecutor.shutdownNow();   // interrupt all running fetchers
      }
    }
  }

  public void informAM(Long shuffleSchedulerId, CompositeInputAttemptIdentifier srcAttempt) {
    ShuffleScheduler shuffleScheduler = (ShuffleScheduler)shuffleClients.get(shuffleSchedulerId);
    if (shuffleScheduler == null) {
      LOG.warn("ShuffleScheduler {} already unregistered, ignoring informAM(): {}",
        shuffleSchedulerId, srcAttempt);
    } else {
      shuffleScheduler.informAM(srcAttempt);
    }
  }

  public void waitForMergeManager(Long shuffleSchedulerId) throws InterruptedException {
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
      // this is the only place where Fetcher can be completely removed from runningFetchers[]
      boolean isCompletedFromStuck = fetcher.setStateCOMPLETED();
      if (isCompletedFromStuck) {
        // This thread is responsible for calling removeHostBlocked().
        removeHostBlocked(fetcher);
        LOG.info("Fetcher STUCK to COMPLETED: {} in stage {}",
            fetcher.getFetcherIdentifier(), fetcher.getStage());
      }

      runningFetchers.remove(fetcher);
      wakeupLoop();
      if (fetcher.attempt > 0) {  // this is a speculative Fetcher
        LOG.info("Speculative Fetcher finished: {}", fetcher.getFetcherIdentifier());
      }
    }

    @Override
    public void onSuccess(FetchResult result) {
      fetcher.shutdown(false);  // disconnect = false to reuse HTTPConnection
      if (isShutdown.get()) {
        if (isDebugEnabled) {
          LOG.debug("Already shutdown. Ignoring event from fetcher");
        }
      } else {
        // if ShuffleClient for this fetcher is gone, do not consume result
        if (result != null && shuffleClients.get(result.getShuffleClientId()) != null) {
          // use '==' instead of 'equals' because we want to avoid conversion from long to Long
          assert result.getShuffleClientId() == fetcher.getShuffleClient().getShuffleClientId();

          Map<CompositeInputAttemptIdentifier, InputHost.PartitionRange> pendingInputs = result.getPendingInputs();
          if (pendingInputs != null && !pendingInputs.isEmpty()) {
            HostPort identifier = result.getHostPort();
            InputHost inputHost = knownSrcHosts.get(identifier);
            if (inputHost != null) {  // can be null (in rare cases) if unregister() has been called
              for (Map.Entry<CompositeInputAttemptIdentifier, InputHost.PartitionRange> input : pendingInputs.entrySet()) {
                InputHost.PartitionRange range = input.getValue();
                inputHost.addKnownInput(fetcher.getShuffleClient(),
                    range.getPartition(), range.getPartitionCount(), input.getKey(), pendingHosts,
                    true);
              }
            } else {
              Long shuffleClientId = result.getShuffleClientId();
              LOG.warn("Reporting fetch failure for all pending inputs because {} for ShuffleClient {} is gone",
                  identifier, shuffleClientId);
              for (Map.Entry<CompositeInputAttemptIdentifier, InputHost.PartitionRange> input : pendingInputs.entrySet()) {
                fetchFailed(shuffleClientId, input.getKey(), false, true, null, null, null);
              }
            }
          }
        }
        doBookKeepingForFetcherComplete();
      }
    }

    @Override
    public void onFailure(Throwable t) {
      // Unsuccessful - the fetcher may not have shutdown correctly. Try shutting it down.
      fetcher.shutdown(true);   // disconnect = true and do not reuse HTTPConnection
      if (isShutdown.get()) {
        if (isDebugEnabled) {
          LOG.debug("Already shutdown. Ignoring error from fetcher: ", t);
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
