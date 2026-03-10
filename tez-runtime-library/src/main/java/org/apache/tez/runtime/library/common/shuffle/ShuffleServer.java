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
import org.apache.tez.runtime.api.ProcessorContext;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
import java.util.stream.Collectors;

public class ShuffleServer implements FetcherCallback {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServer.class);
  private final boolean isDebugEnabled = LOG.isDebugEnabled();

  public static ShuffleServer createInstance(
    ProcessorContext context, Configuration conf) throws IOException {
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
    SCHEME_PRIORITY,
    SCHEME_FIRST,
    SCHEME_MAX
  }

  private final ProcessorContext taskContext;
  private final int maxNumFetchers;
  private final String serverName;

  private final ListeningExecutorService fetcherExecutor;
  private final FetcherConfigCommon fetcherConfigCommon;

  private final int maxTaskOutputAtOnce;
  private final RangesScheme rangesScheme;

  // Invariant: pendingHosts[] (with some valid shuffleClientId in partitionToInputs[]) \subset knownSrcHosts.InputHost[]
  private final ConcurrentMap<HostPort, InputHost> knownSrcHosts;

  private final AtomicLong shuffleClientCount = new AtomicLong(0L);
  protected final ConcurrentMap<Long, ShuffleClient<?>> shuffleClients;
  private final Map<String, Set<Integer>> envContainerIdFinishedMap = new HashMap<String, Set<Integer>>();
  private final Object registerLock = new Object();
  private volatile boolean hasContainerIdFinished = false;  // true iff !envContainerIdFinishedMap.isEmpty()

  // Invariant on InputHost in pendingHosts[]: InputHost.hasPendingInput == true
  // InputHost.partitionToInputs[] can be empty.
  // The same InputHost can appear multiple times in pendingHosts[].
  // Cf. InputHost.clearAndGetOnePartitionRange()
  private final BlockingQueue<InputHost> pendingHosts;

  // for loop in call()
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition wakeLoop = lock.newCondition();

  private final Set<Fetcher<?>> runningFetchers;   // thread-safe because we use ConcurrentHashMap

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  private final Object throwableLock = new Object();
  // Throwable (ex. OutOfMemoryError) from onFailure() should be re-thrown so that
  // the DaemonTaskAttempt running ShuffleServer reports failure to MR3 DAGAppMaster.
  // In this way, we can terminate the current ContainerWorker.
  private Throwable throwableFromFetcherOnFailure = null;

  private final ExecutorService shutdownExecutor;

  private static final int LAUNCH_LOOP_WAIT_PERIOD_MILLIS = 1000;
  private static final int CHECK_STUCK_FETCHER_PERIOD_MILLIS = 250;

  public ShuffleServer(
      ProcessorContext taskContext,
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
    this.rangesScheme =
        scheme.equalsIgnoreCase("first") ? RangesScheme.SCHEME_FIRST :
        scheme.equalsIgnoreCase("max") ? RangesScheme.SCHEME_MAX :
        RangesScheme.SCHEME_PRIORITY;

    knownSrcHosts = new ConcurrentHashMap<HostPort, InputHost>();

    shuffleClients = new ConcurrentHashMap<Long, ShuffleClient<?>>();
    pendingHosts = new LinkedBlockingQueue<InputHost>();

    runningFetchers = Collections.newSetFromMap(new ConcurrentHashMap<Fetcher<?>, Boolean>());

    this.shutdownExecutor = Executors.newSingleThreadExecutor();

    LOG.info("{} Configuration: numFetchers={}, maxTaskOutputAtOnce={}, FetcherConfigCommon={}, rangesScheme={}",
        serverName, numFetchers, maxTaskOutputAtOnce, fetcherConfigCommon, rangesScheme);
  }

  public int getMaxTaskOutputAtOnce() {
    return maxTaskOutputAtOnce;
  }

  public void run() {
    try {
      call();
      LOG.info("{} thread completed", serverName);
    } catch (InterruptedException ex) {
      LOG.error("{} finished, isShutdown = {}. Ignoring: ", serverName, isShutdown.get(), ex);
    }

    synchronized (throwableLock) {
      if (throwableFromFetcherOnFailure != null) {
        throw new TezUncheckedException(throwableFromFetcherOnFailure);
      }
    }
  }

  // variables local to call()
  private List<String> envContainerIdsToBlockFetching;
  private boolean shouldLaunchNewFetchers;
  private boolean existsFetcherToRetry;
  private boolean existsFetcherFromStuckToRecovered;
  private boolean existsFetcherFromStuckToSpeculative;
  private long nextCheckStuckFetcherMillis;
  private boolean shouldCheckStuckFetcher;

  private boolean isBlockedFetching(InputHost p) {
    return envContainerIdsToBlockFetching.contains(p.getHostPort().getEnvContainerId());
  }

  // called only from ShuffleServer.call() thread
  private boolean getShouldLaunchNewFetchers() {
    for (InputHost p: pendingHosts) {
      if (!isBlockedFetching(p)
          && p.isHostNormal()
          && p.hasFetcherToLaunch(shuffleClients)) {
        return true;
      }
    }
    return false;
  }

  private void updateLoopConditions() throws InterruptedException {
    scala.Tuple3<List<String>, List<String>, List<Integer>> p = taskContext.getEnvContainerIdsToBlockFetchingAndFinished();
    envContainerIdsToBlockFetching = p._1();
    List<String> envContainerIdsFinished = p._2();
    List<Integer> dagIdIdsInScheduling = p._3();

    if (!envContainerIdsFinished.isEmpty()) {
      String dagIdIdsInSchedulingStr = dagIdIdsInScheduling.stream()
        .map(String::valueOf)
        .collect(Collectors.joining(", "));
      LOG.info("New envContainerIdFinished = {}, dagIdIdsInScheduling = {}",
          String.join(", ", envContainerIdsFinished), dagIdIdsInSchedulingStr);

      processEnvContainerIdsFinished(envContainerIdsFinished);

      synchronized (registerLock) {
        if (dagIdIdsInScheduling.isEmpty()) {
          for (String envContainerIdFinished : envContainerIdsFinished) {
            retireContainerFinished(envContainerIdFinished);
          }
        } else {
          for (String envContainerIdFinished : envContainerIdsFinished) {
            Set<Integer> runningDagIdIds = new HashSet<Integer>(dagIdIdsInScheduling);
            envContainerIdFinishedMap.put(envContainerIdFinished, runningDagIdIds);
          }
          hasContainerIdFinished = true;
        }
      }
    }

    final long currentMillis = System.currentTimeMillis();

    int currentNumFetchers = runningFetchers.size();
    shouldLaunchNewFetchers =
        currentNumFetchers < maxNumFetchers &&
        getShouldLaunchNewFetchers();

    existsFetcherToRetry = runningFetchers.stream().anyMatch(f -> {
        if (isBlockedFetching(f.inputHost) || !isInputHostReachable(f.inputHost)) {
          return false;
        }
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
      if (isBlockedFetching(f.inputHost) || !isInputHostReachable(f.inputHost)) {
        return false;
      }
      FetcherConfig fetcherConfig = f.fetcherConfig;
      final int STUCK_FETCHER_RELEASE_MILLIS = fetcherConfig.stuckFetcherReleaseMillis;
      return
        f.getState() == Fetcher.STATE_STUCK &&
        f.getStage() < Fetcher.STAGE_FIRST_FETCHED &&
        (currentMillis - f.getStartMillis() >= STUCK_FETCHER_RELEASE_MILLIS);
    });

    shouldCheckStuckFetcher = currentMillis > nextCheckStuckFetcherMillis;
  }

  private void call() throws InterruptedException {
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
          if (isBlockedFetching(fetcher.inputHost) || !isInputHostReachable(fetcher.inputHost)) {
            return;
          }
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
              LOG.warn("Fetcher to RETRY: {} in {}ms, {}",
                  fetcher.getFetcherIdentifier(), elapsed,
                  fetcher.inputHost.getHostPort().getEnvContainerId());
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
          if (isBlockedFetching(fetcher.inputHost) || !isInputHostReachable(fetcher.inputHost)) {
            return;
          }
          FetcherConfig fetcherConfig = fetcher.fetcherConfig;
          final int STUCK_FETCHER_RELEASE_MILLIS = fetcherConfig.stuckFetcherReleaseMillis;
          if (fetcher.getState() == Fetcher.STATE_STUCK &&
              fetcher.getStage() != Fetcher.STAGE_FIRST_FETCHED &&
              (currentMillis - fetcher.getStartMillis()) >= STUCK_FETCHER_RELEASE_MILLIS) {
            boolean result = fetcher.trySetStateSPECULATIVE();
            if (result) {
              // This thread is responsible for calling removeHostBlocked().
              removeHostBlocked(fetcher);
              LOG.warn("Fetcher STUCK to SPECULATIVE: {} in stage {}, {}",
                  fetcher.getFetcherIdentifier(), fetcher.getStage(),
                  fetcher.inputHost.getHostPort().getEnvContainerId());
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
        final int maxFetchersToRun = maxNumFetchers - runningFetchers.size();

        // Do NOT keep checking 'runningFetchers.size() < maxNumFetchers' because we have to check
        // other conditions (e.g. existsFetcherToRetry) and cannot stay indefinitely in this loop.
        //
        // Calling getShouldLaunchNewFetchers() is semantically sound, but it is computationally intensive.
        // However, experiments show that this is the right approach (probably because of the high frequency of
        // adding and removing Fetchers). For an optimized version (without clear advantage), see commit aa631fd34f.
        int numNewFetchers = 0;
        InputHost peekInputHost = pendingHosts.peek();
        while (numNewFetchers < maxFetchersToRun &&
               peekInputHost != null &&
               getShouldLaunchNewFetchers()) {
          // for every ShuffleClient,
          //   1. 'numPartitionRanges > 0' remains the same until the current thread consumes existing inputs
          //   2. 'numFetchers < maxNumFetchers' remains the same until the current thread creates new Fetchers
          InputHost inputHost;
          try {
            inputHost = peekInputHost.takeFromPendingHosts(pendingHosts);
          } catch (InterruptedException ex) {
            if (isShutdown.get()) {
              LOG.info("Interrupted and has been shutdown, breaking out of the loop");
              Thread.currentThread().interrupt();
              break;
            } else {
              throw ex;
            }
          }

          // Optionally, inputHost.hasFetcherToLaunch() can be called as an optimization.
          // Cf. constructFetcherForHost() eventually calls ShuffleClient.shouldScanPendingInputs().
          if (!isBlockedFetching(inputHost) &&
              inputHost.isHostNormal()) {
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

  // called only from ShuffleServer.call() thread
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

  private void processEnvContainerIdsFinished(final List<String> envContainerIdsFinished) throws InterruptedException {
    // use the same logic as in call() when shouldLaunchNewFetchers == true
    // pendingHosts[] does not shrink inside method, but it may expand if addKnownInput() is called.
    final int maxInputHosts = pendingHosts.size();

    int numInputHosts = 0;
    while (numInputHosts < maxInputHosts) {
      InputHost peekInputHost = pendingHosts.peek();
      assert peekInputHost != null;

      InputHost inputHost;
      try {
        inputHost = peekInputHost.takeFromPendingHosts(pendingHosts);
      } catch (InterruptedException ex) {
        if (isShutdown.get()) {
          LOG.info("Interrupted and has been shutdown, breaking out of the envContainerIdsFinished loop");
          Thread.currentThread().interrupt();
          break;
        } else {
          throw ex;
        }
      }

      inputHost.processEnvContainerIdsFinished(envContainerIdsFinished, pendingHosts, shuffleClients);

      numInputHosts += 1;
    }

    // Removing InputHosts from knownSrcHosts[] may destroy the invariant 'pendingInputs[] /subset knownSrcHosts[]'.
    // Combining takeFromPendingHosts() and processEnvContainerIdsFinished() does not solve this problem.
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
    }

    LOG.info("Unregistered ShuffleClient: {}", shuffleClientId);
  }

  public void addKnownInput(ShuffleClient<?> shuffleClient,
                            String hostName, String containerId, int port,
                            CompositeInputAttemptIdentifier srcAttemptIdentifier, int partitionId) {
    if (hasContainerIdFinished) {
      synchronized (registerLock) {
        if (envContainerIdFinishedMap.containsKey(containerId)) {
          LOG.warn("Immediately fail {} because {} is already finished", srcAttemptIdentifier, containerId);
          // directly call shuffleClient.fetchFailed(), instead of this.fetchFailed(),
          // because srcAttemptIdentifier is no longer valid
          shuffleClient.fetchFailed(srcAttemptIdentifier, false, true);
          return;
        }
      }
    }

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

  public void fetchFailed(final Long shuffleClientId,
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

    if (fetcher != null) {
      fetcher.isFailed = true;  // for computing existsConcurrentNotFailedFetcher correctly below
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
        // existsConcurrentNotFailedFetcher is correctly computed because Fetcher.isFailed is volatile.
        // For the last Fetcher setting isFailed to true:
        //   - existsConcurrentNotFailedFetcher is false.
        //   - so, shuffleClient.fetchFailed() is guaranteed to be called.
        // If any Fetcher eventually succeeds:
        //   - its isFailed is never set to true.
        //   - so, existsConcurrentNotFailedFetcher is never true while it is in runningFetchers[].
        //   - by the time it is removed from runningFetchers[], ShuffleClient.fetchSucceeded() is called.
        boolean existsConcurrentNotFailedFetcher = runningFetchers.stream().anyMatch(f -> {
          return f != fetcher &&
            // f.containsInputAttemptIdentifier() does not consider pathComponent.
            // As a result, two different CompositeInputAttemptIdentifier's originating from different source Vertexes
            // are treated equal if they happen to inputIdentifier/attemptNumber/spillEventId.
            // Hence, we should manually check if they originate from the same source Vertex.
            f.useSingleShuffleClientId(shuffleClientId) &&
            !f.isFailed &&
            f.inputHost.getHostPort().equals(inputHost.getHostPort()) &&  // redundant, but for quick filtering
            f.containsInputAttemptIdentifier(srcAttemptIdentifier);
        });
        if (existsConcurrentNotFailedFetcher) {
          LOG.info("Do not fail {} of {} because another fetcher (not failed yet) is running",
              srcAttemptIdentifier, fetcher);
          return;
        }
      }
    }

    shuffleClient.fetchFailed(srcAttemptIdentifier, readFailed, connectFailed);
  }

  public void dagLeaving(int dagIdId) {
    synchronized (registerLock) {
      Iterator<Map.Entry<String, Set<Integer>>> it = envContainerIdFinishedMap.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<String, Set<Integer>> e = it.next();
        Set<Integer> set = e.getValue();
        set.remove(dagIdId);

        if (set.isEmpty()) {
          // this envContainerIdFinished is no longer needed
          retireContainerFinished(e.getKey());
          it.remove();
        }
      }

      if (envContainerIdFinishedMap.isEmpty()) {
        hasContainerIdFinished = false;
      }
    }
  }

  private void retireContainerFinished(String envContainerIdFinished) {
    LOG.info("Removing envContainerIdFinished: {}", envContainerIdFinished);
    knownSrcHosts.entrySet().removeIf(entry ->
      entry.getKey().getEnvContainerId().equals(envContainerIdFinished));
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

  private boolean isInputHostReachable(InputHost inputHost) {
    if (hasContainerIdFinished) {
      synchronized (registerLock) {
        if (envContainerIdFinishedMap.containsKey(inputHost.getHostPort().getEnvContainerId())) {
          return false;
        }
      }
    }
    return true;
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
            // inputHost can be null (in rare cases) if unregister() has been called
            if (inputHost != null && isInputHostReachable(inputHost)) {
              for (Map.Entry<CompositeInputAttemptIdentifier, InputHost.PartitionRange> input : pendingInputs.entrySet()) {
                InputHost.PartitionRange range = input.getValue();
                inputHost.addKnownInput(fetcher.getShuffleClient(),
                    range.getPartition(), range.getPartitionCount(), input.getKey(), pendingHosts,
                    true);
              }
            } else {
              // can be null if unregister() was called
              Long shuffleClientId = result.getShuffleClientId();
              LOG.warn("Reporting fetch failure for all pending inputs because {} for ShuffleClient {} is gone or invalid",
                  identifier, shuffleClientId);
              for (Map.Entry<CompositeInputAttemptIdentifier, InputHost.PartitionRange> input : pendingInputs.entrySet()) {
                fetchFailed(shuffleClientId, input.getKey(), false, true, null, null, null);
              }
            }
          }
        }
      }

      doBookKeepingForFetcherComplete();
    }

    // onFailure() means that Fetcher thread itself failed, e.g., due to OutOfMemoryError.
    // It does not mean that Fetcher failed, e.g., due to IOException, in which case
    // onSuccess() is called (because Fetcher thread itself succeeded).
    // We can continue to run ShuffleServer by recovering InputAttemptIdentifier associated with Fetcher.
    @Override
    public void onFailure(Throwable th) {
      // Unsuccessful - the fetcher may not have shutdown correctly. Try shutting it down.
      fetcher.shutdown(true);   // disconnect = true and do not reuse HTTPConnection
      LOG.error("Fetcher failed with error: ", th);

      if (isShutdown.get()) {
        if (isDebugEnabled) {
          LOG.debug("Already shutdown. Ignoring failure from fetcher");
        }
      } else {
        Long shuffleClientId = fetcher.getShuffleClient().getShuffleClientId();
        ShuffleClient<?> shuffleClient = shuffleClients.get(shuffleClientId);

        // if ShuffleClient for this fetcher is gone, ignore failure
        if (shuffleClient != null) {
          HostPort identifier = fetcher.inputHost.getHostPort();
          InputHost inputHost = knownSrcHosts.get(identifier);

          InputHost.PartitionToInputs pendingInputs = fetcher.getPendingInputs();
          InputHost.PartitionRange range = pendingInputs.getPartitionRange();
          List<CompositeInputAttemptIdentifier> inputs = pendingInputs.getInputs();

          // can be null (in rare cases) if unregister() has been called
          if (inputHost != null && isInputHostReachable(inputHost)) {
            for (CompositeInputAttemptIdentifier input : inputs) {
              inputHost.addKnownInput(shuffleClient,
                  range.getPartition(), range.getPartitionCount(), input, pendingHosts,
                  true);
            }
          } else {
            LOG.warn("Reporting fetch failure for all inputs because {} for ShuffleClient {} is gone or invalid",
                identifier, shuffleClientId);
            for (CompositeInputAttemptIdentifier input : inputs) {
              fetchFailed(shuffleClientId, input, false, true, null, null, null);
            }
          }
        }
      }

      doBookKeepingForFetcherComplete();

      synchronized (throwableLock) {
        if (throwableFromFetcherOnFailure == null && (th instanceof VirtualMachineError)) {
          throwableFromFetcherOnFailure = th;
        }
      }
    }
  }
}
