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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.crypto.SecretKey;

import com.google.common.annotations.VisibleForTesting;
import org.apache.celeborn.client.ShuffleClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.FetcherBase;
import org.apache.tez.runtime.library.common.shuffle.RssFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.shuffle.FetchResult;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput.Type;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.common.shuffle.Fetcher;
import org.apache.tez.runtime.library.common.shuffle.Fetcher.FetcherBuilder;
import org.apache.tez.runtime.library.common.shuffle.FetcherCallback;
import org.apache.tez.runtime.library.common.shuffle.HostPort;
import org.apache.tez.runtime.library.common.shuffle.InputHost;
import org.apache.tez.runtime.library.common.shuffle.InputHost.PartitionToInputs;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils.FetchStatsLogger;

import com.google.common.base.Objects;
import org.apache.tez.common.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

// This only knows how to deal with a single srcIndex for a given targetIndex.
// In case the src task generates multiple outputs for the same target Index
// (multiple src-indices), modifications will be required.
public class ShuffleManager implements FetcherCallback {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleManager.class);
  private static final Logger LOG_FETCH = LoggerFactory.getLogger(LOG.getName() + ".fetch");
  private static final FetchStatsLogger fetchStatsLogger = new FetchStatsLogger(LOG_FETCH, LOG);

  private final InputContext inputContext;
  private final int numInputs;  // AbstractLogicalInput.numPhysicalInputs

  private final DecimalFormat mbpsFormat = new DecimalFormat("0.00");

  private final FetchedInputAllocator inputManager;

  @VisibleForTesting
  final ListeningExecutorService fetcherExecutor;

  private final ListeningExecutorService schedulerExecutor;
  private final RunShuffleCallable schedulerCallable;
  
  private final BlockingQueue<FetchedInput> completedInputs;
  private final AtomicBoolean inputReadyNotificationSent = new AtomicBoolean(false);
  @VisibleForTesting
  final BitSet completedInputSet;   // [0, ..., numInputs - 1]
  private final ConcurrentMap<HostPort, InputHost> knownSrcHosts;
  private final BlockingQueue<InputHost> pendingHosts;
  private final Set<InputAttemptIdentifier> obsoletedInputs;
  private final Set<FetcherBase> runningFetchers;
  
  private final AtomicInteger numCompletedInputs = new AtomicInteger(0);
  private final AtomicInteger numFetchedSpills = new AtomicInteger(0);

  private final long startTime;
  // private long lastProgressTime;
  private long totalBytesShuffledTillNow;

  // Required to be held when manipulating pendingHosts
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition wakeLoop = lock.newCondition();
  
  private final int numFetchers;
  private final boolean asyncHttp;
  
  // Parameters required by Fetchers
  private final JobTokenSecretManager jobTokenSecretMgr;
  private final CompressionCodec codec;
  private final boolean localDiskFetchEnabled;
  private final boolean localFetchComparePort;
  private final boolean sharedFetchEnabled;
  private final boolean verifyDiskChecksum;
  private final boolean compositeFetch;
  
  private final int ifileBufferSize;
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;
  
  private final String srcNameTrimmed;

  private final int maxTaskOutputAtOnce;

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  private long inputRecordsFromEvents;
  private long eventsReceived; 
  private final TezCounter approximateInputRecords;
  private final TezCounter shuffledInputsCounter;
  private final TezCounter failedShufflesCounter;
  private final TezCounter bytesShuffledCounter;
  private final TezCounter decompressedDataSizeCounter;
  private final TezCounter bytesShuffledToDiskCounter;
  private final TezCounter bytesShuffledToMemCounter;
  private final TezCounter bytesShuffledDirectDiskCounter;
  
  private volatile Throwable shuffleError;
  private final HttpConnectionParams httpConnectionParams;
  

  private final LocalDirAllocator localDirAllocator;
  private final RawLocalFileSystem localFs;
  private final Path[] localDisks;
  private final String localhostName;
  public final int[] localShufflePorts;

  private final TezCounter shufflePhaseTime;
  private final TezCounter firstEventReceived;
  private final TezCounter lastEventReceived;

  //To track shuffleInfo events when finalMerge is disabled OR pipelined shuffle is enabled in source.
  @VisibleForTesting
  final Map<Integer, ShuffleEventInfo> shuffleInfoEventsMap;

  // TODO More counters - FetchErrors, speed?
  private final ShuffleClient rssShuffleClient;

  private long rssFetchSplitThresholdSize;

  public ShuffleManager(InputContext inputContext, Configuration conf, int numInputs,
      int bufferSize, boolean ifileReadAheadEnabled, int ifileReadAheadLength,
      CompressionCodec codec, FetchedInputAllocator inputAllocator,
      ShuffleClient rssShuffleClient) throws IOException {
    this.inputContext = inputContext;
    this.numInputs = numInputs;

    this.approximateInputRecords = inputContext.getCounters().findCounter(TaskCounter.APPROXIMATE_INPUT_RECORDS);
    this.shuffledInputsCounter = inputContext.getCounters().findCounter(TaskCounter.NUM_SHUFFLED_INPUTS);
    this.failedShufflesCounter = inputContext.getCounters().findCounter(TaskCounter.NUM_FAILED_SHUFFLE_INPUTS);
    this.bytesShuffledCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES);
    this.decompressedDataSizeCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_DECOMPRESSED);
    this.bytesShuffledToDiskCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_TO_DISK);
    this.bytesShuffledToMemCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_TO_MEM);
    this.bytesShuffledDirectDiskCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_DISK_DIRECT);
  
    this.ifileBufferSize = bufferSize;
    this.ifileReadAhead = ifileReadAheadEnabled;
    this.ifileReadAheadLength = ifileReadAheadLength;
    this.codec = codec;
    this.inputManager = inputAllocator;
    this.localDiskFetchEnabled = conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH,
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_DEFAULT);
    this.localFetchComparePort = conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_LOCAL_FETCH_COMPARE_PORT,
        TezRuntimeConfiguration.TEZ_RUNTIME_LOCAL_FETCH_COMPARE_PORT_DEFAULT);
    this.sharedFetchEnabled = conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_SHARED_FETCH,
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_SHARED_FETCH_DEFAULT);
    this.verifyDiskChecksum = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_VERIFY_DISK_CHECKSUM,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_VERIFY_DISK_CHECKSUM_DEFAULT);

    this.shufflePhaseTime = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_PHASE_TIME);
    this.firstEventReceived = inputContext.getCounters().findCounter(TaskCounter.FIRST_EVENT_RECEIVED);
    this.lastEventReceived = inputContext.getCounters().findCounter(TaskCounter.LAST_EVENT_RECEIVED);
    this.compositeFetch = rssShuffleClient != null ? false : ShuffleUtils.isTezShuffleHandler(conf);

    this.srcNameTrimmed = TezUtilsInternal.cleanVertexName(inputContext.getSourceVertexName());
  
    completedInputSet = new BitSet(numInputs);
    /**
     * In case of pipelined shuffle, it is possible to get multiple FetchedInput per attempt.
     * We do not know upfront the number of spills from source.
     */
    completedInputs = new LinkedBlockingDeque<FetchedInput>();
    knownSrcHosts = new ConcurrentHashMap<HostPort, InputHost>();
    pendingHosts = new LinkedBlockingQueue<InputHost>();
    obsoletedInputs = Collections.newSetFromMap(new ConcurrentHashMap<InputAttemptIdentifier, Boolean>());
    runningFetchers = Collections.newSetFromMap(new ConcurrentHashMap<FetcherBase, Boolean>());

    int maxConfiguredFetchers = 
        conf.getInt(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES_DEFAULT);
    
    this.numFetchers = Math.min(maxConfiguredFetchers, numInputs);

    final ExecutorService fetcherRawExecutor;
    if (conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCHER_USE_SHARED_POOL,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCHER_USE_SHARED_POOL_DEFAULT)) {
      fetcherRawExecutor = inputContext.createTezFrameworkExecutorService(numFetchers,
          inputContext.getUniqueIdentifier() + " Fetcher_B {" + srcNameTrimmed + "} #%d");
    } else {
      fetcherRawExecutor = Executors.newFixedThreadPool(numFetchers, new ThreadFactoryBuilder()
          .setDaemon(true).setNameFormat(inputContext.getUniqueIdentifier() + " Fetcher_B {" + srcNameTrimmed + "} #%d").build());
    }
    this.fetcherExecutor = MoreExecutors.listeningDecorator(fetcherRawExecutor);

    ExecutorService schedulerRawExecutor = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("ShuffleRunner {" + srcNameTrimmed + "}").build());
    this.schedulerExecutor = MoreExecutors.listeningDecorator(schedulerRawExecutor);
    this.schedulerCallable = new RunShuffleCallable(conf);
    
    this.startTime = System.currentTimeMillis();
    // this.lastProgressTime = startTime;

    String auxiliaryService = conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
        TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
    SecretKey shuffleSecret = ShuffleUtils
        .getJobTokenSecretFromTokenBytes(inputContext
            .getServiceConsumerMetaData(auxiliaryService));
    this.jobTokenSecretMgr = new JobTokenSecretManager(shuffleSecret);
    this.asyncHttp = conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_USE_ASYNC_HTTP, false);
    httpConnectionParams = ShuffleUtils.getHttpConnectionParams(conf);

    this.localFs = (RawLocalFileSystem) FileSystem.getLocal(conf).getRaw();

    this.localDirAllocator = new LocalDirAllocator(
        TezRuntimeFrameworkConfigs.LOCAL_DIRS);

    this.localDisks = Iterables.toArray(
        localDirAllocator.getAllLocalPathsToRead(".", conf), Path.class);
    this.localhostName = inputContext.getExecutionContext().getHostName();
    final ByteBuffer shuffleMetaData =
        inputContext.getServiceProviderMetaData(auxiliaryService);
    this.localShufflePorts = ShuffleUtils.deserializeShuffleProviderMetaData(shuffleMetaData);

    /**
     * Setting to very high val can lead to Http 400 error. Cap it to 75; every attempt id would
     * be approximately 48 bytes; 48 * 75 = 3600 which should give some room for other info in URL.
     */
    this.maxTaskOutputAtOnce = Math.max(1, Math.min(75, conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE_DEFAULT)));

    Arrays.sort(this.localDisks);

    shuffleInfoEventsMap = new ConcurrentHashMap<Integer, ShuffleEventInfo>();

    this.rssShuffleClient = rssShuffleClient;
    if (this.rssShuffleClient != null) {
      if (inputContext.readPartitionAllOnce()) {
        rssFetchSplitThresholdSize = conf.getLong(
            TezRuntimeConfiguration.TEZ_RUNTIME_CELEBORN_FETCH_SPLIT_THRESHOLD,
            TezRuntimeConfiguration.TEZ_RUNTIME_CELEBORN_FETCH_SPLIT_THRESHOLD_DEFAULT);
      }
      com.datamonad.mr3.MR3Runtime.env().registerShuffleId(inputContext.getDagId(), inputContext.shuffleId());
      LOG.info("Registered shuffleId = " + inputContext.shuffleId());
    }

    LOG.info("{}: numInputs={}, numFetchers={}, rssShuffleClient={}",
        srcNameTrimmed, numInputs, numFetchers, rssShuffleClient != null);
    if (LOG.isDebugEnabled()) {
      LOG.debug("compressionCodec="
          + (codec == null ? "NoCompressionCodec" : codec.getClass().getName())
          + ", ifileBufferSize=" + ifileBufferSize + ", ifileReadAheadEnabled="
          + ifileReadAhead + ", ifileReadAheadLength=" + ifileReadAheadLength +", "
          + "localDiskFetchEnabled=" + localDiskFetchEnabled + ", "
          + "sharedFetchEnabled=" + sharedFetchEnabled + ", "
          + httpConnectionParams.toString() + ", maxTaskOutputAtOnce=" + maxTaskOutputAtOnce
          + ", asyncHttp=" + asyncHttp);
    }
  }

  public void updateApproximateInputRecords(int delta) {
    if (delta <= 0) {
      return;
    }
    inputRecordsFromEvents += delta;
    eventsReceived++;
    approximateInputRecords.setValue((inputRecordsFromEvents / eventsReceived) * numInputs);
  }

  public void run() throws IOException {
    Preconditions.checkState(inputManager != null, "InputManager must be configured");

    ListenableFuture<Void> runShuffleFuture = schedulerExecutor.submit(schedulerCallable);
    Futures.addCallback(runShuffleFuture, new SchedulerFutureCallback());
    // Shutdown this executor once this task, and the callback complete.
    schedulerExecutor.shutdown();
  }
  
  private class RunShuffleCallable implements Callable<Void> {

    private final Configuration conf;

    public RunShuffleCallable(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Void call() throws Exception {
      while (!isShutdown.get() && numCompletedInputs.get() < numInputs) {
        lock.lock();
        try {
          while (!isShutdown.get()
              && (runningFetchers.size() >= numFetchers || pendingHosts.isEmpty())
              && numCompletedInputs.get() < numInputs) {
            boolean ret = wakeLoop.await(1000, TimeUnit.MILLISECONDS);
          }
        } finally {
          lock.unlock();
        }

        if (shuffleError != null) {
          // InputContext has already been informed of a fatal error. Relying on
          // tez to kill the task.
          break;
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug(srcNameTrimmed + ": NumCompletedInputs: " + numCompletedInputs);
        }
        if (numCompletedInputs.get() < numInputs && !isShutdown.get()) {
          lock.lock();
          try {
            // the while{} loop may create more than maxFetchersToRun Fetchers
            // because constructRssFetcher() returns a list of RssFetchers
            int maxFetchersToRun = numFetchers - runningFetchers.size();
            int count = 0;
            while (pendingHosts.peek() != null && !isShutdown.get()) {
              InputHost inputHost = null;
              try {
                inputHost = pendingHosts.take();
              } catch (InterruptedException e) {
                if (isShutdown.get()) {
                  LOG.info(srcNameTrimmed + ": Interrupted and hasBeenShutdown, Breaking out of ShuffleScheduler Loop");
                  Thread.currentThread().interrupt();
                  break;
                } else {
                  throw e;
                }
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug(srcNameTrimmed + ": Processing pending host: " +
                    inputHost.toDetailedString());
              }
              if (inputHost.getNumPendingPartitions() > 0 && !isShutdown.get()) {
                if (rssShuffleClient == null) {
                  FetcherBase fetcher = constructFetcherForHost(inputHost, conf);
                  if (fetcher == null) {
                    continue;
                  }
                  runningFetchers.add(fetcher);
                  if (isShutdown.get()) {
                    LOG.info(srcNameTrimmed + ": hasBeenShutdown, Breaking out of ShuffleScheduler Loop");
                    break;
                  }
                  ListenableFuture<FetchResult> future = fetcherExecutor.submit(fetcher);
                  Futures.addCallback(future, new FetchFutureCallback(fetcher));
                  if (++count >= maxFetchersToRun) {
                    break;
                  }
                } else {
                  List<RssFetcher> fetchers = constructRssFetcher(inputHost);
                  if (fetchers.isEmpty()) {
                    continue;
                  }
                  runningFetchers.addAll(fetchers);
                  if (isShutdown.get()) {
                    LOG.info(srcNameTrimmed + ": hasBeenShutdown, Breaking out of ShuffleScheduler Loop");
                    break;
                  }
                  for (RssFetcher fetcher: fetchers) {
                    ListenableFuture<FetchResult> future = fetcherExecutor.submit(fetcher);
                    Futures.addCallback(future, new FetchFutureCallback(fetcher));
                  }
                  count += fetchers.size();
                  if (count >= maxFetchersToRun) {
                    break;
                  }
                }
              } else {
                if (LOG.isDebugEnabled()) {
                  LOG.debug(srcNameTrimmed + ": " + "Skipping host: " +
                      inputHost.getIdentifier() +
                      " since it has no inputs to process");
                }
              }
            }
          } finally {
            lock.unlock();
          }
        }
      }
      shufflePhaseTime.setValue(System.currentTimeMillis() - startTime);
      LOG.info(srcNameTrimmed + ": Shutting down FetchScheduler, Was Interrupted: " + Thread.currentThread().isInterrupted());
      if (!fetcherExecutor.isShutdown()) {
        fetcherExecutor.shutdownNow();
      }
      return null;
    }
  }

  private boolean validateInputAttemptForPipelinedShuffle(InputAttemptIdentifier input) {
    //For pipelined shuffle.
    //TODO: TEZ-2132 for error handling. As of now, fail fast if there is a different attempt
    if (input.canRetrieveInputInChunks()) {
      ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(input.getInputIdentifier());
      if (eventInfo != null && input.getAttemptNumber() != eventInfo.attemptNum) {
        if (eventInfo.scheduledForDownload || !eventInfo.eventsProcessed.isEmpty()) {
          IOException exception = new IOException("Previous event already got scheduled for " +
              input + ". Previous attempt's data could have been already merged "
              + "to memory/disk outputs.  Killing (self) this task early."
              + " currentAttemptNum=" + eventInfo.attemptNum
              + ", eventsProcessed=" + eventInfo.eventsProcessed
              + ", scheduledForDownload=" + eventInfo.scheduledForDownload
              + ", newAttemptNum=" + input.getAttemptNumber());
          String message = "Killing self as previous attempt data could have been consumed";
          killSelf(exception, message);
          return false;
        }
      }
    }
    return true;
  }

  void killSelf(Exception exception, String message) {
    LOG.error(message, exception);
    this.inputContext.killSelf(exception, message);
  }

  @VisibleForTesting
  Fetcher constructFetcherForHost(InputHost inputHost, Configuration conf) {

    Path lockDisk = null;

    if (sharedFetchEnabled) {
      // pick a single lock disk from the edge name's hashcode + host hashcode
      final int h = Math.abs(Objects.hashCode(this.srcNameTrimmed, inputHost.getHost()));
      lockDisk = new Path(this.localDisks[h % this.localDisks.length], "locks");
    }

    FetcherBuilder fetcherBuilder = new FetcherBuilder(ShuffleManager.this,
      httpConnectionParams, inputManager, inputContext.getApplicationId(), inputContext.getDagIdentifier(),
        jobTokenSecretMgr, srcNameTrimmed, conf, localFs, localDirAllocator,
        lockDisk, localDiskFetchEnabled, sharedFetchEnabled,
        localhostName, localShufflePorts, asyncHttp, verifyDiskChecksum, compositeFetch, localFetchComparePort, inputContext);

    if (codec != null) {
      fetcherBuilder.setCompressionParameters(codec);
    }
    fetcherBuilder.setIFileParams(ifileReadAhead, ifileReadAheadLength);

    // Remove obsolete inputs from the list being given to the fetcher. Also
    // remove from the obsolete list.
    PartitionToInputs pendingInputsOfOnePartitionRange = inputHost
        .clearAndGetOnePartitionRange();
    int includedMaps = 0;
    for (Iterator<InputAttemptIdentifier> inputIter =
        pendingInputsOfOnePartitionRange.getInputs().iterator();
            inputIter.hasNext();) {
      InputAttemptIdentifier input = inputIter.next();

      //For pipelined shuffle.
      if (!validateInputAttemptForPipelinedShuffle(input)) {
        continue;
      }

      // Avoid adding attempts which have already completed.
      boolean alreadyCompleted;
      if (input instanceof CompositeInputAttemptIdentifier) {
        CompositeInputAttemptIdentifier compositeInput = (CompositeInputAttemptIdentifier)input;
        int nextClearBit = completedInputSet.nextClearBit(compositeInput.getInputIdentifier());
        int maxClearBit = compositeInput.getInputIdentifier() + compositeInput.getInputIdentifierCount();
        alreadyCompleted = nextClearBit > maxClearBit;
      } else {
        alreadyCompleted = completedInputSet.get(input.getInputIdentifier());
      }

      // Avoid adding attempts which have already completed
      if (alreadyCompleted) {
        inputIter.remove();
        continue;
      }
      // Avoid adding attempts which have been marked as OBSOLETE
      if (isObsoleteInputAttemptIdentifier(input)) {
        LOG.info("Skipping obsolete input: " + input);
        inputIter.remove();
        continue;
      }

      // Check if max threshold is met
      if (includedMaps >= maxTaskOutputAtOnce) {
        inputIter.remove();
        //add to inputHost
        inputHost.addKnownInput(pendingInputsOfOnePartitionRange.getPartition(),
            pendingInputsOfOnePartitionRange.getPartitionCount(), input);
      } else {
        includedMaps++;
      }
    }
    if (inputHost.getNumPendingPartitions() > 0) {
      pendingHosts.add(inputHost); //add it to queue
    }
    for(InputAttemptIdentifier input : pendingInputsOfOnePartitionRange.getInputs()) {
      ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(input.getInputIdentifier());
      if (eventInfo != null) {
        eventInfo.scheduledForDownload = true;
      }
    }
    fetcherBuilder.assignWork(inputHost.getHost(), inputHost.getPort(),
        pendingInputsOfOnePartitionRange.getPartition(),
        pendingInputsOfOnePartitionRange.getPartitionCount(),
            pendingInputsOfOnePartitionRange.getInputs());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created Fetcher for host: " + inputHost.getHost()
          + ", info: " + inputHost.getAdditionalInfo()
          + ", with inputs: " + pendingInputsOfOnePartitionRange);
    }
    return fetcherBuilder.build();
  }

  public List<RssFetcher> constructRssFetcher(InputHost inputHost) {
    assert inputHost.getNumPendingPartitions() > 0;
    List<RssFetcher> rssFetchers = new ArrayList<RssFetcher>();

    PartitionToInputs pendingInputs = inputHost.clearAndGetOnePartitionRange();
    assert pendingInputs.getPartitionCount() == 1;
    assert pendingInputs.getInputs().size() == 1;
    assert pendingInputs.getInputs().get(0) instanceof CompositeInputAttemptIdentifier;
    // CompositeInputAttemptIdentifier.getPartitionSize(pendingInputs.getPartition()) to be called

    CompositeInputAttemptIdentifier inputAttemptIdentifier =
        (CompositeInputAttemptIdentifier)pendingInputs.getInputs().get(0);

    // if inputContext.readPartitionAllOnce() == true, we check just one inputIdentifier
    boolean alreadyCompleted = completedInputSet.get(inputAttemptIdentifier.getInputIdentifier());
    // We do not check obsoletedInput because we do not rerun SourceTask when RSS is enabled.

    if (!alreadyCompleted) {
      if (inputContext.readPartitionAllOnce()) {
        for (InputAttemptIdentifier input: inputAttemptIdentifier.getInputIdentifiersForReadPartitionAllOnce()) {
          ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(input.getInputIdentifier());
          if (eventInfo != null) {
            eventInfo.scheduledForDownload = true;
          }
        }
      } else {
        ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputAttemptIdentifier.getInputIdentifier());
        if (eventInfo != null) {
          eventInfo.scheduledForDownload = true;
        }
      }

      long partitionTotalSize = inputAttemptIdentifier.getPartitionSize(pendingInputs.getPartition());

      if (inputContext.readPartitionAllOnce()) {
        List<InputAttemptIdentifier> inputAttemptIdentifiers =
            inputAttemptIdentifier.getInputIdentifiersForReadPartitionAllOnce();
        int numIdentifiers = inputAttemptIdentifiers.size();
        assert numIdentifiers == inputContext.getSourceVertexNumTasks();

        if (partitionTotalSize > rssFetchSplitThresholdSize) {
          // a single call to RSS would create a file larger than thresholdSize
          int numFetchers =
              Math.min((int)((partitionTotalSize - 1L) / rssFetchSplitThresholdSize) + 1, numIdentifiers);
          int numIndentifiersPerFetcher = numIdentifiers / numFetchers;
          int numLargeFetchers = numIdentifiers - numIndentifiersPerFetcher * numFetchers;
          assert numIdentifiers == numLargeFetchers * (numIndentifiersPerFetcher + 1) +
              (numFetchers - numLargeFetchers) * numIndentifiersPerFetcher;

          LOG.info("Splitting InputAttemptIdentifiers to {} RssFetchers: {} / {}",
              numFetchers, partitionTotalSize, numIdentifiers);

          int partitionId = pendingInputs.getPartition();
          int mapIndexStart = 0;
          int mapIndexEnd = numIdentifiers;
          for (int i = 0; i < numFetchers; i++) {
            int numExtra = i < numLargeFetchers ? 1 : 0;
            int numIdentifiersToConsume = numIndentifiersPerFetcher + numExtra;
            mapIndexEnd = mapIndexStart + numIdentifiersToConsume;

            List<InputAttemptIdentifier> subList = inputAttemptIdentifiers.subList(mapIndexStart, mapIndexEnd);
            long subTotalSize = 0L;
            for (InputAttemptIdentifier input: subList) {
              CompositeInputAttemptIdentifier cid = (CompositeInputAttemptIdentifier)input;
              subTotalSize += cid.getPartitionSize(partitionId);
            }

            long[] partitionSizes = new long[partitionId + 1];
            partitionSizes[partitionId] = subTotalSize;

            InputAttemptIdentifier firstId = subList.get(0);
            CompositeInputAttemptIdentifier mergedCid = new CompositeInputAttemptIdentifier(
                firstId.getInputIdentifier(),
                firstId.getAttemptNumber(),
                firstId.getPathComponent(),
                firstId.isShared(),
                firstId.getFetchTypeInfo(),
                firstId.getSpillEventId(),
                1, partitionSizes);
            mergedCid.setInputIdentifiersForReadPartitionAllOnce(subList);
            RssFetcher rssFetcher = new RssFetcher(this, inputManager, rssShuffleClient,
                inputContext.shuffleId(), inputHost.getHost(), inputHost.getPort(),
                partitionId, mergedCid,
                subTotalSize, mapIndexStart, mapIndexEnd, true);
            rssFetchers.add(rssFetcher);

            mapIndexStart = mapIndexEnd;
          }
          assert mapIndexEnd == numIdentifiers;
        } else {
          int mapIndexStart = 0;
          int mapIndexEnd = inputContext.getSourceVertexNumTasks();
          RssFetcher rssFetcher = new RssFetcher(this, inputManager, rssShuffleClient,
              inputContext.shuffleId(), inputHost.getHost(), inputHost.getPort(),
              pendingInputs.getPartition(), inputAttemptIdentifier,
              partitionTotalSize, mapIndexStart, mapIndexEnd, true);
          rssFetchers.add(rssFetcher);
        }
      } else {
        int mapIndexStart = Integer.parseInt(inputHost.getHost());
        int mapIndexEnd = mapIndexStart + 1;
        RssFetcher rssFetcher = new RssFetcher(this, inputManager, rssShuffleClient,
            inputContext.shuffleId(), inputHost.getHost(), inputHost.getPort(),
            pendingInputs.getPartition(), inputAttemptIdentifier,
            partitionTotalSize, mapIndexStart, mapIndexEnd, false);
        rssFetchers.add(rssFetcher);
      }
    }

    // If inputHost contains remaining InputAttemptIdentifiers, re-enqueue it.
    if (inputHost.getNumPendingPartitions() > 0) {
      pendingHosts.add(inputHost);
    }

    return rssFetchers;
  }

  /////////////////// Methods for InputEventHandler
  
  public void addKnownInput(String hostName, int port,
      CompositeInputAttemptIdentifier srcAttemptIdentifier, int srcPhysicalIndex) {
    // srcPhysicalIndex == partitionId
    // if rssShuffleClient != null, hostName == source task index (encoded as a string) and port == 0
    HostPort identifier = new HostPort(hostName, port);
    InputHost host = knownSrcHosts.get(identifier);
    if (host == null) {
      host = new InputHost(identifier,
          inputContext.readPartitionAllOnce(), inputContext.getSourceVertexNumTasks());
      InputHost old = knownSrcHosts.putIfAbsent(identifier, host);
      if (old != null) {
        host = old;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(srcNameTrimmed + ": " + "Adding input: " +
          srcAttemptIdentifier + ", to host: " + host);
    }

    if (!validateInputAttemptForPipelinedShuffle(srcAttemptIdentifier)) {
      return;
    }

    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    for (int i = 0; i < srcAttemptIdentifier.getInputIdentifierCount(); i++) {
      if (shuffleInfoEventsMap.get(inputIdentifier + i) == null) {
        shuffleInfoEventsMap.put(inputIdentifier + i, new ShuffleEventInfo(srcAttemptIdentifier.expand(i)));
      }
    }

    if (rssShuffleClient != null) {
      assert srcAttemptIdentifier.getInputIdentifierCount() == 1;
      assert srcAttemptIdentifier.canRetrieveInputInChunks();
      // validateInputAttemptForPipelinedShuffle() has been called already

      // for simplicity, consider srcAttemptIdentifier processed if it is not the last spill
      if (srcAttemptIdentifier.getFetchTypeInfo() != InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Fetching spill considered complete: " + srcAttemptIdentifier);
        }
        // simulate a call to addCompletedInputWithNoData() -> registerCompletedInputForPipelinedShuffle()
        // eventInfo.spillProcessed() is called outside lock.lock(), so do not get lock.lock()
        ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(srcAttemptIdentifier.getInputIdentifier());
        eventInfo.spillProcessed(srcAttemptIdentifier.getSpillEventId());
        numFetchedSpills.getAndIncrement();
        return;
      }
    }

    host.addKnownInput(srcPhysicalIndex, srcAttemptIdentifier.getInputIdentifierCount(), srcAttemptIdentifier);
    lock.lock();
    try {
      boolean added = pendingHosts.offer(host);
      if (!added) {
        String errorMessage = "Unable to add host: " + host.getIdentifier() + " to pending queue";
        LOG.error(errorMessage);
        throw new TezUncheckedException(errorMessage);
      }
      wakeLoop.signal();
    } finally {
      lock.unlock();
    }
  }

  public void addCompletedInputWithNoData(
      InputAttemptIdentifier srcAttemptIdentifier) {
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    if (LOG.isDebugEnabled()) {
      LOG.debug("No input data exists for SrcTask: " + inputIdentifier + ". Marking as complete.");
    }
    lock.lock();
    try {
      if (!completedInputSet.get(inputIdentifier)) {
        NullFetchedInput fetchedInput = new NullFetchedInput(srcAttemptIdentifier);
        if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
          registerCompletedInput(fetchedInput);
        } else {
          registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier, fetchedInput);
        }
      }
      // Awake the loop to check for termination.
      wakeLoop.signal();
    } finally {
      lock.unlock();
    }
  }

  public void addCompletedInputWithData(
      InputAttemptIdentifier srcAttemptIdentifier, FetchedInput fetchedInput)
      throws IOException {
    //InputIdentifier inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received Data via Event: " + srcAttemptIdentifier + " to "
          + fetchedInput.getType());
    }
    // Count irrespective of whether this is a copy of an already fetched input
    // lock.lock();
    // try {
    //   lastProgressTime = System.currentTimeMillis();
    // } finally {
    //   lock.unlock();
    // }

    boolean committed = false;
    if (!completedInputSet.get(inputIdentifier)) {
      synchronized (completedInputSet) {
        if (!completedInputSet.get(inputIdentifier)) {
          fetchedInput.commit();
          committed = true;
          if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
            registerCompletedInput(fetchedInput);
          } else {
            registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier,
                fetchedInput);
          }
        }
      }
    }
    if (!committed) {
      fetchedInput.abort(); // If this fails, the fetcher may attempt another
      // abort.
    } else {
      lock.lock();
      try {
        // Signal the wakeLoop to check for termination.
        wakeLoop.signal();
      } finally {
        lock.unlock();
      }
    }
  }

  protected synchronized  void updateEventReceivedTime() {
    long relativeTime = System.currentTimeMillis() - startTime;
    if (firstEventReceived.getValue() == 0) {
      firstEventReceived.setValue(relativeTime);
      lastEventReceived.setValue(relativeTime);
      return;
    }
    lastEventReceived.setValue(relativeTime);
  }

  void obsoleteKnownInput(InputAttemptIdentifier srcAttemptIdentifier) {
    obsoletedInputs.add(srcAttemptIdentifier);
  }

  /////////////////// End of Methods for InputEventHandler
  /////////////////// Methods from FetcherCallbackHandler

  /**
   * Placeholder for tracking shuffle events in case we get multiple spills info for the same
   * attempt.
   */
  static class ShuffleEventInfo {
    BitSet eventsProcessed;
    int finalEventId = -1; //0 indexed
    int attemptNum;
    String id;
    boolean scheduledForDownload; // whether chunks got scheduled for download


    ShuffleEventInfo(InputAttemptIdentifier input) {
      this.id = input.getInputIdentifier() + "_" + input.getAttemptNumber();
      this.eventsProcessed = new BitSet();
      this.attemptNum = input.getAttemptNumber();
    }

    void spillProcessed(int spillId) {
      if (finalEventId != -1) {
        Preconditions.checkState(eventsProcessed.cardinality() <= (finalEventId + 1),
            "Wrong state. eventsProcessed cardinality=" + eventsProcessed.cardinality() + " "
                + "finalEventId=" + finalEventId + ", spillId=" + spillId + ", " + toString());
      }
      eventsProcessed.set(spillId);
    }

    void setFinalEventId(int spillId) {
      finalEventId = spillId;
    }

    boolean isDone() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("finalEventId=" + finalEventId + ", eventsProcessed cardinality=" +
            eventsProcessed.cardinality());
      }
      return ((finalEventId != -1) && (finalEventId + 1) == eventsProcessed.cardinality());
    }

    public String toString() {
      return "[eventsProcessed=" + eventsProcessed + ", finalEventId=" + finalEventId
          +  ", id=" + id + ", attemptNum=" + attemptNum
          + ", scheduledForDownload=" + scheduledForDownload + "]";
    }
  }

  @Override
  public void fetchSucceeded(String host, InputAttemptIdentifier srcAttemptIdentifier,
      FetchedInput fetchedInput, long fetchedBytes, long decompressedLength, long copyDuration)
      throws IOException {
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();

    // Count irrespective of whether this is a copy of an already fetched input
    lock.lock();
    try {
      // lastProgressTime = System.currentTimeMillis();
      if (!completedInputSet.get(inputIdentifier)) {
        if (fetchedInput != null) {
          fetchedInput.commit();
          if (!inputContext.readPartitionAllOnce()) {
            fetchStatsLogger.logIndividualFetchComplete(copyDuration,
                fetchedBytes, decompressedLength, fetchedInput.getType().toString(), srcAttemptIdentifier);
          }

          // Processing counters for completed and commit fetches only. Need
          // additional counters for excessive fetches - which primarily comes
          // in after speculation or retries.
          shuffledInputsCounter.increment(1);
          bytesShuffledCounter.increment(fetchedBytes);
          if (fetchedInput.getType() == Type.MEMORY) {
            bytesShuffledToMemCounter.increment(fetchedBytes);
          } else if (fetchedInput.getType() == Type.DISK) {
            bytesShuffledToDiskCounter.increment(fetchedBytes);
          } else if (fetchedInput.getType() == Type.DISK_DIRECT) {
            bytesShuffledDirectDiskCounter.increment(fetchedBytes);
          }
          decompressedDataSizeCounter.increment(decompressedLength);

          if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
            registerCompletedInput(fetchedInput);
          } else {
            registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier, fetchedInput);
          }

          totalBytesShuffledTillNow += fetchedBytes;
          logProgress();
          wakeLoop.signal();
        } else {
          // only mark completion
          assert inputContext.readPartitionAllOnce();
          shuffledInputsCounter.increment(1);
          registerCompletedInputForPipelinedShuffleMarkOnly(srcAttemptIdentifier);
          logProgress();
        }
      } else {
        fetchedInput.abort();
      }
    } finally {
      lock.unlock();
    }
    // TODO NEWTEZ Maybe inform fetchers, in case they have an alternate attempt of the same task in their queue.
  }

  private void registerCompletedInput(FetchedInput fetchedInput) {
    lock.lock();
    try {
      maybeInformInputReady(fetchedInput);
      adjustCompletedInputs(fetchedInput);
      numFetchedSpills.getAndIncrement();
    } finally {
      lock.unlock();
    }
  }

  private void maybeInformInputReady(FetchedInput fetchedInput) {
    lock.lock();
    try {
      if (!(fetchedInput instanceof NullFetchedInput)) {
        completedInputs.add(fetchedInput);
      }
      if (!inputReadyNotificationSent.getAndSet(true)) {
        // TODO Should eventually be controlled by Inputs which are processing the data.
        inputContext.inputIsReady();
      }
    } finally {
      lock.unlock();
    }
  }

  private void adjustCompletedInputs(FetchedInput fetchedInput) {
    lock.lock();
    try {
      completedInputSet.set(fetchedInput.getInputAttemptIdentifier().getInputIdentifier());

      int numComplete = numCompletedInputs.incrementAndGet();
      if (numComplete == numInputs) {
        // Poison pill End of Input message to awake blocking take call
        if (fetchedInput instanceof NullFetchedInput) {
          completedInputs.add(fetchedInput);
        }
        LOG.info("All inputs fetched for input vertex : " + inputContext.getSourceVertexName());
      }
    } finally {
      lock.unlock();
    }
  }

  private void registerCompletedInputForPipelinedShuffle(InputAttemptIdentifier
      srcAttemptIdentifier, FetchedInput fetchedInput) {
    /**
     * For pipelinedshuffle it is possible to get multiple spills. Claim success only when
     * all spills pertaining to an attempt are done.
     */
    if (!validateInputAttemptForPipelinedShuffle(srcAttemptIdentifier)) {
      return;
    }

    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);

    //for empty partition case
    if (eventInfo == null && fetchedInput instanceof NullFetchedInput) {
      eventInfo = new ShuffleEventInfo(srcAttemptIdentifier);
      shuffleInfoEventsMap.put(inputIdentifier, eventInfo);
    }

    assert(eventInfo != null);
    eventInfo.spillProcessed(srcAttemptIdentifier.getSpillEventId());
    numFetchedSpills.getAndIncrement();

    if (srcAttemptIdentifier.getFetchTypeInfo() == InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE) {
      eventInfo.setFinalEventId(srcAttemptIdentifier.getSpillEventId());
    }

    lock.lock();
    try {
      /**
       * When fetch is complete for a spill, add it to completedInputs to ensure that it is
       * available for downstream processing. Final success will be claimed only when all
       * spills are downloaded from the source.
       */
      maybeInformInputReady(fetchedInput);

      //check if we downloaded all spills pertaining to this InputAttemptIdentifier
      if (eventInfo.isDone()) {
        adjustCompletedInputs(fetchedInput);
        shuffleInfoEventsMap.remove(srcAttemptIdentifier.getInputIdentifier());
      }
    } finally {
      lock.unlock();
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("eventInfo " + eventInfo.toString());
    }
  }

  private void registerCompletedInputForPipelinedShuffleMarkOnly(InputAttemptIdentifier srcAttemptIdentifier) {
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);

    // TODO: assert eventInfo != null (???)
    if (eventInfo == null) {
      eventInfo = new ShuffleEventInfo(srcAttemptIdentifier);
      shuffleInfoEventsMap.put(inputIdentifier, eventInfo);
    }

    eventInfo.spillProcessed(srcAttemptIdentifier.getSpillEventId());
    numFetchedSpills.getAndIncrement();

    assert srcAttemptIdentifier.getFetchTypeInfo() == InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE;
    eventInfo.setFinalEventId(srcAttemptIdentifier.getSpillEventId());

    lock.lock();
    try {
      if (eventInfo.isDone()) {
        completedInputSet.set(srcAttemptIdentifier.getInputIdentifier());
        numCompletedInputs.incrementAndGet();
        shuffleInfoEventsMap.remove(srcAttemptIdentifier.getInputIdentifier());
      }
    } finally {
      lock.unlock();
    }
    if (eventInfo.isDone()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Mark completion okay: {}/{}, {}", numCompletedInputs.get(), numInputs, srcAttemptIdentifier);
      }
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("eventInfo " + eventInfo.toString());
    }
  }

  private void reportFatalError(Throwable exception, String message) {
    LOG.error(message);
    inputContext.reportFailure(TaskFailureType.NON_FATAL, exception, message);
  }

  @Override
  public void fetchFailed(String host,
      InputAttemptIdentifier srcAttemptIdentifier, boolean connectFailed) {
    LOG.info("{}: Fetch failed for src: InputIdentifier: {}, connectFailed: {}",
        srcNameTrimmed, srcAttemptIdentifier, connectFailed);
    failedShufflesCounter.increment(1);
    if (srcAttemptIdentifier == null) {
      reportFatalError(null, "Received fetchFailure for an unknown src (null)");
    } 
    // we send InputReadError regardless of connectFailed (Cf. gla2019.6.10.pptx, page 21)
    {
      // TODO NEWTEZ. Implement logic to report fetch failures after a threshold.
      // For now, reporting immediately.
      if (isObsoleteInputAttemptIdentifier(srcAttemptIdentifier)) {
        LOG.info("Do not report obsolete input: " + srcAttemptIdentifier);
        return;
      }
      InputReadErrorEvent readError = InputReadErrorEvent.create(
          "Unordered: Fetch failure while fetching from "
              + TezRuntimeUtils.getTaskAttemptIdentifier(
              inputContext.getSourceVertexName(),
              srcAttemptIdentifier.getInputIdentifier(),
              srcAttemptIdentifier.getAttemptNumber()),
          srcAttemptIdentifier.getInputIdentifier(),
          srcAttemptIdentifier.getAttemptNumber());

      List<Event> failedEvents = Lists.newArrayListWithCapacity(1);
      failedEvents.add(readError);
      inputContext.sendEvents(failedEvents);
    }
  }

  private boolean isObsoleteInputAttemptIdentifier(InputAttemptIdentifier input) {
    if (input == null) {
      return false;
    }
    InputAttemptIdentifier obsoleteInput;
    Iterator<InputAttemptIdentifier> obsoleteInputsIter = obsoletedInputs.iterator();
    while (obsoleteInputsIter.hasNext()) {
      obsoleteInput = obsoleteInputsIter.next();
      if (input.include(obsoleteInput.getInputIdentifier(), obsoleteInput.getAttemptNumber())) {
        return true;
      }
    }
    return false;
  }

  /////////////////// End of Methods from FetcherCallbackHandler

  public void shutdown() throws InterruptedException {
    if (Thread.currentThread().isInterrupted()) {
      //TODO: need to cleanup all FetchedInput (DiskFetchedInput, LocalDisFetchedInput), lockFile
      //As of now relying on job cleanup (when all directories would be cleared)
      LOG.info(srcNameTrimmed + ": Thread interrupted. Need to cleanup the local dirs");
    }
    if (!isShutdown.getAndSet(true)) {
      // Shut down any pending fetchers
      LOG.info("Shutting down pending fetchers on source {}: {}", srcNameTrimmed, runningFetchers.size());
      lock.lock();
      try {
        wakeLoop.signal(); // signal the fetch-scheduler
        for (FetcherBase fetcher : runningFetchers) {
          try {
            fetcher.shutdown(); // This could be parallelized.
          } catch (Exception e) {
            LOG.warn(
                "Error while stopping fetcher during shutdown. Ignoring and continuing. Message={}",
                e.getMessage());
          }
        }
      } finally {
        lock.unlock();
      }

      if (this.schedulerExecutor != null && !this.schedulerExecutor.isShutdown()) {
        this.schedulerExecutor.shutdownNow();
      }
      if (this.fetcherExecutor != null && !this.fetcherExecutor.isShutdown()) {
        this.fetcherExecutor.shutdownNow(); // Interrupts all running fetchers.
      }
    }
  }

  /**
   * @return true if all of the required inputs have been fetched.
   */
  public boolean allInputsFetched() {
    lock.lock();
    try {
      return numCompletedInputs.get() == numInputs;
    } finally {
      lock.unlock();
    }
  }

  /**
   * @return the next available input, or null if there are no available inputs.
   *         This method will block if there are currently no available inputs,
   *         but more may become available.
   */
  public FetchedInput getNextInput() throws InterruptedException {
    // Check for no additional inputs
    lock.lock();
    try {
      if (completedInputs.peek() == null && allInputsFetched()) {
        return null;
      }
    } finally {
      lock.unlock();
    }
    // Block until next input or End of Input message
    FetchedInput fetchedInput = completedInputs.take();
    if (fetchedInput instanceof NullFetchedInput) {
      fetchedInput = null;
    }
    return fetchedInput;
  }

  public int getNumInputs() {
    return numInputs;
  }

  public float getNumCompletedInputsFloat() {
    return numCompletedInputs.floatValue();
  }

  /////////////////// End of methods for walking the available inputs


  /**
   * Fake input that is added to the completed input list in case an input does not have any data.
   *
   */
  @VisibleForTesting
  static class NullFetchedInput extends FetchedInput {

    public NullFetchedInput(InputAttemptIdentifier inputAttemptIdentifier) {
      super(inputAttemptIdentifier, null);
    }

    @Override
    public Type getType() {
      return Type.MEMORY;
    }

    @Override
    public long getSize() {
      return -1;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }

    @Override
    public InputStream getInputStream() throws IOException {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }

    @Override
    public void commit() throws IOException {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }

    @Override
    public void abort() throws IOException {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }

    @Override
    public void free() {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }
  }

  private final AtomicInteger nextProgressLineEventCount = new AtomicInteger(0);

  private void logProgress() {
    int inputsDone = numCompletedInputs.get();

    if (inputsDone > nextProgressLineEventCount.get() || inputsDone == numInputs) {
      nextProgressLineEventCount.addAndGet(1000);
      double mbs = (double) totalBytesShuffledTillNow / (1024 * 1024);
      long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;

      double transferRate = mbs / secsSinceStart;
      StringBuilder s = new StringBuilder();
      s.append("copy=" + inputsDone);
      s.append(", numFetchedSpills=" + numFetchedSpills);
      s.append(", numInputs=" + numInputs);
      s.append(", transfer rate (MB/s) = " + mbpsFormat.format(transferRate));  // CumulativeDataFetched/TimeSinceInputStarted
      LOG.info(s.toString());
    }
  }


  private class SchedulerFutureCallback implements FutureCallback<Void> {

    @Override
    public void onSuccess(Void result) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(srcNameTrimmed + ": Scheduler thread completed");
      }
    }

    @Override
    public void onFailure(Throwable t) {
      if (isShutdown.get()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(srcNameTrimmed + ": Already shutdown. Ignoring error: " + t);
        }
      } else {
        LOG.error(srcNameTrimmed + ": Scheduler failed with error: ", t);
        inputContext.reportFailure(TaskFailureType.NON_FATAL, t, "Shuffle Scheduler Failed");
      }
    }
    
  }
  
  private class FetchFutureCallback implements FutureCallback<FetchResult> {

    private final FetcherBase fetcher;
    
    public FetchFutureCallback(FetcherBase fetcher) {
      this.fetcher = fetcher;
    }
    
    private void doBookKeepingForFetcherComplete() {
      lock.lock();
      try {
        runningFetchers.remove(fetcher);
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
          LOG.debug(srcNameTrimmed + ": " + "Already shutdown. Ignoring event from fetcher");
        }
      } else {
        Iterable<InputAttemptIdentifier> pendingInputs = result.getPendingInputs();
        if (pendingInputs != null && pendingInputs.iterator().hasNext()) {
          HostPort identifier = new HostPort(result.getHost(),
              result.getPort());
          InputHost inputHost = knownSrcHosts.get(identifier);
          assert inputHost != null;
          for (InputAttemptIdentifier input : pendingInputs) {
            inputHost.addKnownInput(result.getPartition(), result.getPartitionCount(), input);
          }
          inputHost.setAdditionalInfo(result.getAdditionalInfo());
          pendingHosts.add(inputHost);
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
          LOG.debug(srcNameTrimmed + ": Already shutdown. Ignoring error from fetcher: " + t);
        }
      } else {
        LOG.error(srcNameTrimmed + ": Fetcher failed with error: ", t);
        shuffleError = t;
        inputContext.reportFailure(TaskFailureType.NON_FATAL, t, "Fetch failed");
        doBookKeepingForFetcherComplete();
      }
    }
  }
}

