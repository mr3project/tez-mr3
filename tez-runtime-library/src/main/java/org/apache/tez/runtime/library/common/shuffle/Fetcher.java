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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.http.BaseHttpConnection;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;

import java.io.DataInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

// T = FetcherInput
public abstract class Fetcher<T extends ShuffleInput> implements Callable<FetchResult> {

  protected static final ThreadLocal<CompressionCodec> codecHolder = new ThreadLocal<>();
  protected static final AtomicInteger fetcherIdGen = new AtomicInteger(0);

  protected final ShuffleServer fetcherCallback;
  protected final Configuration conf;
  protected final String applicationId;
  protected final ShuffleServer.FetcherConfig fetcherConfig;
  protected final TaskContext taskContext;

  public final InputHost inputHost;
  protected final String host;
  protected final int port;

  protected final int numInputs;
  protected final InputHost.PartitionToInputs pendingInputsSeq;   // contents are also immutable after initialization
  protected final int minPartition;
  protected final int maxPartition;

  public final int attempt;   // 0, 1, 2, ...

  //
  // fields set during the execution of call()
  //

  // Maps from the pathComponents (unique per srcTaskId) to the specific taskId
  // filled in buildPathToAttemptMap() at the beginning of call()
  protected final Map<ShuffleServer.PathPartition, InputAttemptIdentifier> pathToAttemptMap;

  // Initial value is 0, which means it hasn't retried yet.
  protected long retryStartTime = 0;

  // volatile because it can be read in multiple threads
  protected volatile BaseHttpConnection httpConnection;
  protected volatile DataInputStream input;

  protected CompressionCodec codec;

  // Set at the start of call(), so may be invalid when accessed from ShuffleServer.call() thread
  // Hence, we initialize it to Long.MAX_VALUE.
  public volatile long startMillis = Long.MAX_VALUE;

  public static int STAGE_INITIAL = 0;  // all until STAGE_FIRST_FETCHED
  public static int STAGE_FIRST_FETCHED = 1;
  private volatile int stage = STAGE_INITIAL;

  // accessed only from
  public static int STATE_NORMAL = 10;
  public static int STATE_STUCK = 11;
  public static int STATE_RECOVERED = 12;
  public static int STATE_SPECULATIVE = 13;
  private volatile int state = STATE_NORMAL;

  // read by ShuffleServer
  public long getStartMillis() {
    return startMillis;
  }

  // set by Fetcher
  protected void setStage(int newStage) {
    stage = newStage;
  }

  // read by ShuffleServer
  public int getStage() {
    return stage;
  }

  // set by ShuffleServer
  public void setState(int newState) {
    state = newState;
  }

  // read by Fetcher
  protected int getState() {
    return state;
  }

  public Fetcher(ShuffleServer fetcherCallback,
                 Configuration conf,
                 InputHost inputHost,
                 ShuffleServer.FetcherConfig fetcherConfig,
                 TaskContext taskContext,
                 InputHost.PartitionToInputs pendingInputsSeq,
                 int attempt) {
    this.fetcherCallback = fetcherCallback;
    this.conf = conf;
    this.applicationId = taskContext.getApplicationId().toString();
    this.fetcherConfig = fetcherConfig;
    this.taskContext = taskContext;

    this.inputHost = inputHost;
    this.host = inputHost.getHost();
    this.port = inputHost.getPort();

    this.numInputs = pendingInputsSeq.getInputs().size();
    this.pendingInputsSeq = pendingInputsSeq;
    this.minPartition = pendingInputsSeq.getPartition();
    this.maxPartition = pendingInputsSeq.getPartition() + pendingInputsSeq.getPartitionCount() - 1;

    this.attempt = attempt;

    this.pathToAttemptMap = new HashMap<ShuffleServer.PathPartition, InputAttemptIdentifier>();
  }

  abstract public ShuffleClient<T> getShuffleClient();
  abstract public boolean useSingleShuffleClientId(Long shuffleClientId);
  abstract public String getFetcherIdentifier();
  abstract public void shutdown();
  abstract public FetchResult call() throws Exception;
  abstract public Fetcher<T> createClone();

  protected InputAttemptIdentifier[] buildInputSeqFromIndex(int pendingInputsIndex) {
    // TODO: just create a sub-array
    InputAttemptIdentifier[] inputsSeq = new InputAttemptIdentifier[numInputs - pendingInputsIndex];
    for (int i = pendingInputsIndex; i < numInputs; i++) {
      inputsSeq[i - pendingInputsIndex] = pendingInputsSeq.getInputs().get(i);
    }
    return inputsSeq;
  }

  protected void buildPathToAttemptMap() {
    // partitionId == common to all InputAttemptIdentifiers == DME.sourceIndex
    // we read from 'partitionId + 0' to 'partitionId + partitionCount -1' partition in 'pathComponent'
    int partitionId = pendingInputsSeq.getPartition();
    int partitionCount = pendingInputsSeq.getPartitionCount();

    // build pathToAttemptMap[]
    for (int i = 0; i < numInputs; i++) {
      String pathComponent = pendingInputsSeq.getInputs().get(i).getPathComponent();
      InputAttemptIdentifier input = pendingInputsSeq.getInputs().get(i);

      if (input instanceof CompositeInputAttemptIdentifier) {
        CompositeInputAttemptIdentifier cin = (CompositeInputAttemptIdentifier)input;
        assert cin.getInputIdentifierCount() == partitionCount;

        for (int k = 0; k < partitionCount; k++) {
          ShuffleServer.PathPartition pp = new ShuffleServer.PathPartition(pathComponent, partitionId + k);
          assert !pathToAttemptMap.containsKey(pp);
          pathToAttemptMap.put(pp, cin.expand(k));
        }
      } else {
        assert partitionCount == 1;
        ShuffleServer.PathPartition pp = new ShuffleServer.PathPartition(pathComponent, partitionId);
        assert !pathToAttemptMap.containsKey(pp);
        pathToAttemptMap.put(pp, input);
      }
    }
  }

  protected Map<InputAttemptIdentifier, InputHost.PartitionRange> buildInputMapFromIndex(int pendingInputsIndex) {
    Map<InputAttemptIdentifier, InputHost.PartitionRange> inputsMap = new HashMap<>();
    for (int i = pendingInputsIndex; i <  numInputs; i++) {
      inputsMap.put(pendingInputsSeq.getInputs().get(i), pendingInputsSeq.getPartitionRange());
    }
    return inputsMap;
  }
}
