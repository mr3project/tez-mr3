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

package org.apache.tez.runtime.common.resources;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.MemoryUpdateCallback;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.TaskContext;

import com.google.common.base.Function;
import org.apache.tez.common.Preconditions;
import com.google.common.collect.Iterables;

// Not calling this a MemoryManager explicitly. Not yet anyway.
public class MemoryDistributor {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryDistributor.class);

  private final int numTotalInputs;
  private final int numTotalOutputs;
  private final Configuration conf;
  
  private AtomicInteger numInputsSeen = new AtomicInteger(0);
  private AtomicInteger numOutputsSeen = new AtomicInteger(0);

  private final long totalTaskMemoryBytes;
  private final boolean isScaleMemoryEnabled;
  private final Set<TaskContext> dupSet = Collections
      .newSetFromMap(new ConcurrentHashMap<TaskContext, Boolean>());
  private final List<RequestorInfo> requestList;

  /**
   * @param numTotalInputs
   *          total number of Inputs for the task
   * @param numTotalOutputs
   *          total number of Outputs for the task
   * @param conf
   *          Tez specific task configuration
   */
  public MemoryDistributor(
      int numTotalInputs, int numTotalOutputs, Configuration conf, long totalTaskMemoryBytes,
      String taskAttemptIdStr) {
    this.conf = conf;
    this.isScaleMemoryEnabled = conf.getBoolean(TezConfiguration.TEZ_TASK_SCALE_MEMORY_ENABLED,
        TezConfiguration.TEZ_TASK_SCALE_MEMORY_ENABLED_DEFAULT);

    this.numTotalInputs = numTotalInputs;
    this.numTotalOutputs = numTotalOutputs;
    this.totalTaskMemoryBytes = totalTaskMemoryBytes;
    this.requestList = Collections.synchronizedList(new LinkedList<RequestorInfo>());
    LOG.info("InitialMemoryDistributor for {}: isScaleMemoryEnabled={}, numInputs={}, numOutputs={}, totalTaskMemoryBytes={}",
        taskAttemptIdStr, isScaleMemoryEnabled, numTotalInputs, numTotalOutputs, totalTaskMemoryBytes);
  }

  /**
   * Used by the Tez framework to request memory on behalf of user requests.
   */
  public void requestMemory(long requestSize, MemoryUpdateCallback callback,
      TaskContext taskContext, InitialMemoryRequestContext.RequestType requestType) {
    registerRequest(requestSize, callback, taskContext, requestType);
  }

  /**
   * Used by the Tez framework to distribute initial memory after components
   * have made their initial requests.
   * @throws TezException
   */
  public void makeInitialAllocations() throws TezException {
    Preconditions.checkState(numInputsSeen.get() == numTotalInputs, "All inputs are expected to ask for memory");
    Preconditions.checkState(numOutputsSeen.get() == numTotalOutputs, "All outputs are expected to ask for memory");

    if (LOG.isDebugEnabled()) {
      logInitialRequests(requestList);
    }

    Iterable<InitialMemoryRequestContext> requestContexts = Iterables.transform(requestList,
        new Function<RequestorInfo, InitialMemoryRequestContext>() {
          public InitialMemoryRequestContext apply(RequestorInfo requestInfo) {
            return requestInfo.getRequestContext();
          }
        });

    Iterable<Long> allocations = null;
    if (!isScaleMemoryEnabled) {
      allocations = Iterables.transform(requestList, new Function<RequestorInfo, Long>() {
        public Long apply(RequestorInfo requestInfo) {
          return requestInfo.getRequestContext().getRequestedSize();
        }
      });
    } else {
      WeightedScalingMemoryDistributor allocator = new WeightedScalingMemoryDistributor();
      allocator.setConf(conf);
      allocations = allocator.assignMemory(totalTaskMemoryBytes, numTotalInputs, numTotalOutputs,
          Iterables.unmodifiableIterable(requestContexts));
      validateAllocations(allocations, requestList.size());
      if (LOG.isDebugEnabled()) {
        logFinalAllocations(allocations, requestList);
      }
    }

    // Making the callbacks directly for now, instead of spawning threads. The
    // callback implementors - all controlled by Tez at the moment are
    // lightweight.
    Iterator<Long> allocatedIter = allocations.iterator();
    for (RequestorInfo rInfo : requestList) {
      long allocated = allocatedIter.next();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Informing: " + rInfo.getRequestContext().getComponentType() + ", "
            + rInfo.getRequestContext().getComponentVertexName() + ", "
            + rInfo.getRequestContext().getRequestType() + ": requested="
            + rInfo.getRequestContext().getRequestedSize() + ", allocated=" + allocated);
      }
      rInfo.getCallback().memoryAssigned(allocated);
    }
  }

  private void registerRequest(long requestSize, MemoryUpdateCallback callback,
      TaskContext entityContext, InitialMemoryRequestContext.RequestType requestType) {
    Preconditions.checkArgument(requestSize >= 0);
    Objects.requireNonNull(callback);
    Objects.requireNonNull(entityContext);
    Objects.requireNonNull(requestType);
    if (!dupSet.add(entityContext)) {
      throw new TezUncheckedException(
          "A single entity can only make one call to request resources for now");
    }

    RequestorInfo requestInfo = new RequestorInfo(entityContext, requestSize, callback, requestType);
    switch (requestInfo.getRequestContext().getComponentType()) {
    case INPUT:
      numInputsSeen.incrementAndGet();
      Preconditions.checkState(numInputsSeen.get() <= numTotalInputs,
          "Num Requesting Inputs higher than total # of inputs: {}, {}",
          numInputsSeen, numTotalInputs);
      break;
    case OUTPUT:
      numOutputsSeen.incrementAndGet();
      Preconditions.checkState(numOutputsSeen.get() <= numTotalOutputs,
          "Num Requesting Inputs higher than total # of outputs: {}, {}",
          numOutputsSeen, numTotalOutputs);
      break;
    case PROCESSOR:
      break;
    default:
      break;
    }
    requestList.add(requestInfo);
  }

  private void validateAllocations(Iterable<Long> allocations, int numRequestors) {
    Objects.requireNonNull(allocations);
    long totalAllocated = 0l;
    int numAllocations = 0;
    for (Long l : allocations) {
      totalAllocated += l;
      numAllocations++;
    }
    Preconditions.checkState(numAllocations == numRequestors,
        "Number of allocations {} must match number of requesters {}",
        numAllocations, numRequestors);
    Preconditions.checkState(totalAllocated <= totalTaskMemoryBytes,
        "Total allocation {} should be <= totalTaskMemoryBytes {}",
        totalAllocated, totalTaskMemoryBytes);
  }

  private static class RequestorInfo {

    private final MemoryUpdateCallback callback;
    private final InitialMemoryRequestContext requestContext;

    public RequestorInfo(TaskContext taskContext, long requestSize,
        final MemoryUpdateCallback callback, InitialMemoryRequestContext.RequestType requestType) {
      InitialMemoryRequestContext.ComponentType type;
      String componentVertexName;
      if (taskContext instanceof InputContext) {
        type = InitialMemoryRequestContext.ComponentType.INPUT;
        componentVertexName = ((InputContext) taskContext).getSourceVertexName();
      } else if (taskContext instanceof OutputContext) {
        type = InitialMemoryRequestContext.ComponentType.OUTPUT;
        componentVertexName = ((OutputContext) taskContext).getDestinationVertexName();
      } else if (taskContext instanceof ProcessorContext) {
        type = InitialMemoryRequestContext.ComponentType.PROCESSOR;
        componentVertexName = ((ProcessorContext) taskContext).getTaskVertexName();
      } else {
        throw new IllegalArgumentException("Unknown type of entityContext: "
            + taskContext.getClass().getName());
      }
      this.requestContext = new InitialMemoryRequestContext(requestSize, requestType, type, componentVertexName);
      this.callback = callback;
    }

    public MemoryUpdateCallback getCallback() {
      return callback;
    }

    public InitialMemoryRequestContext getRequestContext() {
      return requestContext;
    }
  }

  private void logInitialRequests(List<RequestorInfo> initialRequests) {
    if (initialRequests != null && !initialRequests.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < initialRequests.size(); i++) {
        InitialMemoryRequestContext context = initialRequests.get(i).getRequestContext();
        sb.append("[");
        sb.append(context.getComponentVertexName()).append(":");
        sb.append(context.getComponentType()).append(":");
        sb.append(context.getRequestedSize()).append(":").append(context.getRequestType());
        sb.append("]");
        if (i < initialRequests.size() - 1) {
          sb.append(", ");
        }
      }
      LOG.debug("InitialRequests=" + sb.toString());
    }
  }

  private void logFinalAllocations(Iterable<Long> allocations, List<RequestorInfo> requestList) {
    if (requestList != null && !requestList.isEmpty()) {
      Iterator<Long> allocatedIter = allocations.iterator();
      StringBuilder sb = new StringBuilder();

      for (int i = 0 ; i < requestList.size() ; i++) {
        long allocated = allocatedIter.next();
        InitialMemoryRequestContext context = requestList.get(i).getRequestContext();
        sb.append("[");
        sb.append(context.getComponentVertexName()).append(":");
        sb.append(context.getRequestType()).append(":");
        sb.append(context.getComponentType()).append(":");
        sb.append(context.getRequestedSize()).append(":").append(allocated);
        sb.append("]");
        if (i < requestList.size() - 1) {
          sb.append(", ");
        }
      }
      LOG.debug("Allocations=" + sb.toString());
    }
  }
}
