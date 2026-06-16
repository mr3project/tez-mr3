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

import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.common.resources.InitialMemoryRequestContext.ComponentType;
import org.apache.tez.runtime.common.resources.InitialMemoryRequestContext.RequestType;

import org.apache.tez.common.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Distributes memory between various requesting components by applying a
 * weighted scaling function. Overall, ensures that all requestors stay within the JVM limits.
 *
 * Configuration involves specifying weights for the different Inputs available
 * in the tez-runtime-library. As an example, SortedShuffle : SortedOutput :
 * UnsortedShuffle could be configured to be 20:10:1. In this case, if both
 * SortedShuffle and UnsortedShuffle ask for the same amount of initial memory,
 * SortedShuffle will be given 20 times more; both may be scaled down to fit within the JVM though.
 *
 */
public class WeightedScalingMemoryDistributor implements InitialMemoryAllocator {

  private static final Logger LOG = LoggerFactory.getLogger(WeightedScalingMemoryDistributor.class);

  static final double MAX_ADDITIONAL_RESERVATION_FRACTION_PER_IO = 0.1d;
  static final double RESERVATION_FRACTION_PER_IO = 0.015d;
  static final long PARTITIONED_UNSORTED_OUTPUT_UNSCALED_THRESHOLD_BYTES = 20L << 20;

  // TODO: update populateTypeScaleMap() as well
  static final String[] DEFAULT_TASK_MEMORY_WEIGHTED_RATIOS =
      generateWeightStrings(1, 1, 1, 1, 1, 1, 1);

  private static class Request {
    ComponentType componentType;
    long requestSize;
    private final RequestType requestType;
    private int requestWeight;

    Request(ComponentType componentType, long requestSize,
            RequestType requestType, int requestWeight) {
      this.componentType = componentType;
      this.requestSize = requestSize;
      this.requestType = requestType;
      this.requestWeight = requestWeight;
    }
  }

  private Configuration conf;

  public WeightedScalingMemoryDistributor() {
  }

  private final EnumMap<RequestType, Integer> typeScaleMap = Maps.newEnumMap(RequestType.class);

  private int numRequests = 0;

  private final List<Request> requests = Lists.newArrayList();

  @Override
  public Iterable<Long> assignMemory(long availableBytesForAllocation, int numTotalInputs,
      int numTotalOutputs, Iterable<InitialMemoryRequestContext> initialRequests) {

    // Read in configuration
    populateTypeScaleMap();

    for (InitialMemoryRequestContext context : initialRequests) {
      initialProcessMemoryRequestContext(context);
    }

    // Take a certain amount of memory away for general usage.
    double reserveFraction = computeReservedFraction(numRequests);
    Preconditions.checkState(reserveFraction >= 0.0d && reserveFraction <= 1.0d);

    availableBytesForAllocation = (long) (availableBytesForAllocation - (reserveFraction * availableBytesForAllocation));

    Set<Request> unscaledRequests = new HashSet<Request>();
    long unscaledRequestTotal = 0;
    boolean unscaledRequestsFit = true;
    for (Request request : requests) {
      if (request.requestType == RequestType.PARTITIONED_UNSORTED_OUTPUT &&
          request.requestSize <= PARTITIONED_UNSORTED_OUTPUT_UNSCALED_THRESHOLD_BYTES) {
        unscaledRequests.add(request);
        if (request.requestSize > availableBytesForAllocation - unscaledRequestTotal) {
          unscaledRequestsFit = false;
          break;
        }
        unscaledRequestTotal += request.requestSize;
      }
    }
    if (!unscaledRequestsFit) {
      unscaledRequests.clear();
      unscaledRequestTotal = 0;
    }

    long availableBytesForScaling = availableBytesForAllocation - unscaledRequestTotal;
    int remainingRequestWeights = 0;
    for (Request request : requests) {
      if (!unscaledRequests.contains(request)) {
        remainingRequestWeights += request.requestWeight;
      }
    }
    if (remainingRequestWeights == 0) {
      // Fall back to regular scaling for requests that were not allocated in full.
      for (Request request : requests) {
        if (!unscaledRequests.contains(request)) {
          request.requestWeight = 1;
          remainingRequestWeights++;
        }
      }
    }

    // Scale down while adding requests - don't want to hit Long limits.
    double totalScaledRequest = 0d;
    for (Request request : requests) {
      if (!unscaledRequests.contains(request)) {
        totalScaledRequest +=
            request.requestSize * (request.requestWeight / (double) remainingRequestWeights);
      }
    }

    // Actual allocation
    List<Long> allocations = Lists.newArrayListWithCapacity(numRequests);
    for (Request request : requests) {
      if (unscaledRequests.contains(request)) {
        allocations.add(request.requestSize);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Allocating requested " + request.requestType + " of type "
              + request.requestType + " " + request.requestSize + " without scaling");
        }
      } else if (request.requestSize == 0) {
        allocations.add(0L);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Scaling requested " + request.requestType + " of type "
              + request.requestType + " 0 to allocated: 0");
        }
      } else {
        double requestFactor = request.requestWeight / (double) remainingRequestWeights;
        double scaledRequest = requestFactor * request.requestSize;
        long allocated = Math.min(
            (long) ((scaledRequest / totalScaledRequest) * availableBytesForScaling), request.requestSize);
        allocations.add(allocated);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Scaling requested " + request.requestType + " of type "
              + request.requestType + " " + request.requestSize + "  to allocated: " + allocated);
        }
      }
    }

    return allocations;
  }

  private void initialProcessMemoryRequestContext(InitialMemoryRequestContext context) {
    numRequests++;

    RequestType requestType = context.getRequestType();
    Integer typeScaleFactor = getScaleFactorForType(requestType);
    Request request = new Request(
        context.getComponentType(), context.getRequestedSize(), requestType, typeScaleFactor);

    requests.add(request);
  }

  private Integer getScaleFactorForType(RequestType requestType) {
    Integer typeScaleFactor = typeScaleMap.get(requestType);
    if (typeScaleFactor == null) {
      LOG.warn("Bad scale factor for requestType: {}, using factor 0", requestType);
      typeScaleFactor = 0;
    }
    return typeScaleFactor;
  }

  private void populateTypeScaleMap() {
    if (conf.get(TezConfiguration.TEZ_TASK_SCALE_MEMORY_WEIGHTED_RATIOS) == null) {
      // set according to DEFAULT_TASK_MEMORY_WEIGHTED_RATIOS
      typeScaleMap.put(RequestType.PARTITIONED_UNSORTED_OUTPUT, 1);
      typeScaleMap.put(RequestType.UNSORTED_OUTPUT, 1);
      typeScaleMap.put(RequestType.UNSORTED_INPUT, 1);
      typeScaleMap.put(RequestType.SORTED_OUTPUT, 1);
      typeScaleMap.put(RequestType.SORTED_MERGED_INPUT, 1);
      typeScaleMap.put(RequestType.PROCESSOR, 1);
      typeScaleMap.put(RequestType.OTHER, 1);
      return;
    }

    String[] ratios = conf.getStrings(TezConfiguration.TEZ_TASK_SCALE_MEMORY_WEIGHTED_RATIOS,
        DEFAULT_TASK_MEMORY_WEIGHTED_RATIOS);
    int numExpectedValues = RequestType.values().length;
    if (ratios == null) {
      LOG.info("No ratio specified. Falling back to Linear scaling");
      ratios = new String[numExpectedValues];
      int i = 0;
      for (RequestType requestType : RequestType.values()) {
        ratios[i] = requestType.name() + ":1"; // Linear scale
        i++;
      }
    } else {
      if (ratios.length != RequestType.values().length) {
        throw new IllegalArgumentException(
            "Number of entries in the configured ratios should be equal to the number of entries in RequestType: "
                + numExpectedValues);
      }
    }

    StringBuilder sb = new StringBuilder();
    Set<RequestType> seenTypes = new HashSet<RequestType>();
    for (String ratio : ratios) {
      String[] parts = ratio.split(":");
      Preconditions.checkState(parts.length == 2);
      RequestType requestType = RequestType.valueOf(parts[0]);
      Integer ratioVal = Integer.parseInt(parts[1]);
      if (!seenTypes.add(requestType)) {
        throw new IllegalArgumentException("Cannot configure the same RequestType: " + requestType
            + " multiple times");
      }
      Preconditions.checkState(ratioVal >= 0, "Ratio must be >= 0");
      typeScaleMap.put(requestType, ratioVal);
      sb.append("[").append(requestType).append(":").append(ratioVal).append("]");
    }
    LOG.info("Scale ratios constructed={}", sb.toString());
  }

  private double computeReservedFraction(int numTotalRequests) {
    double reserveFractionPerIo = conf.getDouble(
        TezConfiguration.TEZ_TASK_SCALE_MEMORY_ADDITIONAL_RESERVATION_FRACTION_PER_IO,
        RESERVATION_FRACTION_PER_IO);
    double maxAdditionalReserveFraction = conf.getDouble(
        TezConfiguration.TEZ_TASK_SCALE_MEMORY_ADDITIONAL_RESERVATION_FRACTION_MAX,
        MAX_ADDITIONAL_RESERVATION_FRACTION_PER_IO);
    Preconditions.checkArgument(
        maxAdditionalReserveFraction >= 0.0d && maxAdditionalReserveFraction <= 1.0d);
    Preconditions.checkArgument(
        reserveFractionPerIo >= 0.0d && reserveFractionPerIo <= maxAdditionalReserveFraction);

    double initialReserveFraction = conf.getDouble(
        TezConfiguration.TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION,
        TezConfiguration.TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION_DEFAULT);
    double additionalReserveFraction = Math.min(
        maxAdditionalReserveFraction, numTotalRequests * reserveFractionPerIo);

    return initialReserveFraction + additionalReserveFraction;
  }

  public static String[] generateWeightStrings(int unsortedPartitioned, int unsorted,
      int broadcastIn, int sortedOut, int scatterGatherShuffleIn, int proc, int other) {
    String[] weights = new String[RequestType.values().length];
    weights[0] = RequestType.PARTITIONED_UNSORTED_OUTPUT.name() + ":" + unsortedPartitioned;
    weights[1] = RequestType.UNSORTED_OUTPUT.name() + ":" + unsorted;
    weights[2] = RequestType.UNSORTED_INPUT.name() + ":" + broadcastIn;
    weights[3] = RequestType.SORTED_OUTPUT.name() + ":" + sortedOut;
    weights[4] = RequestType.SORTED_MERGED_INPUT.name() + ":" + scatterGatherShuffleIn;
    weights[5] = RequestType.PROCESSOR.name() + ":" + proc;
    weights[6] = RequestType.OTHER.name() + ":" + other;
    return weights;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }
}
