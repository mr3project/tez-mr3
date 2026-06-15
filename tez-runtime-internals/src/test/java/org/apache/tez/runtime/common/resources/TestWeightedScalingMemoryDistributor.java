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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.common.resources.InitialMemoryRequestContext.ComponentType;
import org.apache.tez.runtime.common.resources.InitialMemoryRequestContext.RequestType;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestWeightedScalingMemoryDistributor {

  private static final long MB = 1L << 20;

  @Test
  public void testSmallPartitionedUnsortedOutputReceivesFullRequest() {
    List<Long> allocations = allocate(50 * MB,
        request(10, RequestType.PARTITIONED_UNSORTED_OUTPUT),
        request(100, RequestType.SORTED_OUTPUT));

    assertEquals(10 * MB, allocations.get(0).longValue());
    assertEquals(40 * MB, allocations.get(1).longValue());
  }

  @Test
  public void testRequestAtThresholdReceivesFullRequest() {
    List<Long> allocations = allocate(30 * MB,
        request(20, RequestType.PARTITIONED_UNSORTED_OUTPUT),
        request(100, RequestType.SORTED_OUTPUT));

    assertEquals(20 * MB, allocations.get(0).longValue());
    assertEquals(10 * MB, allocations.get(1).longValue());
  }

  @Test
  public void testRequestAboveThresholdIsScaled() {
    List<Long> allocations = allocate(20 * MB,
        request(21, RequestType.PARTITIONED_UNSORTED_OUTPUT),
        request(100, RequestType.SORTED_OUTPUT));

    assertTrue(allocations.get(0) < 21 * MB);
  }

  @Test
  public void testOtherRequestTypeIsNotExempt() {
    List<Long> allocations = allocate(50 * MB,
        request(10, RequestType.UNSORTED_OUTPUT),
        request(100, RequestType.SORTED_OUTPUT));

    assertTrue(allocations.get(0) < 10 * MB);
  }

  @Test
  public void testMultipleEligibleRequestsFit() {
    List<Long> allocations = allocate(50 * MB,
        request(10, RequestType.PARTITIONED_UNSORTED_OUTPUT),
        request(20, RequestType.PARTITIONED_UNSORTED_OUTPUT),
        request(100, RequestType.SORTED_OUTPUT));

    assertEquals(10 * MB, allocations.get(0).longValue());
    assertEquals(20 * MB, allocations.get(1).longValue());
    assertEquals(20 * MB, allocations.get(2).longValue());
  }

  @Test
  public void testEligibleRequestsFallBackToScalingWhenTheyDoNotFit() {
    List<Long> allocations = allocate(20 * MB,
        request(15, RequestType.PARTITIONED_UNSORTED_OUTPUT),
        request(15, RequestType.PARTITIONED_UNSORTED_OUTPUT));

    assertEquals(10 * MB, allocations.get(0).longValue());
    assertEquals(10 * MB, allocations.get(1).longValue());
  }

  @Test
  public void testReservedFractionIsHonored() {
    Configuration conf = createConfiguration();
    conf.setDouble(TezConfiguration.TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION, 0.3d);

    List<Long> allocations = allocate(conf, 100 * MB,
        request(20, RequestType.PARTITIONED_UNSORTED_OUTPUT),
        request(100, RequestType.SORTED_OUTPUT));

    assertEquals(20 * MB, allocations.get(0).longValue());
    assertEquals(50 * MB, allocations.get(1).longValue());
    assertEquals(70 * MB, sum(allocations));
  }

  @Test
  public void testRemainingZeroWeightRequestsUseLinearScaling() {
    Configuration conf = createConfiguration();
    conf.setStrings(TezConfiguration.TEZ_TASK_SCALE_MEMORY_WEIGHTED_RATIOS,
        WeightedScalingMemoryDistributor.generateWeightStrings(1, 0, 0, 0, 0, 0, 0));

    List<Long> allocations = allocate(conf, 40 * MB,
        request(10, RequestType.PARTITIONED_UNSORTED_OUTPUT),
        request(20, RequestType.UNSORTED_OUTPUT),
        request(40, RequestType.OTHER));

    assertEquals(10 * MB, allocations.get(0).longValue());
    assertEquals(10 * MB, allocations.get(1).longValue());
    assertEquals(20 * MB, allocations.get(2).longValue());
  }

  @Test
  public void testAllRequestsEligibleAndFit() {
    List<Long> allocations = allocate(40 * MB,
        request(10, RequestType.PARTITIONED_UNSORTED_OUTPUT),
        request(20, RequestType.PARTITIONED_UNSORTED_OUTPUT));

    assertEquals(10 * MB, allocations.get(0).longValue());
    assertEquals(20 * MB, allocations.get(1).longValue());
  }

  @Test
  public void testZeroSizedRequestRemainsZero() {
    List<Long> allocations = allocate(20 * MB,
        request(0, RequestType.OTHER),
        request(10, RequestType.PARTITIONED_UNSORTED_OUTPUT));

    assertEquals(0, allocations.get(0).longValue());
    assertEquals(10 * MB, allocations.get(1).longValue());
  }

  private static InitialMemoryRequestContext request(long megabytes, RequestType requestType) {
    return new InitialMemoryRequestContext(megabytes * MB, requestType, ComponentType.OUTPUT,
        "destination");
  }

  private static List<Long> allocate(long availableBytes,
      InitialMemoryRequestContext... requests) {
    return allocate(createConfiguration(), availableBytes, requests);
  }

  private static List<Long> allocate(Configuration conf, long availableBytes,
      InitialMemoryRequestContext... requests) {
    WeightedScalingMemoryDistributor distributor = new WeightedScalingMemoryDistributor();
    distributor.setConf(conf);
    return Lists.newArrayList(distributor.assignMemory(availableBytes, 0, requests.length,
        Arrays.asList(requests)));
  }

  private static Configuration createConfiguration() {
    Configuration conf = new Configuration(false);
    conf.setDouble(TezConfiguration.TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION, 0d);
    conf.setDouble(TezConfiguration.TEZ_TASK_SCALE_MEMORY_ADDITIONAL_RESERVATION_FRACTION_PER_IO,
        0d);
    conf.setDouble(TezConfiguration.TEZ_TASK_SCALE_MEMORY_ADDITIONAL_RESERVATION_FRACTION_MAX, 0d);
    return conf;
  }

  private static long sum(List<Long> allocations) {
    long total = 0;
    for (long allocation : allocations) {
      total += allocation;
    }
    return total;
  }
}
