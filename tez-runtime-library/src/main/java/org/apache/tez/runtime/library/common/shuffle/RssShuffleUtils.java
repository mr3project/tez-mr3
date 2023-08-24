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

import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class RssShuffleUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleUtils.class);

  public static final int RSS_SHUFFLE_HEADER_SIZE = 2 * Long.BYTES;

  public static boolean checkUseSameAttemptNumber(CompositeInputAttemptIdentifier inputAttemptIdentifier) {
    List<CompositeInputAttemptIdentifier> childInputAttemptIdentifiers = inputAttemptIdentifier.getInputIdentifiersForReadPartitionAllOnce();
    int attemptNumber = childInputAttemptIdentifiers.get(0).getAttemptNumber();
    boolean useSameAttemptNumber = true;
    for (CompositeInputAttemptIdentifier input: childInputAttemptIdentifiers) {
      if (input.getAttemptNumber() != attemptNumber) {
        LOG.error("Different attemptNumber found: {}, {}, {}",
            input.getAttemptNumber(), attemptNumber, input.getTaskIndex());
        useSameAttemptNumber = false;
        break;
      }
    }
    return useSameAttemptNumber;
  }

  public interface FetcherCreate {
    void operate(
        CompositeInputAttemptIdentifier mergedCid,
        long subTotalSize, int mapIndexStart, int mapIndexEnd);
  }

  public static void createRssFetchersForReadPartitionAllOnce(
      CompositeInputAttemptIdentifier inputAttemptIdentifier,
      int partitionId,
      int sourceVertexNumTasks, long rssFetchSplitThresholdSize,
      FetcherCreate createFn, boolean ordered) {
    long partitionTotalSize = inputAttemptIdentifier.getPartitionSize(partitionId);
    List<CompositeInputAttemptIdentifier> inputAttemptIdentifiers =
        inputAttemptIdentifier.getInputIdentifiersForReadPartitionAllOnce();
    int numIdentifiers = inputAttemptIdentifiers.size();
    assert numIdentifiers == sourceVertexNumTasks;
    assert partitionTotalSize == inputAttemptIdentifiers.stream()
        .map(x -> x.getPartitionSize(partitionId)).mapToLong(Long::longValue).sum();

    Collections.sort(inputAttemptIdentifiers,
        Comparator.comparingInt(CompositeInputAttemptIdentifier::getTaskIndex));
    int mapIndexOrigin = inputAttemptIdentifiers.get(0).getTaskIndex();
    int mapIndexLast = mapIndexOrigin + numIdentifiers;
    assert mapIndexLast == inputAttemptIdentifiers.get(numIdentifiers - 1).getTaskIndex() + 1;

    if (partitionTotalSize > rssFetchSplitThresholdSize) {
      // a single call to RSS would create a file larger than thresholdSize
      int numFetchers =
          Math.min((int)((partitionTotalSize - 1L) / rssFetchSplitThresholdSize) + 1, numIdentifiers);
      int numIdentifiersPerFetcher = numIdentifiers / numFetchers;
      int numLargeFetchers = numIdentifiers - numIdentifiersPerFetcher * numFetchers;
      assert numIdentifiers == numLargeFetchers * (numIdentifiersPerFetcher + 1) +
          (numFetchers - numLargeFetchers) * numIdentifiersPerFetcher;

      int mapIndexStart = mapIndexOrigin;
      int mapIndexEnd = -1;

      LOG.info("{} - Splitting InputAttemptIdentifiers to {} RssFetchers: {} / {}, task index {} - {}",
          ordered ? "Ordered" : "Unordered",
          numFetchers, partitionTotalSize, numIdentifiers,
          mapIndexStart, inputAttemptIdentifiers.get(numIdentifiers - 1).getTaskIndex());
      assert mapIndexStart + numIdentifiers - 1 == inputAttemptIdentifiers.get(numIdentifiers - 1).getTaskIndex();

      for (int i = 0; i < numFetchers; i++) {
        int numExtra = i < numLargeFetchers ? 1 : 0;
        int numIdentifiersToConsume = numIdentifiersPerFetcher + numExtra;
        mapIndexEnd = mapIndexStart + numIdentifiersToConsume;

        List<CompositeInputAttemptIdentifier> subList =
            inputAttemptIdentifiers.subList(mapIndexStart - mapIndexOrigin, mapIndexEnd - mapIndexOrigin);
        long subTotalSize = 0L;
        for (CompositeInputAttemptIdentifier cid: subList) {
          assert (mapIndexStart <= cid.getTaskIndex() && cid.getTaskIndex() < mapIndexEnd) :
              "mapIndexStart=" + mapIndexStart + ", taskIndex=" + cid.getTaskIndex() + ", mapIndexEnd=" + mapIndexEnd;
          subTotalSize += cid.getPartitionSize(partitionId);
          LOG.info("Fetcher batch #{} - taskIndex = {}, size = {}",   // TODO: remove
              i, cid.getTaskIndex(), cid.getPartitionSize(partitionId));
        }
        LOG.info("Fetcher batch #{} --- total partition size = {}", i, subTotalSize);

        // TODO: optimize for mapIndexEnd == mapIndexStart + 1 (where numIdentifiersToConsume == 1)

        // optimize partitionSizes[] because only partitionId is used and other fields are never used
        long[] partitionSizes = new long[1];
        partitionSizes[0] = subTotalSize;

        InputAttemptIdentifier firstId = subList.get(0);
        CompositeInputAttemptIdentifier mergedCid = new CompositeInputAttemptIdentifier(
            firstId.getInputIdentifier(),
            firstId.getAttemptNumber(),
            firstId.getPathComponent(),
            firstId.isShared(),
            firstId.getFetchTypeInfo(),
            firstId.getSpillEventId(),
            1, partitionSizes, -1);
        mergedCid.setInputIdentifiersForReadPartitionAllOnce(subList);
        createFn.operate(mergedCid, subTotalSize, mapIndexStart, mapIndexEnd);
        mapIndexStart = mapIndexEnd;
      }
      assert mapIndexEnd == mapIndexLast;

      LOG.info("{} - Finished - Splitting InputAttemptIdentifiers to {} RssFetchers: {} / {}",
          ordered ? "Ordered" : "Unordered",
          numFetchers, partitionTotalSize, numIdentifiers);
    } else {
      createFn.operate(inputAttemptIdentifier, partitionTotalSize, mapIndexOrigin, mapIndexLast);
      LOG.info("{} - Finished - Creating a single RssFetcher: {} / {}, from {} to {}",
          ordered ? "Ordered" : "Unordered",
          partitionTotalSize, numIdentifiers, mapIndexOrigin, mapIndexLast);
    }
  }
}
