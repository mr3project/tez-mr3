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

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class RssShuffleUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleUtils.class);

  public static final int EOF_MARKERS_SIZE = 2 * WritableUtils.getVIntSize(IFile.EOF_MARKER);

  public static void shuffleToMemory(InputStream inputStream, byte[] buffer, long dataLength)
      throws IOException {
    IOUtils.readFully(inputStream, buffer, 0, (int) dataLength);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    WritableUtils.writeVInt(dos, IFile.EOF_MARKER);
    WritableUtils.writeVInt(dos, IFile.EOF_MARKER);
    byte[] eofMarker = baos.toByteArray();

    System.arraycopy(eofMarker, 0, buffer, (int) dataLength, EOF_MARKERS_SIZE);
  }

  public static long shuffleToDisk(InputStream inputStream, OutputStream outputStream,
      long dataLength) throws IOException {
    byte[] buffer = new byte[ShuffleUtils.BUFFER_SIZE];
    boolean reachEOF = false;
    long bytesWritten = 0L;
    while (!reachEOF) {
      int curBytesRead = inputStream.read(buffer, 0, ShuffleUtils.BUFFER_SIZE);

      if (curBytesRead <= 0) {
        reachEOF = true;
      } else {
        reachEOF = curBytesRead < ShuffleUtils.BUFFER_SIZE;

        outputStream.write(buffer, 0, curBytesRead);
        bytesWritten += curBytesRead;
      }
    }

    assert !(dataLength != -1L) || dataLength == bytesWritten;

    DataOutputStream dos = new DataOutputStream(outputStream);
    WritableUtils.writeVInt(dos, IFile.EOF_MARKER);
    WritableUtils.writeVInt(dos, IFile.EOF_MARKER);
    bytesWritten += EOF_MARKERS_SIZE;

    return bytesWritten;
  }

  public static boolean checkUseSameAttemptNumber(CompositeInputAttemptIdentifier inputAttemptIdentifier) {
    List<CompositeInputAttemptIdentifier> childInputAttemptIdentifiers = inputAttemptIdentifier.getInputIdentifiersForReadPartitionAllOnce();
    int attemptNumber = childInputAttemptIdentifiers.get(0).getAttemptNumber();
    boolean useSameAttemptNumber = true;
    for (InputAttemptIdentifier input: childInputAttemptIdentifiers) {
      if (input.getAttemptNumber() != attemptNumber) {
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

    if (partitionTotalSize > rssFetchSplitThresholdSize) {
      // a single call to RSS would create a file larger than thresholdSize
      int numFetchers =
          Math.min((int)((partitionTotalSize - 1L) / rssFetchSplitThresholdSize) + 1, numIdentifiers);
      int numIdentifiersPerFetcher = numIdentifiers / numFetchers;
      int numLargeFetchers = numIdentifiers - numIdentifiersPerFetcher * numFetchers;
      assert numIdentifiers == numLargeFetchers * (numIdentifiersPerFetcher + 1) +
          (numFetchers - numLargeFetchers) * numIdentifiersPerFetcher;

      LOG.info("{} - Splitting InputAttemptIdentifiers to {} RssFetchers: {} / {}",
          ordered ? "Ordered" : "Unordered",
          numFetchers, partitionTotalSize, numIdentifiers);

      if (ordered) {
        Collections.sort(inputAttemptIdentifiers,
          Comparator.comparingInt(CompositeInputAttemptIdentifier::getTaskIndex));
      }

      int mapIndexStart = 0;
      int mapIndexEnd = -1;
      for (int i = 0; i < numFetchers; i++) {
        int numExtra = i < numLargeFetchers ? 1 : 0;
        int numIdentifiersToConsume = numIdentifiersPerFetcher + numExtra;
        mapIndexEnd = mapIndexStart + numIdentifiersToConsume;

        List<CompositeInputAttemptIdentifier> subList = inputAttemptIdentifiers.subList(mapIndexStart, mapIndexEnd);
        long subTotalSize = 0L;
        for (CompositeInputAttemptIdentifier cid: subList) {
          assert mapIndexStart <= cid.getTaskIndex() && cid.getTaskIndex() < mapIndexEnd;
          subTotalSize += cid.getPartitionSize(partitionId);
        }

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
      assert mapIndexEnd == numIdentifiers;
    } else {
      int mapIndexStart = 0;
      int mapIndexEnd = sourceVertexNumTasks;
      createFn.operate(inputAttemptIdentifier, partitionTotalSize, mapIndexStart, mapIndexEnd);
    }
  }
}
