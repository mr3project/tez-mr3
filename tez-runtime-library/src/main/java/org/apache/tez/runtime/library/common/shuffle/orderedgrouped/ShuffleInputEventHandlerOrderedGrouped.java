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

package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.Inflater;

import com.google.protobuf.UnsafeByteOperations;
import org.apache.tez.runtime.api.events.CompositeRoutedDataMovementEvent;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.ShuffleEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.apache.tez.util.StringInterner;

import com.google.protobuf.InvalidProtocolBufferException;

public class ShuffleInputEventHandlerOrderedGrouped implements ShuffleEventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleInputEventHandlerOrderedGrouped.class);

  private final ShuffleScheduler shuffleScheduler;
  private final InputContext inputContext;
  private final boolean compositeFetch;
  private final Inflater inflater;

  private final AtomicInteger numDmeEvents = new AtomicInteger(0);
  private final AtomicInteger numObsoletionEvents = new AtomicInteger(0);
  private final AtomicInteger numDmeEventsNoData = new AtomicInteger(0);

  private final int portIndex;

  public ShuffleInputEventHandlerOrderedGrouped(InputContext inputContext,
      ShuffleScheduler shuffleScheduler,
      boolean compositeFetch) {
    this.inputContext = inputContext;
    this.shuffleScheduler = shuffleScheduler;
    this.compositeFetch = compositeFetch;
    this.inflater = TezCommonUtils.newInflater();

    int taskIndex = inputContext.getTaskIndex();
    int vertexIndex = inputContext.getTaskVertexIndex();
    this.portIndex = vertexIndex * 7 + taskIndex;
  }

  @Override
  public void handleEvents(List<Event> events) throws IOException {
    for (Event event : events) {
      handleEvent(event);
    }
    shuffleScheduler.wakeupLoop();
  }

  @Override
  public void logProgress(boolean updateOnClose) {
    StringBuilder s = new StringBuilder();
    s.append(inputContext.getSourceVertexName());
    s.append(", numDmeEventsSeen=" + numDmeEvents.get());
    s.append(", numDmeEventsSeenWithNoData=" + numDmeEventsNoData.get());
    s.append(", numObsoletionEventsSeen=" + numObsoletionEvents.get());
    s.append(updateOnClose ? ", updateOnClose" : "");
    LOG.info(s.toString());
  }

  private void handleEvent(Event event) throws IOException {
    if (event instanceof DataMovementEvent) {
      numDmeEvents.incrementAndGet();
      DataMovementEvent dmEvent = (DataMovementEvent)event;
      DataMovementEventPayloadProto shufflePayload;
      try {
        shufflePayload = DataMovementEventPayloadProto.parseFrom(UnsafeByteOperations.unsafeWrap(dmEvent.getUserPayload()));
      } catch (InvalidProtocolBufferException e) {
        throw new TezUncheckedException("Unable to parse DataMovementEvent payload", e);
      }
      BitSet emptyPartitionsBitSet = null;
      if (shufflePayload.hasEmptyPartitions()) {
        try {
          byte[] emptyPartitions = TezCommonUtils.decompressByteStringToByteArray(shufflePayload.getEmptyPartitions(), inflater);
          emptyPartitionsBitSet = TezUtilsInternal.fromByteArray(emptyPartitions);
        } catch (IOException e) {
          throw new TezUncheckedException("Unable to set the empty partition to succeeded", e);
        }
      }
      processDataMovementEvent(dmEvent, shufflePayload, emptyPartitionsBitSet);
    } else if (event instanceof CompositeRoutedDataMovementEvent) {
      CompositeRoutedDataMovementEvent crdme = (CompositeRoutedDataMovementEvent)event;
      DataMovementEventPayloadProto shufflePayload;
      try {
        shufflePayload = DataMovementEventPayloadProto.parseFrom(UnsafeByteOperations.unsafeWrap(crdme.getUserPayload()));
      } catch (InvalidProtocolBufferException e) {
        throw new TezUncheckedException("Unable to parse DataMovementEvent payload", e);
      }
      BitSet emptyPartitionsBitSet = null;
      if (shufflePayload.hasEmptyPartitions()) {
        try {
          byte[] emptyPartitions = TezCommonUtils.decompressByteStringToByteArray(shufflePayload.getEmptyPartitions(), inflater);
          emptyPartitionsBitSet = TezUtilsInternal.fromByteArray(emptyPartitions);
        } catch (IOException e) {
          throw new TezUncheckedException("Unable to set the empty partition to succeeded", e);
        }
      }
      if (compositeFetch) {
        numDmeEvents.addAndGet(crdme.getCount());
        processCompositeRoutedDataMovementEvent(crdme, shufflePayload, emptyPartitionsBitSet);
      } else {
        for (int offset = 0; offset < crdme.getCount(); offset++) {
          numDmeEvents.incrementAndGet();
          processDataMovementEvent(crdme.expand(offset), shufflePayload, emptyPartitionsBitSet);
        }
      }
    } else if (event instanceof InputFailedEvent) {
      numObsoletionEvents.incrementAndGet();
      processTaskFailedEvent((InputFailedEvent) event);
    }

    if (numDmeEvents.get() == shuffleScheduler.getNumInputs() ||
        numDmeEvents.get() + numObsoletionEvents.get() == shuffleScheduler.getNumInputs() ||
        numDmeEvents.get() - numObsoletionEvents.get() == shuffleScheduler.getNumInputs()) {
      logProgress(false);
    }
  }

  private void processDataMovementEvent(
      DataMovementEvent dmEvent, DataMovementEventPayloadProto shufflePayload, BitSet emptyPartitionsBitSet) {
    int partitionId = dmEvent.getSourceIndex();
    CompositeInputAttemptIdentifier srcAttemptIdentifier = constructInputAttemptIdentifier(
        dmEvent.getTargetIndex(), 1, dmEvent.getVersion(), shufflePayload);

    if (LOG.isDebugEnabled()) {
      LOG.debug("DME srcIdx: " + partitionId + ", targetIdx: " + dmEvent.getTargetIndex()
          + ", attemptNum: " + dmEvent.getVersion() + ", payload: " +
          ShuffleUtils.stringify(shufflePayload));
    }

    if (shufflePayload.hasEmptyPartitions()) {
      try {
        if (emptyPartitionsBitSet.get(partitionId)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Source partition: " + partitionId + " did not generate any data. SrcAttempt: ["
              + srcAttemptIdentifier + "]. Not fetching.");
          }
          numDmeEventsNoData.getAndIncrement();
          shuffleScheduler.fetchSucceeded(srcAttemptIdentifier.expand(0), null, 0, 0, 0);
          return;
        }
      } catch (IOException e) {
        throw new TezUncheckedException("Unable to set the empty partition to succeeded", e);
      }
    }
    int port = getShufflePort(shufflePayload, dmEvent.getTargetIndex());
    shuffleScheduler.addKnownMapOutput(
        StringInterner.intern(shufflePayload.getHost()),
        StringInterner.intern(shufflePayload.getContainerId()),
        port, partitionId, srcAttemptIdentifier);
  }

  private void processCompositeRoutedDataMovementEvent(
      CompositeRoutedDataMovementEvent crdmEvent,
      DataMovementEventPayloadProto shufflePayload,
      BitSet emptyPartitionsBitSet) throws IOException {
    int partitionId = crdmEvent.getSourceIndex();
    CompositeInputAttemptIdentifier compositeInputAttemptIdentifier = constructInputAttemptIdentifier(
        crdmEvent.getTargetIndex(), crdmEvent.getCount(), crdmEvent.getVersion(), shufflePayload);

    if (LOG.isDebugEnabled()) {
      LOG.debug("DME srcIdx: " + partitionId + ", targetIdx: " + crdmEvent.getTargetIndex() + ", count:" + crdmEvent.getCount()
          + ", attemptNum: " + crdmEvent.getVersion() + ", payload: " +
          ShuffleUtils.stringify(shufflePayload));
    }

    if (shufflePayload.hasEmptyPartitions()) {
      boolean allPartitionsEmpty = true;
      for (int i = 0; i < crdmEvent.getCount(); i++) {
        int srcPartitionId = partitionId + i;
        allPartitionsEmpty &= emptyPartitionsBitSet.get(srcPartitionId);
        if (emptyPartitionsBitSet.get(srcPartitionId)) {
          InputAttemptIdentifier srcInputAttemptIdentifier = compositeInputAttemptIdentifier.expand(i);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Source partition: " + srcPartitionId + " did not generate any data. SrcAttempt: ["
                + srcInputAttemptIdentifier + "]. Not fetching.");
          }
          numDmeEventsNoData.getAndIncrement();
          shuffleScheduler.fetchSucceeded(srcInputAttemptIdentifier, null, 0, 0, 0);
        }
      }

      if (allPartitionsEmpty) {
        return;
      }
    }

    int port = getShufflePort(shufflePayload, crdmEvent.getTargetIndex());
    shuffleScheduler.addKnownMapOutput(
        StringInterner.intern(shufflePayload.getHost()),
        StringInterner.intern(shufflePayload.getContainerId()),
        port, partitionId, compositeInputAttemptIdentifier);
  }

  private int getShufflePort(DataMovementEventPayloadProto shufflePayload, int targetIndex) {
    int numPorts = shufflePayload.getNumPorts();
    return numPorts > 0 ? shufflePayload.getPorts((portIndex + targetIndex) % numPorts) : 0;
  }

  private void processTaskFailedEvent(InputFailedEvent ifEvent) {
    InputAttemptIdentifier taIdentifier = new InputAttemptIdentifier(ifEvent.getTargetIndex(), ifEvent.getVersion());
    shuffleScheduler.obsoleteKnownInput(taIdentifier);
  }

  /**
   * Helper method to create InputAttemptIdentifier
   *
   * @param targetIndex
   * @param targetIndexCount
   * @param version
   * @param shufflePayload
   * @return CompositeInputAttemptIdentifier
   */
  private CompositeInputAttemptIdentifier constructInputAttemptIdentifier(int targetIndex, int targetIndexCount, int version,
      DataMovementEventPayloadProto shufflePayload) {
    String pathComponent = (shufflePayload.hasPathComponent()) ? StringInterner.intern(shufflePayload.getPathComponent()) : null;
    CompositeInputAttemptIdentifier srcAttemptIdentifier = null;
    if (shufflePayload.hasSpillId()) {
      int spillEventId = shufflePayload.getSpillId();
      boolean lastEvent = shufflePayload.getLastEvent();
      InputAttemptIdentifier.SPILL_INFO info = (lastEvent) ? InputAttemptIdentifier.SPILL_INFO
          .FINAL_UPDATE : InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE;
      srcAttemptIdentifier =
          new CompositeInputAttemptIdentifier(targetIndex, version, pathComponent, info, spillEventId, targetIndexCount);
    } else {
      srcAttemptIdentifier =
          new CompositeInputAttemptIdentifier(targetIndex, version, pathComponent, targetIndexCount);
    }
    return srcAttemptIdentifier;
  }
}

