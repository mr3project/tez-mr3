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
package org.apache.tez.runtime.library.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.runtime.library.api.KeyValuesReaderEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.RawComparator;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.MergedLogicalInput;
import org.apache.tez.runtime.api.MergedInputContext;

/**
 * A {@link MergedLogicalInput} which merges multiple
 * {@link OrderedGroupedKVInput}s and returns a single view of these by merging
 * values which belong to the same key.
 * 
 * Combiners and Secondary Sort are not implemented, so there is no guarantee on
 * the order of values.
 */
public class OrderedGroupedMergedKVInput extends MergedLogicalInput {

  private static final Logger LOG = LoggerFactory.getLogger(OrderedGroupedMergedKVInput.class);
  private final Set<Input> completedInputs = Collections
      .newSetFromMap(new IdentityHashMap<Input, Boolean>());

  public OrderedGroupedMergedKVInput(MergedInputContext context, List<Input> inputs) {
    super(context, inputs);
  }

  /**
   * Provides an ordered {@link KeyValuesReaderEdge}
   */
  @Override
  public KeyValuesReaderEdge getReader() throws Exception {
    return new OrderedGroupedMergedKeyValuesReader(getInputs(), getContext());
  }

  @Override
  public void setConstituentInputIsReady(Input input) {
    synchronized (completedInputs) {
      completedInputs.add(input);
      if (completedInputs.size() == getInputs().size()) {
        informInputReady();
      }
    }
  }

  private static class OrderedGroupedMergedKeyValuesReader extends KeyValuesReaderEdge {
    private final PriorityQueue<KeyValuesReaderEdge> pQueue;
    @SuppressWarnings("rawtypes")
    private final RawComparator keyComparator;
    private final List<KeyValuesReaderEdge> finishedReaders;
    private final ValuesIterable currentValues;
    private KeyValuesReaderEdge nextKVReader;
    private BytesWritable currentKey;
    private final MergedInputContext context;

    public OrderedGroupedMergedKeyValuesReader(List<Input> inputs, MergedInputContext context) 
        throws Exception {
      keyComparator = ((OrderedGroupedKVInput) inputs.get(0))
          .getInputKeyComparator();
      pQueue = new PriorityQueue<KeyValuesReaderEdge>(inputs.size(),
          new KVReaderComparator(keyComparator));
      finishedReaders = new ArrayList<KeyValuesReaderEdge>(inputs.size());
      for (Input input : inputs) {
        KeyValuesReaderEdge reader = (KeyValuesReaderEdge) input.getReader();
        if (reader.next()) {
          pQueue.add(reader);
        }
      }
      currentValues = new ValuesIterable();
      this.context = context;
    }

    private void advanceAndAddToQueue(KeyValuesReaderEdge kvsReadr)
        throws IOException {
      if (kvsReadr.next()) {
        pQueue.add(kvsReadr);
      }
    }

    private void addToQueue(KeyValuesReaderEdge kvsReadr) throws IOException {
      if (kvsReadr != null) {
        pQueue.add(kvsReadr);
      }
    }

    @Override
    public boolean next() throws IOException {
      // Skip values of current key if not consumed by the user
      currentValues.discardCurrent();

      for (KeyValuesReaderEdge reader : finishedReaders) {
        // add them back to queue
        advanceAndAddToQueue(reader);
      }
      finishedReaders.clear();

      nextKVReader = pQueue.poll();
      if (nextKVReader != null) {
        currentKey = nextKVReader.getCurrentKey();
        currentValues.moveToNext();
        return true;
      } else {
        hasCompletedProcessing();
        completedProcessing = true;
      }
      return false;
    }

    @Override
    public BytesWritable getCurrentKey() throws IOException {
      return currentKey;
    }

    @Override
    public Iterable<BytesWritable> getCurrentValues() throws IOException {
      return currentValues;
    }

    private class ValuesIterable implements Iterable<BytesWritable> {
      private ValuesIterator iterator = new ValuesIterator();

      @Override
      public Iterator<BytesWritable> iterator() {
        return iterator;
      }

      public void discardCurrent() throws IOException {
        iterator.discardCurrent();
      }

      public void moveToNext() throws IOException {
        iterator.moveToNext();
      }

    }

    @SuppressWarnings("unchecked")
    private class ValuesIterator implements Iterator<BytesWritable> {

      private Iterator<BytesWritable> currentValuesIter;

      public void moveToNext() throws IOException {
        currentValuesIter = nextKVReader.getCurrentValues().iterator();
      }

      @Override
      public boolean hasNext() {
        if (currentValuesIter != null) { // No current key. next needs to be called.
          if (currentValuesIter.hasNext()) {
            return true;
          } else {
            finishedReaders.add(nextKVReader);
            nextKVReader = pQueue.poll();
            try {
              if (nextKVReader != null
                  && keyComparator.compare(currentKey, nextKVReader.getCurrentKey()) == 0) {
                currentValuesIter = nextKVReader.getCurrentValues().iterator();
                return true;
              } else { // key changed or no more data.
                // Add the reader back to queue
                addToQueue(nextKVReader);
                currentValuesIter = null;
                return false;
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        } else {
          return false;
        }
      }

      public void discardCurrent() throws IOException {
        if (currentValuesIter != null) {
          do {
            finishedReaders.add(nextKVReader);
            nextKVReader = pQueue.poll();
          } while (nextKVReader != null
              && keyComparator.compare(currentKey, nextKVReader.getCurrentKey()) == 0);
          addToQueue(nextKVReader);
          currentValuesIter = null;
        }
      }

      @Override
      public BytesWritable next() {
        return currentValuesIter.next();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

    }

    /**
     * Comparator that compares KeyValuesReaderEdge on their current key
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static class KVReaderComparator implements Comparator<KeyValuesReaderEdge> {

      private final RawComparator keyComparator;

      public KVReaderComparator(RawComparator keyComparator) {
        this.keyComparator = keyComparator;
      }

      @Override
      public int compare(KeyValuesReaderEdge o1, KeyValuesReaderEdge o2) {
        try {
          return keyComparator.compare(o1.getCurrentKey(), o2.getCurrentKey());
        } catch (IOException e) {
          LOG.error("Caught exception while comparing keys in shuffle input", e);
          throw new RuntimeException(e);
        }
      }
    }
  }
}
