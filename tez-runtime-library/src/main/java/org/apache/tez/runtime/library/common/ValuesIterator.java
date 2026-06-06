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

package org.apache.tez.runtime.library.common;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.runtime.library.common.sort.impl.TezRawDataBuffer;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.library.common.comparator.TezBytesComparator;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;

import org.apache.tez.common.Preconditions;

/**
 * Iterates values while keys match in sorted input.
 *
 * This class is not thread safe. Accessing methods from multiple threads will
 * lead to corrupt data.
 *
 */
public class ValuesIterator {

  protected TezRawKeyValueIterator in;  // input iterator
  private BytesWritable key;            // current key
  private BytesWritable nextKey;
  private BytesWritable value;          // current value
  private boolean more;                 // more in file
  private boolean currentRecordStable;
  private final TezCounter inputKeyCounter;
  private final TezCounter inputValueCounter;

  private int keyCtr = 0;
  private boolean hasMoreValues; // For the current key.
  private boolean isFirstRecord = true;

  private boolean completedProcessing;

  public ValuesIterator(TezRawKeyValueIterator in,
                        TezCounter inputKeyCounter,
                        TezCounter inputValueCounter) {
    this.in = in;
    this.inputKeyCounter = inputKeyCounter;
    this.inputValueCounter = inputValueCounter;
  }

  TezRawKeyValueIterator getRawIterator() { return in; }

  /**
   * Move to the next K-Vs pair
   * @return true if another pair exists, otherwise false.
   * @throws IOException
   */
  public boolean moveToNext() throws IOException {
    if (isFirstRecord) {
      readNextKey();
      key = nextKey;
      nextKey = null;
      isFirstRecord = false;
    } else {
      nextKey();
    }
    if (!more) {
      hasCompletedProcessing();
      completedProcessing = true;
    }
    return more;
  }

  /** The current key. */
  // Invariant:
  //   The backing byte[] array of BytesWritable is immutable, so the consumer may keep pointers to it.
  public BytesWritable getKey() {
    return key;
  }

  // Invariant:
  //   The backing byte[] array of BytesWritable is immutable, so the consumer may keep pointers to it.
  public Iterable<BytesWritable> getValues() {
    return new Iterable<BytesWritable>() {

      @Override
      public Iterator<BytesWritable> iterator() {
        return new Iterator<BytesWritable>() {

          private final int keyNumber = keyCtr;

          @Override
          public boolean hasNext() {
            return hasMoreValues;
          }

          @Override
          public BytesWritable next() {
            if (!hasMoreValues) {
              throw new NoSuchElementException("iterate past last value");
            }
            Preconditions.checkState(keyNumber == keyCtr,
                "Cannot use values iterator on the previous K-V pair after moveToNext has been invoked to move to the next K-V pair");

            try {
              readNextValue();
              readNextKey();
            } catch (IOException ie) {
              throw new RuntimeException("problem advancing post rec#"+keyCtr, ie);
            }
            inputValueCounter.increment(1);
            return value;
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException("Cannot remove elements");
          }
        };
      }
    };
  }

  /** Start processing next unique key. */
  private void nextKey() throws IOException {
    // read until we find a new key
    while (hasMoreValues) {
      readNextKey();
    }

    // move the next key to the current one
    BytesWritable tmpKey = key;
    key = nextKey;
    nextKey = tmpKey;
    hasMoreValues = more;
  }

  /**
   * read the next key - which may be the same as the current key.
   */
  private void readNextKey() throws IOException {
    int nextResult = in.next();
    more = nextResult != TezRawKeyValueIterator.NO_MORE_KEY_VALUE;
    currentRecordStable = nextResult == TezRawKeyValueIterator.NEXT_KEY_VALUE_STABLE;
    if (more) {
      TezRawDataBuffer nextKeyBytes = in.getKey();
      if (!in.isSameKey()) {
        nextKey = copyToWritable(nextKey, nextKeyBytes, currentRecordStable);
        // hasMoreValues = is it first key or is key the same?
        hasMoreValues = (key == null) || (TezBytesComparator.compare(key, nextKey) == 0);
        if (key == null || !hasMoreValues) {
          // invariant: more=true & there are no more values in an existing key group
          // so this indicates start of new key group
          if(inputKeyCounter != null) {
            inputKeyCounter.increment(1);
          }
          ++keyCtr;
        }
      } else {
        hasMoreValues = in.isSameKey();
      }
    } else {
      hasMoreValues = false;
    }
  }

  /**
   * Read the next value
   * @throws IOException
   */
  private void readNextValue() throws IOException {
    TezRawDataBuffer nextValueBytes = in.getValue();
    value = copyToWritable(value, nextValueBytes, currentRecordStable);
  }

  private BytesWritable copyToWritable(BytesWritable writable, TezRawDataBuffer source, boolean stable) {
    BytesWritable target = writable;
    if (target == null) {
      target = new BytesWritable();
    }

    int pos = source.getPosition();
    int length = source.getLength() - pos;
    if (stable) {
      target.setDirect(source.getData(), pos, length);
    } else {
      byte[] bytes = target.reinitialize(length);
      System.arraycopy(source.getData(), pos, bytes, 0, length);
    }

    return target;
  }

  /**
   * Check whether processing has been completed.
   *
   * @throws IOException
   */
  protected void hasCompletedProcessing() throws IOException {
    if (completedProcessing) {
      throw new IOException("Please check if you are invoking moveToNext() even after it returned"
          + " false.");
    }
  }
}
