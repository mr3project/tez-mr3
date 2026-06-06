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
package org.apache.tez.runtime.library.common.sort.impl;

import java.io.IOException;


/**
 * <code>TezRawKeyValueIterator</code> is an iterator used to iterate over
 * the raw keys and values during sort/merge of intermediate data.
 */
public interface TezRawKeyValueIterator {

  int NO_MORE_KEY_VALUE = 0;
  int NEXT_KEY_VALUE_VOLATILE = 1;
  int NEXT_KEY_VALUE_STABLE = 2;

  // Invariant for current merge-based implementations:
  //   For any record loaded with next(),
  //    - getKey() and getValue() describe data from the same current record source/segment.
  //    - A record is not assembled from a key from one segment and a value from another segment.
  //
  // This provenance invariant does not by itself imply that the returned backing arrays are stable/immutable.
  // Segment/source-specific stability must be reported separately.

  /**
   * Gets the current raw key.
   *
   * @return Gets the current raw key as a TezRawDataBuffer
   * @throws IOException
   */
  TezRawDataBuffer getKey() throws IOException;

  /**
   * Gets the current raw value.
   *
   * @return Gets the current raw value as a TezRawDataBuffer
   * @throws IOException
   */
  TezRawDataBuffer getValue() throws IOException;

  /**
   * Sets up the current key and value (for getKey and getValue).
   *
   * The returned stability code describes backing-array stability for both the
   * key and the value of the current record. A stable result means the byte[]
   * slices returned through getKey() and getValue() may be
   * retained by callers without being overwritten or reused by this iterator. A
   * volatile result means the current record exists, but the backing arrays must
   * not be retained without copying.
   *
   * For current merge-based implementations, for any record loaded with this
   * method, getKey() and getValue() describe data from the
   * same current record source/segment. A record is not assembled from a key
   * from one segment and a value from another segment.
   *
   * @return NO_MORE_KEY_VALUE if no key/value remains,
   *         NEXT_KEY_VALUE_VOLATILE if a key/value exists but its backing arrays are not stable, or
   *         NEXT_KEY_VALUE_STABLE if a key/value exists and both key and value backing arrays are stable.
   */
  int next() throws IOException;

  /**
   * Returns true if any items are left in the iterator.
   *
   * @return true if a call to next will succeed
   *         false otherwise.
   */
  boolean hasNext() throws IOException;

  /**
   * Closes the iterator so that the underlying streams can be closed.
   *
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * Whether the current key is same as the previous key
   *
   * @return true if key is the same as the previous key
   */
  // false negatives are allowed: two keys are the same, but isSameKey() returns false
  boolean isSameKey();
}
