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

package org.apache.tez.runtime.library.api;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.runtime.api.ReaderEdge;

public abstract class KeyValuesReaderEdge extends KeyValuesReader implements ReaderEdge {

  public interface KeyGroupConsumer {
    void startKey(BytesWritable key) throws Exception;
    void consumeValue(BytesWritable value) throws Exception;
    void endKey() throws Exception;
  }

  @FunctionalInterface
  public interface ThrowingConsumer<T> {
    void accept(T t) throws Exception;
  }

  /**
   * Returns the current key
   * @return the current key
   */
  // Invariant:
  //   The backing byte[] array of BytesWritable is immutable, so the consumer may keep pointers to it.
  @Override
  public abstract BytesWritable getCurrentKey() throws IOException;
  
  /**
   * Returns an Iterable view of the values associated with the current key
   * @return an Iterable view of the values associated with the current key
   */
  // Invariant:
  //   The backing byte[] array of BytesWritable is immutable, so the consumer may keep pointers to it.
  @Override
  public abstract Iterable<BytesWritable> getCurrentValues() throws IOException;


  /**
   * Consume all values for the current key only.
   *
   * Implementations may override for optimized paths.
   *
   * @return number of consumed values for the current key
   */
  public long consumeCurrentValuesOnly(ThrowingConsumer<BytesWritable> consumer) throws Exception {
    throw new UnsupportedOperationException(
        "consumeCurrentValuesOnly() should not be called in " + getClass().getName());
  }

  public long consumeAll(KeyGroupConsumer consumer) throws Exception {
    throw new UnsupportedOperationException(
        "consumeAll(KeyGroupConsumer) is not supported by " + getClass().getName());
  }
}
