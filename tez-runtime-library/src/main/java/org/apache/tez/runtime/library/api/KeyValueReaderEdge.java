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
import java.util.function.BiConsumer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.runtime.api.ReaderEdge;

public abstract class KeyValueReaderEdge extends KeyValueReader implements ReaderEdge {
  // Contract: next() and consumeAll() are mutually exclusive and must not be mixed.

  /**
   * Returns the current key
   * @return the current key
   */
  // Invariant:
  //   The backing byte[] array of BytesWritable is immutable, so the consumer may keep pointers to it.
  @Override
  public abstract BytesWritable getCurrentKey() throws IOException;

  /**
   * Returns the current value
   * @return the current value
   *
   * @throws IOException
   */
  // Invariant:
  //   The backing byte[] array of BytesWritable is immutable, so the consumer may keep pointers to it.
  @Override
  public abstract BytesWritable getCurrentValue() throws IOException;

  // Invariant:
  //   The backing byte[] arrays of both BytesWritable arguments are immutable.
  public abstract int consumeAll(BiConsumer<BytesWritable, BytesWritable> consumer) throws IOException;
}
