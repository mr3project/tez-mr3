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
package org.apache.tez.runtime.io.compress;

import java.io.IOException;

/** A reusable, block-oriented Tez compressor independent of Hadoop's compression API. */
public interface Compressor extends AutoCloseable {
  /** Returns the stable algorithm identity used to prevent cross-pool returns. */
  CompressionAlgorithm getAlgorithm();

  /**
   * Returns a safe bound for one complete block without changing compressor state.
   * Negative, overflowing, and unsupported lengths are rejected.
   */
  int maxCompressedLength(int uncompressedLength);

  /**
   * Compresses exactly one independent block. Implementations validate both array ranges, retain
   * no caller arrays, and throw {@link IOException} without partial caller output if capacity is
   * insufficient or compression fails.
   */
  int compress(byte[] input, int inputOffset, int inputLength,
      byte[] output, int outputOffset, int outputCapacity) throws IOException;

  /** Restores new-borrower state while retaining reusable allocations. */
  void reset();

  /** Permanently releases resources. This operation is idempotent and does not return to a pool. */
  @Override
  void close();
}
