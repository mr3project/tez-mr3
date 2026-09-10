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

/** A reusable, block-oriented Tez decompressor independent of Hadoop's compression API. */
public interface Decompressor extends AutoCloseable {
  /** Returns the stable algorithm identity used to prevent cross-pool returns. */
  CompressionAlgorithm getAlgorithm();

  /**
   * Decompresses exactly one complete provider-defined unit. The caller supplies valid array
   * ranges and enough output capacity. Implementations retain no caller arrays, reject invalid
   * input, and throw {@link IOException} without partial caller output if decompression fails.
   */
  int decompress(byte[] input, int inputOffset, int inputLength,
      byte[] output, int outputOffset, int outputCapacity) throws IOException;

  /** Restores new-borrower state while retaining reusable allocations. */
  void reset();

  /** Permanently releases resources. This operation is idempotent and does not return to a pool. */
  @Override
  void close();
}
