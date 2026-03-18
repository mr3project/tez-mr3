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

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.IFile.KeyState;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;

import java.io.IOException;

class RawKVIteratorReader implements IFile.ReaderRead {

  private final TezRawKeyValueIterator kvIter;

  // size and bytesRead refer to the actual payload bytes to be read in readRawKey and nextRawValue.
  // Hence, we have an invariant: bytesRead <= size
  private final long size;
  private long bytesRead = 0;

  public RawKVIteratorReader(TezRawKeyValueIterator kvIter, long size) {
    this.kvIter = kvIter;
    this.size = size;
  }

  @Override
  public KeyState readRawKey(DataInputBuffer key) throws IOException {
    if (kvIter.next()) {
      final DataInputBuffer kb = kvIter.getKey();
      final int kp = kb.getPosition();
      final int klen = kb.getLength() - kp;
      key.reset(kb.getData(), kp, klen);

      // TODO: add 2L * IFile.INT_SIZE for consistency with IFile.Reader
      bytesRead += klen;
      return KeyState.NEW_KEY;
    }
    return KeyState.NO_KEY;
  }

  @Override
  public boolean nextRawKey(DataInputBuffer key) throws IOException {
    return readRawKey(key) != KeyState.NO_KEY;
  }

  @Override
  public void nextRawValue(DataInputBuffer value) throws IOException {
    final DataInputBuffer vb = kvIter.getValue();
    final int vp = vb.getPosition();
    final int vlen = vb.getLength() - vp;
    value.reset(vb.getData(), vp, vlen);
    bytesRead += vlen;
  }

  @Override
  public long getPosition() throws IOException {
    return bytesRead;
  }

  @Override
  public void close() throws IOException {
    kvIter.close();
  }

  @Override
  public long getLength() {
    return size;
  }
}