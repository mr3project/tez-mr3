/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.FileChunk;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;

public interface FetchedInputAllocatorOrderedGrouped {

  // TODO TEZ-912 Consolidate this with FetchedInputAllocator.
  public MapOutput reserve(InputAttemptIdentifier srcAttemptIdentifier,
                           long requestedSize,
                           long compressedLength,
                           int fetcherId) throws IOException;

  void closeInMemoryFile(MapOutput mapOutput);

  FileSystem getLocalFileSystem();

  void closeOnDiskFile(FileChunk file);

  void unreserve(long bytes);

  void releaseCommittedMemory(long bytes);
}
