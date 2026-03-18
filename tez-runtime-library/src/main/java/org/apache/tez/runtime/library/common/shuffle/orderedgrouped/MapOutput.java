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
import java.io.OutputStream;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tez.runtime.library.common.shuffle.ShuffleInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FileChunk;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;

public abstract class MapOutput implements ShuffleInput {
  private static final Logger LOG = LoggerFactory.getLogger(MapOutput.class);
  private static AtomicInteger ID = new AtomicInteger(0);
  
  public enum Type {
    WAIT,
    MEMORY,
    DISK,
    DISK_DIRECT
  }

  private final int id;
  private InputAttemptIdentifier attemptIdentifier;
  private IFile.SectionLayout sectionLayout;

  private final boolean primaryMapOutput;
  protected final FetchedInputAllocatorOrderedGrouped callback;

  private MapOutput(InputAttemptIdentifier attemptIdentifier, FetchedInputAllocatorOrderedGrouped callback,
                    boolean primaryMapOutput) {
    this.id = ID.incrementAndGet();
    this.attemptIdentifier = attemptIdentifier;
    this.callback = callback;
    this.primaryMapOutput = primaryMapOutput;
  }

  public static MapOutput createDiskMapOutput(InputAttemptIdentifier attemptIdentifier,
                                              FetchedInputAllocatorOrderedGrouped callback, long size, Configuration conf,
                                              int fetcher, boolean primaryMapOutput,
                                              TezTaskOutputFiles mapOutputFile) throws
      IOException {
    FileSystem fs = FileSystem.getLocal(conf).getRaw();
    Path outputPath = mapOutputFile.getInputFileForWrite(
        attemptIdentifier.getInputIdentifier(), attemptIdentifier.getSpillEventId(), size);
    // Files are not clobbered due to the id being appended to the outputPath in the tmpPath,
    // Otherwise fetches for the same task but from different attempts would clobber each other.
    // tmpOutputPath is always unique because fetcher is unique (obtained from Fetcher.fetcherIdGen),
    // so no additional logic is necessary for speculative fetchers.
    Path tmpOutputPath = outputPath.suffix(String.valueOf(fetcher));
    long offset = 0;

    DiskMapOutput mapOutput = new DiskMapOutput(attemptIdentifier, callback, size, outputPath, offset,
        primaryMapOutput, tmpOutputPath);
    mapOutput.disk = fs.create(tmpOutputPath);

    return mapOutput;
  }

  public static MapOutput createLocalDiskMapOutput(InputAttemptIdentifier attemptIdentifier,
                                                   FetchedInputAllocatorOrderedGrouped callback, Path path,  long offset,
                                                   long size, boolean primaryMapOutput,
                                                   IFile.SectionLayout sectionLayout)  {
    DiskDirectMapOutput mapOutput =
        new DiskDirectMapOutput(attemptIdentifier, callback, size, path, offset, primaryMapOutput);
    mapOutput.setSectionLayout(sectionLayout);
    return mapOutput;
  }

  // may throw OutOfMemoryError
  public static MapOutput createMemoryMapOutput(InputAttemptIdentifier attemptIdentifier,
                                                FetchedInputAllocatorOrderedGrouped callback,
                                                long usedMemoryForMergeManger,
                                                long size,
                                                boolean primaryMapOutput)  {
    return new InMemoryMapOutput(attemptIdentifier, callback, usedMemoryForMergeManger, size, primaryMapOutput);
  }

  public static MapOutput createWaitMapOutput(InputAttemptIdentifier attemptIdentifier) {
    return new WaitMapOutput(attemptIdentifier);
  }

  public boolean isPrimaryMapOutput() {
    return primaryMapOutput;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MapOutput) {
      return id == ((MapOutput)obj).id;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return id;
  }

  public FileChunk getOutputPath() {
    return null;
  }

  public byte[] getMemory() {
    return null;
  }
  
  public OutputStream getDisk() {
    return null;
  }

  public InputAttemptIdentifier getAttemptIdentifier() {
    return this.attemptIdentifier;
  }

  public abstract Type getType();

  public IFile.SectionLayout getSectionLayout() {
    return sectionLayout;
  }

  public void setSectionLayout(IFile.SectionLayout sectionLayout) {
    this.sectionLayout = sectionLayout;
  }

  public long getSize() {
    return -1;
  }

  public long getUsedMemoryForMergeManager() {
    return 0;
  }

  public void commit() throws IOException {
  }
  
  public void abort() {
  }
  
  public String toString() {
    return "MapOutput( AttemptIdentifier: " + attemptIdentifier + ", Type: " + getType() + ")";
  }
  
  public static class MapOutputComparator 
  implements Comparator<MapOutput> {
    public int compare(MapOutput o1, MapOutput o2) {
      if (o1.id == o2.id) { 
        return 0;
      }
      
      if (o1.getSize() < o2.getSize()) {
        return -1;
      } else if (o1.getSize() > o2.getSize()) {
        return 1;
      }
      
      if (o1.id < o2.id) {
        return -1;
      } else {
        return 1;
      }
    }
  }

  private static class DiskDirectMapOutput extends MapOutput {
    private final FileChunk outputPath;
    private DiskDirectMapOutput(InputAttemptIdentifier attemptIdentifier, FetchedInputAllocatorOrderedGrouped callback,
                      long size, Path outputPath, long offset, boolean primaryMapOutput) {
      super(attemptIdentifier, callback, primaryMapOutput);
      this.outputPath = new FileChunk(outputPath, offset, size, true, attemptIdentifier);
    }

    @Override
    public FileChunk getOutputPath() {
      return outputPath;
    }

    @Override
    public long getSize() {
      return outputPath.getLength();
    }

    @Override
    public void commit() throws IOException {
      callback.closeOnDiskFile(outputPath);
    }

    @Override
    public void abort() {
      // nothing to do
    }

    @Override
    public Type getType() {
      return Type.DISK_DIRECT;
    }
  }

  private static class DiskMapOutput extends MapOutput {
    private final Path tmpOutputPath;
    private final FileChunk outputPath;
    private OutputStream disk;
    private DiskMapOutput(InputAttemptIdentifier attemptIdentifier, FetchedInputAllocatorOrderedGrouped callback,
                                long size, Path outputPath, long offset, boolean primaryMapOutput, Path tmpOutputPath) {
      super(attemptIdentifier, callback, primaryMapOutput);

      this.tmpOutputPath = tmpOutputPath;
      this.disk = null;
      this.outputPath = new FileChunk(outputPath, offset, size, false, attemptIdentifier);
    }

    @Override
    public FileChunk getOutputPath() {
      return outputPath;
    }

    @Override
    public OutputStream getDisk() {
      return disk;
    }

    @Override
    public long getSize() {
      return outputPath.getLength();
    }

    @Override
    public void commit() throws IOException {
      callback.getLocalFileSystem().rename(tmpOutputPath, outputPath.getPath());
      callback.closeOnDiskFile(outputPath);
    }

    @Override
    public void abort() {
      try {
        callback.getLocalFileSystem().delete(tmpOutputPath, true);
      } catch (IOException ie) {
        LOG.info("failure to clean up " + tmpOutputPath, ie);
      }
    }

    @Override
    public Type getType() {
      return Type.DISK;
    }
  }

  private static class InMemoryMapOutput extends MapOutput {
    private final byte[] byteArray;
    // if usedMemoryForMergeManger == 0, we can think of InMemoryMapOutput as a special case of DiskMapOutput
    // Invariant: usedMemoryForMergeManger has already been added to MergeManager.usedMemory.
    private final long usedMemoryForMergeManger;
    private InMemoryMapOutput(InputAttemptIdentifier attemptIdentifier,
                              FetchedInputAllocatorOrderedGrouped callback,
                              long usedMemoryForMergeManger,
                              long size, boolean primaryMapOutput) {
      super(attemptIdentifier, callback, primaryMapOutput);
      this.byteArray = new byte[(int)size];
      this.usedMemoryForMergeManger = usedMemoryForMergeManger;
      assert usedMemoryForMergeManger == size || usedMemoryForMergeManger == 0L;
    }

    @Override
    public byte[] getMemory() {
      return byteArray;
    }

    @Override
    public long getSize() {
      return byteArray.length;
    }

    @Override
    public long getUsedMemoryForMergeManager() {
      return usedMemoryForMergeManger;
    }

    @Override
    public void commit() throws IOException {
      callback.closeInMemoryFile(this);
    }

    @Override
    public void abort() {
      callback.unreserve(usedMemoryForMergeManger);
    }

    @Override
    public Type getType() {
      return Type.MEMORY;
    }
  }

  private static class WaitMapOutput extends MapOutput {
    private WaitMapOutput(InputAttemptIdentifier attemptIdentifier) {
      super(attemptIdentifier, null, false);
    }

    @Override
    public void commit() throws IOException {
      throw new IOException("Cannot commit MapOutput of type WAIT!");
    }

    @Override
    public void abort() {
      throw new IllegalArgumentException("Cannot commit MapOutput of type WAIT!");
    }

    @Override
    public Type getType() {
      return Type.WAIT;
    }
  }
}
