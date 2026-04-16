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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.tez.runtime.api.DecompressorPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.PriorityQueue;
import org.apache.hadoop.util.Progressable;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.MultiByteArrayOutputStream;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Reader;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Reader.KeyState;
import org.apache.tez.runtime.library.common.sort.impl.IFile.WriterDataInputBuffer;
import org.apache.tez.runtime.library.utils.BufferUtils;

/**
 * Merger is an utility class used by the Map and Reduce tasks for merging
 * both their memory and disk segments
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TezMerger {
  private static final Logger LOG = LoggerFactory.getLogger(TezMerger.class);

  // Local directories
  private static LocalDirAllocator lDirAlloc = 
    new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);

  public static <K extends Object, V extends Object>
  TezRawKeyValueIterator merge(Configuration conf, FileSystem fs,
      CompressionCodec codec,
      List<Segment> segments,
      int mergeFactor, int inMemSegments, Path tmpDir,
      RawComparator comparator, Progressable reporter,
      boolean sortSegments,
      TezCounter readsCounter,
      TezCounter writesCounter,
      TezCounter bytesReadCounter,
      boolean checkForSameKeys,
      DecompressorPool inputContext)
      throws IOException, InterruptedException {
    return new MergeQueue(conf, fs, segments, comparator, reporter,
        sortSegments, codec, checkForSameKeys).merge(mergeFactor, inMemSegments, tmpDir,
        readsCounter, writesCounter, bytesReadCounter, inputContext);
  }

  public static void writeFile(TezRawKeyValueIterator records, IFile.WriterAppendDataInputBuffer writer,
      Progressable progressable, long recordsBeforeProgress)
      throws IOException, InterruptedException {
    writeFile(records, writer, progressable, recordsBeforeProgress, false);
  }

  public static void writeFile(TezRawKeyValueIterator records, IFile.WriterAppendDataInputBuffer writer,
      Progressable progressable, long recordsBeforeProgress, boolean compositeFetch)
      throws IOException, InterruptedException {
    writeFile(records, writer, progressable, recordsBeforeProgress, compositeFetch, false);
  }

  public static void writeFile(TezRawKeyValueIterator records, IFile.WriterAppendDataInputBuffer writer,
      Progressable progressable, long recordsBeforeProgress, boolean compositeFetch,
      boolean forceLegacyNoRleEncoding)
      throws IOException, InterruptedException {
    boolean isRleEnabled = writer.isRleEnabled();

    long recordCtr = 0;
    if (isRleEnabled) {
      while (records.next()) {
        // Even if records.isSameKey() is false, the two keys may be the same.
        DataInputBuffer key = records.isSameKey() ? IFile.REPEAT_KEY : records.getKey();
        writer.appendRle(key, records.getValue());
        if (((recordCtr++) % recordsBeforeProgress) == 0) { checkProgress(progressable); }
      }
    } else {
      while (records.next()) {
        if (compositeFetch && !forceLegacyNoRleEncoding) {
          writer.appendNoRleTez(records.getKey(), records.getValue());
        } else {
          writer.appendNoRle(records.getKey(), records.getValue());
        }
        if (((recordCtr++) % recordsBeforeProgress) == 0) { checkProgress(progressable); }
      }
    }
  }

  private static void checkProgress(Progressable progressable) throws InterruptedException {
    progressable.progress();
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException("Current thread=" + Thread.currentThread().getName() + " interrupted");
    }
  }

  static class KeyValueBuffer {
    private byte[] buf;
    private int position;
    private int length;

    public KeyValueBuffer(byte buf[], int position, int length) {
      reset(buf, position, length);
    }

    public void reset(byte[] input, int position, int length) {
      this.buf = input;
      this.position = position;
      this.length = length;
    }

    public byte[] getData() {
      return buf;
    }

    public int getPosition() {
      return position;
    }

    public int getLength() {
      return length;
    }
  }

  public static class Segment {
    static final byte[] EMPTY_BYTES = new byte[0];
    IFile.KeyValueReaderDataInputBuffer reader = null;
    final KeyValueBuffer key = new KeyValueBuffer(EMPTY_BYTES, 0, 0);
    TezCounter mapOutputsCounter = null;

    public Segment(IFile.KeyValueReaderDataInputBuffer reader, TezCounter mapOutputsCounter) {
      this.reader = reader;
      this.mapOutputsCounter = mapOutputsCounter;
    }

    void init(TezCounter readsCounter, TezCounter bytesReadCounter) throws IOException {
      if (mapOutputsCounter != null) {
        mapOutputsCounter.increment(1);
      }
    }

    boolean inMemory() {
      return true;
    }

    KeyValueBuffer getKey() { return key; }

    DataInputBuffer getValue(DataInputBuffer value) throws IOException {
      nextRawValue(value);
      return value;
    }

    public long getLength() {
      return reader.getLength();
    }

    KeyState readRawKey(DataInputBuffer nextKey) throws IOException {
      KeyState keyState = reader.readRawKey(nextKey);
      key.reset(nextKey.getData(), nextKey.getPosition(), nextKey.getLength() - nextKey.getPosition());
      return keyState;
    }

    boolean nextRawKey(DataInputBuffer nextKey) throws IOException {
      boolean hasNext = reader.readRawKey(nextKey) != KeyState.NO_KEY;
      key.reset(nextKey.getData(), nextKey.getPosition(), nextKey.getLength() - nextKey.getPosition());
      return hasNext;
    }

    void nextRawValue(DataInputBuffer value) throws IOException {
      reader.nextRawValue(value);
    }

    void closeReader() throws IOException {
      if (reader != null) {
        reader.close();
        reader = null;
      }
    }

    void close() throws IOException {
      closeReader();
    }
  }

  public static class DiskSegment extends Segment {

    FileSystem fs = null;
    Path file = null;
    boolean preserve = false;   // Signifies whether the segment should be kept after a merge is complete. Checked in the close method.
    CompressionCodec codec = null;
    long segmentOffset = 0;
    long segmentLength = -1;
    boolean ifileReadAhead;
    int ifileReadAheadLength;

    final DecompressorPool inputContext;

    public DiskSegment(FileSystem fs, Path file,
        long segmentOffset, long segmentLength, CompressionCodec codec,
        boolean ifileReadAhead, int ifileReadAheadLength,
        boolean preserve, TezCounter mergedMapOutputsCounter, DecompressorPool inputContext) {
      super(null, mergedMapOutputsCounter);
      this.fs = fs;
      this.file = file;
      this.codec = codec;
      this.preserve = preserve;
      this.ifileReadAhead = ifileReadAhead;
      this.ifileReadAheadLength = ifileReadAheadLength;

      this.segmentOffset = segmentOffset;
      this.segmentLength = segmentLength;

      this.inputContext = inputContext;
    }

    @Override
    void init(TezCounter readsCounter, TezCounter bytesReadCounter) throws IOException {
      super.init(readsCounter, bytesReadCounter);
      FSDataInputStream in = fs.open(file);
      in.seek(segmentOffset);
      reader = new Reader(in, segmentLength, codec, readsCounter, bytesReadCounter, ifileReadAhead,
          ifileReadAheadLength, inputContext);
    }

    @Override
    boolean inMemory() {
      return false;
    }

    @Override
    public long getLength() {
      return (reader == null) ?
        segmentLength : reader.getLength();
    }

    @Override
    void close() throws IOException {
      super.close();
      if (!preserve && fs != null) {
        fs.delete(file, false);
      }
    }
  }

  public static final class IntermediateMemorySegment extends Segment {
    private final MultiByteArrayOutputStream byteArrayOutput;
    private final boolean cleanupOnClose;

    IntermediateMemorySegment(IFile.KeyValueReaderDataInputBuffer reader,
                              MultiByteArrayOutputStream byteArrayOutput, boolean cleanupOnClose) {
      super(reader, null);
      this.byteArrayOutput = byteArrayOutput;
      this.cleanupOnClose = cleanupOnClose;
    }

    @Override
    void close() throws IOException {
      try {
        super.close();
      } finally {
        if (cleanupOnClose && byteArrayOutput != null) {
          byteArrayOutput.clean();
        }
      }
    }
  }

  static class MergeQueue<K extends Object, V extends Object>
  extends PriorityQueue<Segment> implements TezRawKeyValueIterator {
    final Configuration conf;
    final FileSystem fs;
    final CompressionCodec codec;
    final boolean checkForSameKeys;
    static final boolean ifileReadAhead = TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT;
    static final int ifileReadAheadLength = TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT;
    static final long recordsBeforeProgress = TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT;

    // Invariant: Segment.close() is called for all Segment objects
    List<Segment> segments = new ArrayList<Segment>();
    
    final RawComparator comparator;

    final Progressable reporter;
    
    final DataInputBuffer key = new DataInputBuffer();
    final DataInputBuffer value = new DataInputBuffer();
    final DataInputBuffer nextKey = new DataInputBuffer();
    final DataInputBuffer diskIFileValue = new DataInputBuffer();
    
    Segment minSegment;
    Comparator<Segment> segmentComparator =   
      new Comparator<Segment>() {
      public int compare(Segment o1, Segment o2) {
        if (o1.getLength() == o2.getLength()) {
          return 0;
        }

        return o1.getLength() < o2.getLength() ? -1 : 1;
      }
    };

    KeyState hasNext;
    DataOutputBuffer prevKey = new DataOutputBuffer();

    public MergeQueue(Configuration conf, FileSystem fs,
        List<Segment> segments, RawComparator comparator,
        Progressable reporter, boolean sortSegments, CompressionCodec codec,
        boolean checkForSameKeys) {
      this.conf = conf;
      this.fs = fs;
      this.comparator = comparator;
      this.segments = segments;
      this.reporter = reporter;
      if (sortSegments) {
        Collections.sort(segments, segmentComparator);
      }
      this.checkForSameKeys = checkForSameKeys;
      this.codec = codec;
    }

    public void close() throws IOException {
      Segment segment;
      while((segment = pop()) != null) {
        segment.close();
      }
    }

    public DataInputBuffer getKey() throws IOException {
      return key;
    }

    public DataInputBuffer getValue() throws IOException {
      return value;
    }

    private void populatePreviousKey() throws IOException {
      key.reset();
      BufferUtils.copy(key, prevKey);
    }

    private void adjustPriorityQueue(Segment reader) throws IOException{
      if (checkForSameKeys) {
        if (hasNext == null) {
          /**
           * hasNext can be null during first iteration & prevKey is initialized here.
           * In cases of NO_KEY/NEW_KEY, we readjust the queue later. If new segment/file is found
           * during this process, we need to compare keys for RLE across segment boundaries.
           * prevKey can't be empty at that time (e.g custom comparators)
           */
          populatePreviousKey();
        } else {
          // indicates a key has been read already
          if (hasNext != KeyState.SAME_KEY) {
            /**
             * Store previous key before reading next for later key comparisons.
             * If all keys in a segment are unique, it would always hit this code path and key copies
             * are wasteful in such condition, as these comparisons are mainly done for RLE.
             * TODO: When better stats are available, this condition can be avoided.
             */
            populatePreviousKey();
          }
        }
      }
      hasNext = reader.readRawKey(nextKey);
      if (hasNext == KeyState.NEW_KEY) {
        adjustTop();
        compareKeyWithNextTopKey(reader);
      } else if(hasNext == KeyState.NO_KEY) {
        pop();
        reader.close();
        compareKeyWithNextTopKey(null);
      } else if(hasNext == KeyState.SAME_KEY) {
        // do not rebalance the priority queue
      }
    }

    /**
     * Check if the previous key is same as the next top segment's key.
     * This would be useful to compute whether same key is spread across multiple segments.
     *
     * @param current
     * @throws IOException
     */
    void compareKeyWithNextTopKey(Segment current) throws IOException {
      Segment nextTop = top();
      if (checkForSameKeys && nextTop != current) {
        // we have a different file. Compare it with previous key
        KeyValueBuffer nextKey = nextTop.getKey();
        int compare = compare(nextKey, prevKey);
        if (compare == 0) {
          // Same key is available in the next segment.
          hasNext = KeyState.SAME_KEY;
        }
      }
    }

    public boolean next() throws IOException {
      if (!hasNext()) {
        return false;
      }

      minSegment = top();
      KeyValueBuffer nextKey = minSegment.getKey();
      key.reset(nextKey.getData(), nextKey.getPosition(), nextKey.getLength());
      if (!minSegment.inMemory()) {
        // When we load the value from an inmemory segment, we reset
        // the "value" DIB in this class to the inmem segment's byte[].
        // When we load the value bytes from disk, we shouldn't use
        // the same byte[] since it would corrupt the data in the inmem segment.
        // So we maintain an explicit DIB for value bytes obtained from disk,
        // and if the current segment is a disk segment,
        // we reset the "value" DIB to the byte[] in that (so we reuse the disk segment DIB
        // whenever we consider a disk segment).
        minSegment.getValue(diskIFileValue);
        value.reset(diskIFileValue.getData(), diskIFileValue.getLength());
      } else {
        minSegment.getValue(value);
      }

      return true;
    }

    int compare(KeyValueBuffer nextKey, DataOutputBuffer buf2) {
      byte[] b1 = nextKey.getData();
      byte[] b2 = buf2.getData();
      int s1 = nextKey.getPosition();
      int s2 = 0;
      int l1 = nextKey.getLength();
      int l2 = buf2.getLength();
      return comparator.compare(b1, s1, l1, b2, s2, l2);
    }

    protected boolean lessThan(Object a, Object b) {
      KeyValueBuffer key1 = ((Segment)a).getKey();
      KeyValueBuffer key2 = ((Segment)b).getKey();
      int s1 = key1.getPosition();
      int l1 = key1.getLength();
      int s2 = key2.getPosition();
      int l2 = key2.getLength();;

      return comparator.compare(key1.getData(), s1, l1, key2.getData(), s2, l2) < 0;
    }
    
    TezRawKeyValueIterator merge(int factor, int inMem, Path tmpDir,
                                     TezCounter readsCounter,
                                     TezCounter writesCounter,
                                     TezCounter bytesReadCounter,
                                     DecompressorPool inputContext)
        throws IOException, InterruptedException {
      if (segments.size() == 0) {
        LOG.info("Nothing to merge. Returning an empty iterator");
        return new EmptyIterator();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Merging " + segments.size() + " sorted segments");
      }

      /*
       * If there are inMemory segments, then they come first in the segments
       * list and then the sorted disk segments. Otherwise(if there are only
       * disk segments), then they are sorted segments if there are more than
       * factor segments in the segments list.
       */
      int numSegments = segments.size();
      int origFactor = factor;
      int passNo = 1;
      
      // create the MergeStreams from the sorted map created in the constructor
      // and dump the final output to a file
      byte[] writeBuffer = IFile.allocateWriteBuffer();
      do {
        // get the factor for this pass of merge. We assume in-memory segments are
        // the first entries in the segment list and that the pass factor doesn't apply to them
        factor = getPassFactor(factor, passNo, numSegments - inMem);
        if (1 == passNo) {
          factor += inMem;
        }
        List<Segment> segmentsToMerge =
          new ArrayList<Segment>();
        int segmentsConsidered = 0;
        int numSegmentsToConsider = factor;
        while (true) {
          // extract the smallest 'factor' number of segments
          // Call cleanup on the empty segments (no key/value data)
          List<Segment> mStream = 
            getSegmentDescriptors(numSegmentsToConsider);
          for (Segment segment : mStream) {
            // Initialize the segment at the last possible moment;
            // this helps in ensuring we don't use buffers until we need them

            segment.init(readsCounter, bytesReadCounter);
            boolean hasNext = segment.nextRawKey(nextKey);
            
            if (hasNext) {
              segmentsToMerge.add(segment);
              segmentsConsidered++;
            }
            else { // Empty segments. Can this be avoided altogether ?
              segment.close();
              numSegments--; //we ignore this segment for the merge
            }
          }
          // if we have the desired number of segments or looked at all available segments, we break
          if (segmentsConsidered == factor || 
              segments.size() == 0) {
            break;
          }

          // Get the correct # of segments in case some of them were empty.
          numSegmentsToConsider = factor - segmentsConsidered;
        }
        
        // feed the streams to the priority queue
        initialize(segmentsToMerge.size());
        clear();
        for (Segment segment : segmentsToMerge) {
          put(segment);
        }
        
        // if we have lesser number of segments remaining, then just return the iterator,
        // else do another single level merge
        if (numSegments <= factor) { // Will always kick in if only in-mem segments are provided.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Down to the last merge-pass, with " + numSegments +
                " segments left");
          }
          // At this point, Factor Segments have not been physically
          // materialized. The merge will be done dynamically. Some of them may
          // be in-memory segments, other on-disk semgnets. Decision to be made
          // by a finalMerge is that is required.
          return this;
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Merging " + segmentsToMerge.size() +
                " intermediate segments out of a total of " +
                (segments.size() + segmentsToMerge.size()));
          }
          
          // we want to spread the creation of temp files on multiple disks if available under the space constraints
          long approxOutputSize = 0; 
          for (Segment s : segmentsToMerge) {
            approxOutputSize += s.getLength() + (long)ChecksumFileSystem.getApproxChkSumLength(s.getLength());
          }
          Path tmpFilename = new Path(tmpDir, "intermediate").suffix("." + passNo);

          Path outputFile =  lDirAlloc.getLocalPathForWrite(
                                              tmpFilename.toString(),
                                              approxOutputSize, conf);

          boolean useFreeMemoryWriterOutput = conf.getBoolean(
              TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT,
              TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT_DEFAULT);
          long freeMemoryThreshold = 1024L * 1024L * conf.getInt(
              TezRuntimeConfiguration.TEZ_RUNTIME_FREE_MEMORY_WRITER_OUTPUT_THRESHOLD_MB,
              TezRuntimeConfiguration.TEZ_RUNTIME_FREE_MEMORY_WRITER_OUTPUT_THRESHOLD_MB_DEFAULT);
          boolean writeIntermediateToMemory = useFreeMemoryWriterOutput
              && MultiByteArrayOutputStream.canUseFreeMemoryBuffers(freeMemoryThreshold);

          MultiByteArrayOutputStream byteArrayOutput = null;
          IFile.WriterAppendDataInputBuffer writer;
          if (writeIntermediateToMemory) {
            byteArrayOutput = new MultiByteArrayOutputStream(fs, outputFile);
            FSDataOutputStream outputStream = new FSDataOutputStream(byteArrayOutput, null);
            writer = new WriterDataInputBuffer(outputStream, codec, writesCounter, null,
                checkForSameKeys, -1, -1, writeBuffer, null);
          } else {
            writer = new WriterDataInputBuffer(fs, outputFile, codec, writesCounter, null,
                checkForSameKeys, -1, -1, writeBuffer);
          }

          writeFile(this, writer, reporter, recordsBeforeProgress);
          writer.close();
          
          // we finished one single level merge; now clean up the priority queue
          this.close();

          // Add the newly create segment to the list of segments to be merged
          Segment tempSegment;
          if (byteArrayOutput == null) {
            tempSegment = new DiskSegment(fs, outputFile, 0, fs.getFileStatus(outputFile).getLen(), codec,
                ifileReadAhead, ifileReadAheadLength, false, null, inputContext);
          } else {
            IFile.KeyValueReaderDataInputBuffer reader = new Reader(byteArrayOutput.createInputStream(), byteArrayOutput.getTotalBytes(),
                codec, null, null, ifileReadAhead, ifileReadAheadLength, inputContext);
            tempSegment = new IntermediateMemorySegment(reader, byteArrayOutput, true);
          }

          // Insert new merged segment into the sorted list
          int pos = Collections.binarySearch(segments, tempSegment, segmentComparator);
          if (pos < 0) {
            // binary search failed. So position to be inserted at is -pos-1
            pos = -pos-1;
          }
          segments.add(pos, tempSegment);
          numSegments = segments.size();

          passNo++;
        }
        // we are worried about only the first pass merge factor. So reset the factor to what it originally was
        factor = origFactor;
      } while(true);
    }
    
    /**
     * Determine the number of segments to merge in a given pass. Assuming more
     * than factor segments, the first pass should attempt to bring the total
     * number of segments - 1 to be divisible by the factor - 1 (each pass
     * takes X segments and produces 1) to minimize the number of merges.
     */
    private static int getPassFactor(int factor, int passNo, int numSegments) {
      // passNo > 1 in the OR list - is that correct ?
      if (passNo > 1 || numSegments <= factor || factor == 1) 
        return factor;
      int mod = (numSegments - 1) % (factor - 1);
      if (mod == 0)
        return factor;
      return mod + 1;
    }
    
    /** Return (& remove) the requested number of segment descriptors from the
     * sorted map.
     */
    private List<Segment> getSegmentDescriptors(int numDescriptors) {
      if (numDescriptors > segments.size()) {
        List<Segment> subList = new ArrayList<Segment>(segments);
        segments.clear();
        return subList;
      }

      // Efficiently bulk remove segments
      List<Segment> subList = segments.subList(0, numDescriptors);
      List<Segment> subListCopy = new ArrayList<>(subList);
      subList.clear();
      return subListCopy;
    }
    
    @Override
    public boolean isSameKey() {
      return (hasNext != null) && (hasNext == KeyState.SAME_KEY);
    }

    public boolean hasNext() throws IOException {
      if (size() == 0)
        return false;

      if (minSegment != null) {
        // minSegment is non-null for all invocations of next except the first one.
        // For the first invocation, the priority queue is ready for use
        // but for the subsequent invocations, first adjust the queue.
        adjustPriorityQueue(minSegment);
        if (size() == 0) {
          minSegment = null;
          return false;
        }
      }

      return true;
    }

  }

  private static class EmptyIterator implements TezRawKeyValueIterator {
    @Override
    public DataInputBuffer getKey() throws IOException {
      throw new RuntimeException("No keys on an empty iterator");
    }

    @Override
    public DataInputBuffer getValue() throws IOException {
      throw new RuntimeException("No values on an empty iterator");
    }

    @Override
    public boolean next() throws IOException {
      return false;
    }

    @Override
    public boolean hasNext() throws IOException {
      return false;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public boolean isSameKey() {
      throw new UnsupportedOperationException("isSameKey is not supported");
    }
  }
}
