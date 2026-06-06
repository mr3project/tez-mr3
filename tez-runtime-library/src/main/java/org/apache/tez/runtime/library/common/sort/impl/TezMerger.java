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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.tez.runtime.api.DecompressorPool;
import org.apache.tez.util.FastByteComparisons;
import org.apache.tez.runtime.api.TaskContext;
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
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.MultiByteArrayOutputStream;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Reader;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Reader.KeyState;
import org.apache.tez.runtime.library.common.sort.impl.IFile.WriterDataInputBuffer;

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

  public static
  TezRawKeyValueIterator merge(Configuration conf, FileSystem fs,
      CompressionCodec codec,
      List<Segment> segments,
      int mergeFactor, int inMemSegments, Path tmpDir,
      boolean sortSegments,
      TezCounter readsCounter,
      TezCounter writesCounter,
      TezCounter bytesReadCounter,
      boolean checkForSameKeys,
      TaskContext taskContext)
      throws IOException, InterruptedException {
    return new MergeQueue(conf, fs, segments, sortSegments, codec, checkForSameKeys).merge(
      mergeFactor, inMemSegments, tmpDir, readsCounter, writesCounter, bytesReadCounter, taskContext);
  }

  public static void writeFile(TezRawKeyValueIterator records, IFile.WriterAppendDataInputBuffer writer,
      long recordsBeforeProgress)
      throws IOException, InterruptedException {
    boolean isRleEnabled = writer.isRleEnabled();

    long recordCtr = 0;
    if (isRleEnabled) {
      int nextResult;
      while ((nextResult = records.next()) != TezRawKeyValueIterator.NO_MORE_KEY_VALUE) {
        // Even if records.isSameKey() is false, the two keys may be the same.
        DataInputBuffer key = records.isSameKey() ? IFile.REPEAT_KEY : records.getKey();
        writer.appendRle(key, records.getValue(),
            nextResult == TezRawKeyValueIterator.NEXT_KEY_VALUE_STABLE);
        if (((recordCtr++) % recordsBeforeProgress) == 0) { checkProgress(); }
      }
    } else {
      while (records.next() != TezRawKeyValueIterator.NO_MORE_KEY_VALUE) {
        writer.appendNoRle(records.getKey(), records.getValue());
        if (((recordCtr++) % recordsBeforeProgress) == 0) { checkProgress(); }
      }
    }
  }

  private static void checkProgress() throws InterruptedException {
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException("Current thread=" + Thread.currentThread().getName() + " interrupted");
    }
  }

  static class KeyValueBuffer {
    private byte[] buf;
    private int position;
    private int length;

    public KeyValueBuffer(byte[] buf, int position, int length) {
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
    IFile.KeyValueReaderDataInputBuffer reader;
    final KeyValueBuffer key = new KeyValueBuffer(EMPTY_BYTES, 0, 0);
    TezCounter mapOutputsCounter;

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

    boolean isCurrentRecordStable() {
      return reader.isCurrentRecordStable();
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

    final TaskContext taskContext;

    public DiskSegment(FileSystem fs, Path file,
        long segmentOffset, long segmentLength, CompressionCodec codec,
        boolean ifileReadAhead, int ifileReadAheadLength,
        boolean preserve, TezCounter mergedMapOutputsCounter, TaskContext taskContext) {
      super(null, mergedMapOutputsCounter);
      this.fs = fs;
      this.file = file;
      this.codec = codec;
      this.preserve = preserve;
      this.ifileReadAhead = ifileReadAhead;
      this.ifileReadAheadLength = ifileReadAheadLength;

      this.segmentOffset = segmentOffset;
      this.segmentLength = segmentLength;

      this.taskContext = taskContext;
    }

    @Override
    void init(TezCounter readsCounter, TezCounter bytesReadCounter) throws IOException {
      super.init(readsCounter, bytesReadCounter);
      FSDataInputStream in = fs.open(file);
      in.seek(segmentOffset);
      reader = new Reader(in, segmentLength, codec, readsCounter, bytesReadCounter, ifileReadAhead,
          ifileReadAheadLength, taskContext);
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

  public static final class InputStreamSegment extends Segment {
    public InputStreamSegment(IFile.KeyValueReaderDataInputBuffer reader, TezCounter mapOutputsCounter) {
      super(reader, mapOutputsCounter);
    }

    @Override
    boolean inMemory() {
      return false;
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

  static class MergeQueue
  implements TezRawKeyValueIterator {
    final Configuration conf;
    final FileSystem fs;
    final CompressionCodec codec;
    final boolean checkForSameKeys;
    private final SegmentLoserTree loserTree = new SegmentLoserTree();
    static final boolean ifileReadAhead = TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT;
    static final int ifileReadAheadLength = TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT;
    static final long recordsBeforeProgress = TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT;

    // Invariant: Segment.close() is called for all Segment objects
    List<Segment> segments = new ArrayList<Segment>();
    
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
    KeyValueBuffer prevKey = new KeyValueBuffer(Segment.EMPTY_BYTES, 0, 0);
    byte[] prevKeyCopy = Segment.EMPTY_BYTES;

    public MergeQueue(Configuration conf, FileSystem fs,
        List<Segment> segments,
        boolean sortSegments, CompressionCodec codec,
        boolean checkForSameKeys) {
      this.conf = conf;
      this.fs = fs;
      this.segments = segments;
      if (sortSegments) {
        Collections.sort(segments, segmentComparator);
      }
      this.checkForSameKeys = checkForSameKeys;
      this.codec = codec;
    }

    public void close() throws IOException {
      Segment segment;
      while((segment = loserTree.pop()) != null) {
        segment.close();
      }
    }

    public DataInputBuffer getKey() throws IOException {
      return key;
    }

    public DataInputBuffer getValue() throws IOException {
      return value;
    }

    private void populatePreviousKey(Segment currentSegment) {
      KeyValueBuffer currentKey = currentSegment.getKey();
      byte[] keyData = currentKey.getData();
      int keyPosition = currentKey.getPosition();
      int keyLength = currentKey.getLength();
      if (currentSegment.isCurrentRecordStable()) {
        prevKey.reset(keyData, keyPosition, keyLength);
      } else {
        if (prevKeyCopy.length < keyLength) {
          prevKeyCopy = new byte[keyLength];
        }
        System.arraycopy(keyData, keyPosition, prevKeyCopy, 0, keyLength);
        prevKey.reset(prevKeyCopy, 0, keyLength);
      }
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
          populatePreviousKey(reader);
        } else {
          // indicates a key has been read already
          if (hasNext != KeyState.SAME_KEY) {
            /**
             * Store previous key before reading next for later key comparisons.
             * If all keys in a segment are unique, it would always hit this code path. For
             * volatile records this still requires a fallback key copy, which is wasteful in
             * such condition, as these comparisons are mainly done for RLE.
             * TODO: When better stats are available, this condition can be avoided.
             */
            populatePreviousKey(reader);
          }
        }
      }
      hasNext = reader.readRawKey(nextKey);
      if (hasNext == KeyState.NEW_KEY) {
        loserTree.updateTop();
        compareKeyWithNextTopKey(reader);
      } else if(hasNext == KeyState.NO_KEY) {
        loserTree.pop();
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
    void compareKeyWithNextTopKey(Segment current) {
      Segment nextTop = loserTree.top();
      if (checkForSameKeys && nextTop != null && nextTop != current) {
        // we have a different file. Compare it with previous key
        KeyValueBuffer nextKey = nextTop.getKey();
        boolean isEqual = compare(nextKey, prevKey);
        if (isEqual) {
          // Same key is available in the next segment.
          hasNext = KeyState.SAME_KEY;
        }
      }
    }

    public int next() throws IOException {
      if (!hasNext()) {
        return TezRawKeyValueIterator.NO_MORE_KEY_VALUE;
      }

      minSegment = loserTree.top();
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

      return minSegment.isCurrentRecordStable()
          ? TezRawKeyValueIterator.NEXT_KEY_VALUE_STABLE
          : TezRawKeyValueIterator.NEXT_KEY_VALUE_VOLATILE;
    }

    boolean compare(KeyValueBuffer key1, KeyValueBuffer key2) {
      return FastByteComparisons.compareEqual(
          key1.getData(), key1.getPosition(), key1.getLength(),
          key2.getData(), key2.getPosition(), key2.getLength());
    }

    /*
     * MergeQueue advances the top Segment and then replays that leaf through
     * the loser tree. Unlike a binary heap repair, each replay level compares
     * the changed Segment only with the loser stored at that level.
     */
    private static final class SegmentLoserTree {
      private static final int NO_SEGMENT = -1;

      private Segment[] leaves = new Segment[0];
      private int[] losers = new int[0];
      private int leafCapacity;
      private int leafCount;
      private int size;
      private int winner = NO_SEGMENT;

      void initialize(int capacity) {
        clear();
        int requiredCapacity = nextPowerOfTwo(Math.max(1, capacity));
        if (leafCapacity < requiredCapacity) {
          leafCapacity = requiredCapacity;
          leaves = new Segment[leafCapacity];
          losers = new int[leafCapacity];
        }
      }

      void clear() {
        Arrays.fill(leaves, null);
        Arrays.fill(losers, NO_SEGMENT);
        leafCount = 0;
        size = 0;
        winner = NO_SEGMENT;
      }

      int size() {
        return size;
      }

      Segment top() {
        return winner == NO_SEGMENT ? null : leaves[winner];
      }

      Segment pop() {
        if (winner == NO_SEGMENT) {
          return null;
        }

        int winnerIndex = winner;
        Segment result = leaves[winnerIndex];
        leaves[winnerIndex] = null;
        size--;
        replay(winnerIndex);
        return result;
      }

      void put(Segment segment) {
        leaves[leafCount++] = segment;
        size++;
      }

      void build() {
        Arrays.fill(losers, NO_SEGMENT);
        winner = build(1);
      }

      Segment updateTop() {
        if (winner == NO_SEGMENT) {
          return null;
        }

        replay(winner);
        return top();
      }

      private int build(int node) {
        if (node >= leafCapacity) {
          int leafIndex = node - leafCapacity;
          return leafIndex < leafCount && leaves[leafIndex] != null ? leafIndex : NO_SEGMENT;
        }

        int leftWinner = build(node << 1);
        int rightWinner = build((node << 1) + 1);
        return storeLoserAndReturnWinner(node, leftWinner, rightWinner);
      }

      private void replay(int leafIndex) {
        int candidate = leaves[leafIndex] == null ? NO_SEGMENT : leafIndex;
        int node = (leafIndex + leafCapacity) >>> 1;
        while (node > 0) {
          candidate = storeLoserAndReturnWinner(node, candidate, losers[node]);
          node >>>= 1;
        }
        winner = candidate;
      }

      private int storeLoserAndReturnWinner(int node, int left, int right) {
        if (left == NO_SEGMENT) {
          losers[node] = NO_SEGMENT;
          return right;
        }
        if (right == NO_SEGMENT) {
          losers[node] = NO_SEGMENT;
          return left;
        }
        if (wins(left, right)) {
          losers[node] = right;
          return left;
        }
        losers[node] = left;
        return right;
      }

      private boolean wins(int left, int right) {
        KeyValueBuffer key1 = leaves[left].getKey();
        KeyValueBuffer key2 = leaves[right].getKey();
        int comparison = FastByteComparisons.compareTo(
            key1.getData(), key1.getPosition(), key1.getLength(),
            key2.getData(), key2.getPosition(), key2.getLength());
        return comparison < 0 || (comparison == 0 && left < right);
      }

      private static int nextPowerOfTwo(int value) {
        int highestOneBit = Integer.highestOneBit(value);
        return value == highestOneBit ? value : highestOneBit << 1;
      }
    }

    TezRawKeyValueIterator merge(int factor, int inMem, Path tmpDir,
                                 TezCounter readsCounter,
                                 TezCounter writesCounter,
                                 TezCounter bytesReadCounter,
                                 TaskContext taskContext)
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
        
        // feed the streams to the loser tree
        loserTree.initialize(segmentsToMerge.size());
        for (Segment segment : segmentsToMerge) {
          loserTree.put(segment);
        }
        loserTree.build();
        
        // if we have lesser number of segments remaining, then just return the iterator,
        // else do another single level merge
        if (numSegments <= factor) { // Will always kick in if only in-mem segments are provided.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Down to the last merge-pass, with " + numSegments +
                " segments left");
          }
          // At this point, Factor Segments have not been physically materialized.
          // The merge will be done dynamically. Some of them may be in-memory segments, other on-disk semgnets.
          // Decision to be made by a finalMerge is that is required.
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

          Path outputFile = lDirAlloc.getLocalPathForWrite(tmpFilename.toString(), approxOutputSize, conf);

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
                false, checkForSameKeys, -1, -1, writeBuffer, null, taskContext);
          } else {
            writer = new WriterDataInputBuffer(fs, outputFile, codec, writesCounter, null,
                checkForSameKeys, writeBuffer, taskContext);
          }

          writeFile(this, writer, recordsBeforeProgress);
          writer.close();
          
          // we finished one single level merge; now clean up the loser tree
          this.close();

          // Add the newly create segment to the list of segments to be merged
          Segment tempSegment;
          if (byteArrayOutput == null) {
            tempSegment = new DiskSegment(fs, outputFile, 0, fs.getFileStatus(outputFile).getLen(), codec,
                ifileReadAhead, ifileReadAheadLength, false, null, taskContext);
          } else {
            IFile.KeyValueReaderDataInputBuffer reader = new Reader(
                byteArrayOutput.createInputStreamFrom(0, byteArrayOutput.getTotalBytes()),
                byteArrayOutput.getTotalBytes(),
                codec, null, null, ifileReadAhead, ifileReadAheadLength, taskContext);
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
      if (loserTree.size() == 0)
        return false;

      if (minSegment != null) {
        // minSegment is non-null for all invocations of next except the first one.
        // For the first invocation, the loser tree is ready for use
        // but for the subsequent invocations, first adjust the queue.
        adjustPriorityQueue(minSegment);
        if (loserTree.size() == 0) {
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
    public int next() throws IOException {
      return TezRawKeyValueIterator.NO_MORE_KEY_VALUE;
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
