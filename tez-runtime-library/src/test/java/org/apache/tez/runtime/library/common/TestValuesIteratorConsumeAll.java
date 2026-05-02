package org.apache.tez.runtime.library.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.library.api.KeyValuesReaderEdge;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.junit.Test;

public class TestValuesIteratorConsumeAll {

  @Test
  public void testStartKeyCalledExactlyOncePerDistinctKeyWithStableKeyObject() throws Exception {
    FakeRawIterator in = new FakeRawIterator(
        new String[] {"k1", "k1", "k2"},
        new String[] {"v1", "v2", "v3"});

    TezCounters counters = new TezCounters();
    TezCounter keyCounter = counters.findCounter("test", "keys");
    TezCounter valueCounter = counters.findCounter("test", "values");
    ValuesIterator iterator = new ValuesIterator(in, keyCounter, valueCounter);

    List<String> startedKeys = new ArrayList<>();
    List<String> consumedValues = new ArrayList<>();

    long totalValues = iterator.consumeAll(new KeyValuesReaderEdge.KeyGroupConsumer() {
      @Override
      public void startKey(BytesWritable key) {
        startedKeys.add(asString(key));
      }

      @Override
      public void consumeValue(BytesWritable value) {
        consumedValues.add(asString(value));
      }

      @Override
      public void endKey() {
      }
    });

    assertEquals(Arrays.asList("k1", "k2"), startedKeys);
    assertEquals(Arrays.asList("v1", "v2", "v3"), consumedValues);
    assertEquals(3L, totalValues);
  }

  @Test
  public void testStartKeyReceivesIndependentKeyInstances() throws Exception {
    FakeRawIterator in = new FakeRawIterator(
        new String[] {"k1", "k1", "k2"},
        new String[] {"v1", "v2", "v3"});

    TezCounters counters = new TezCounters();
    TezCounter keyCounter = counters.findCounter("test", "keys");
    TezCounter valueCounter = counters.findCounter("test", "values");
    ValuesIterator iterator = new ValuesIterator(in, keyCounter, valueCounter);

    List<BytesWritable> seenKeys = new ArrayList<>();
    iterator.consumeAll(new KeyValuesReaderEdge.KeyGroupConsumer() {
      @Override
      public void startKey(BytesWritable key) {
        seenKeys.add(key);
      }

      @Override
      public void consumeValue(BytesWritable value) {
      }

      @Override
      public void endKey() {
      }
    });

    assertEquals(2, seenKeys.size());
    assertNotSame(seenKeys.get(0), seenKeys.get(1));
    assertEquals("k1", asString(seenKeys.get(0)));
    assertEquals("k2", asString(seenKeys.get(1)));
  }

  private static String asString(BytesWritable writable) {
    return new String(writable.getBytesRaw(), writable.getOffset(), writable.getLength(), StandardCharsets.UTF_8);
  }

  private static final class FakeRawIterator implements TezRawKeyValueIterator {
    private final byte[][] keys;
    private final byte[][] values;
    private int index = -1;
    private boolean sameKey;

    private final DataInputBuffer keyBuffer = new DataInputBuffer();
    private final DataInputBuffer valueBuffer = new DataInputBuffer();

    FakeRawIterator(String[] keys, String[] values) {
      this.keys = new byte[keys.length][];
      this.values = new byte[values.length][];
      for (int i = 0; i < keys.length; i++) {
        this.keys[i] = keys[i].getBytes(StandardCharsets.UTF_8);
      }
      for (int i = 0; i < values.length; i++) {
        this.values[i] = values[i].getBytes(StandardCharsets.UTF_8);
      }
    }

    @Override
    public DataInputBuffer getKey() {
      return keyBuffer;
    }

    @Override
    public DataInputBuffer getValue() {
      return valueBuffer;
    }

    @Override
    public boolean next() {
      if (index + 1 >= keys.length) {
        return false;
      }
      index++;
      keyBuffer.reset(keys[index], 0, keys[index].length);
      valueBuffer.reset(values[index], 0, values[index].length);
      sameKey = index > 0 && Arrays.equals(keys[index - 1], keys[index]);
      return true;
    }

    @Override
    public boolean hasNext() {
      return index + 1 < keys.length;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public boolean isSameKey() {
      return sameKey;
    }
  }
}
