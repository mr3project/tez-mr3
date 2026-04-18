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
package org.apache.tez.runtime.library.common.serializer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.tez.runtime.library.common.comparator.TezBytesComparator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Specialized serialization context for key = HiveKey (extending BytesWritable) / value = BytesWritable payloads.
 */
public final class SerializationContext {

  private SerializationContext() {
  }

  public static Class<BytesWritable> getKeyClass() {
    return BytesWritable.class;
  }

  public static Class<BytesWritable> getValueClass() {
    return BytesWritable.class;
  }

  public static TezBytesWritableSerializer getKeySerializer() {
    return new TezBytesWritableSerializer();
  }

  public static TezBytesWritableDeserializer getKeyDeserializer() {
    return new TezBytesWritableDeserializer();
  }

  public static TezBytesWritableSerializer getValueSerializer() {
    return new TezBytesWritableSerializer();
  }

  public static TezBytesWritableDeserializer getValueDeserializer() {
    return new TezBytesWritableDeserializer();
  }

  public static TezBytesComparator getKeyComparator() {
    return new TezBytesComparator();
  }

  public static class TezBytesWritableDeserializer implements Deserializer<BytesWritable> {

    private DataInputBuffer dataIn;

    public TezBytesWritableDeserializer() {
    }

    @Override
    public void open(InputStream in) {
      dataIn = (DataInputBuffer) in;
    }

    @Override
    public BytesWritable deserialize(BytesWritable writable) throws IOException {
      BytesWritable value = writable;
      if (value == null) {
        value = new BytesWritable();
      }

      int pos = dataIn.getPosition();
      int length = dataIn.getLength() - pos;
      value.set(dataIn.getData(), pos, length);

      return value;
    }

    @Override
    public void close() throws IOException {
      dataIn.close();
    }
  }

  public static class TezBytesWritableSerializer implements Serializer<BytesWritable> {

    private OutputStream dataOut;

    @Override
    public void open(OutputStream out) {
      this.dataOut = out;
    }

    @Override
    public void serialize(BytesWritable writable) throws IOException {
      dataOut.write(writable.getBytes(), 0, writable.getLength());
    }

    @Override
    public void close() throws IOException {
      dataOut.close();
    }
  }
}
