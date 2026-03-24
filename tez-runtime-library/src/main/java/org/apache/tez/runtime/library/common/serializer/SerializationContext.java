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
import org.apache.tez.runtime.library.common.comparator.TezBytesComparator;
import org.apache.tez.runtime.library.common.serializer.TezBytesWritableSerialization.TezBytesWritableDeserializer;
import org.apache.tez.runtime.library.common.serializer.TezBytesWritableSerialization.TezBytesWritableSerializer;

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
}