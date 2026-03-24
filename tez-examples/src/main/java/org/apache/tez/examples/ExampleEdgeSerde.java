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

package org.apache.tez.examples;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.BytesWritable;

/**
 * Utility methods for encoding/decoding edge payloads using {@link BytesWritable}.
 */
public final class ExampleEdgeSerde {

  private static final BytesWritable NULL_SENTINEL = new BytesWritable();

  private ExampleEdgeSerde() {
  }

  public static BytesWritable encodeString(String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    return new BytesWritable(bytes);
  }

  public static String decodeString(BytesWritable value) {
    return new String(value.getBytes(), 0, value.getLength(), StandardCharsets.UTF_8);
  }

  public static BytesWritable encodeInt(int value) {
    byte[] bytes = ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    return new BytesWritable(bytes);
  }

  public static int decodeInt(BytesWritable value) {
    if (value.getLength() != Integer.BYTES) {
      throw new IllegalArgumentException(
          "Expected " + Integer.BYTES + " bytes for int, got " + value.getLength());
    }
    return ByteBuffer.wrap(value.getBytes(), 0, Integer.BYTES).getInt();
  }

  public static BytesWritable nullSentinel() {
    return new BytesWritable(NULL_SENTINEL.getBytes());
  }

  public static boolean isNullSentinel(BytesWritable value) {
    return value.getLength() == 0;
  }

  public static BytesWritable copy(BytesWritable value) {
    return new BytesWritable(value.copyBytes());
  }
}
