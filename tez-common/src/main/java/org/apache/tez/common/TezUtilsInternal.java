/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.common;

import org.apache.hadoop.classification.InterfaceAudience.Private;

import java.util.BitSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Private
public class TezUtilsInternal {

  private static final Pattern pattern = Pattern.compile("\\W");
  @Private
  public static final int MAX_VERTEX_NAME_LENGTH = 40;

  @Private
  public static String cleanVertexName(String vertexName) {
    return sanitizeString(vertexName).substring(0,
        vertexName.length() > MAX_VERTEX_NAME_LENGTH ? MAX_VERTEX_NAME_LENGTH : vertexName.length());
  }

  private static String sanitizeString(String srcString) {
    Matcher matcher = pattern.matcher(srcString);
    String res = matcher.replaceAll("_");
    return res; // Number starts allowed rightnow
  }

  public static BitSet fromByteArray(byte[] bytes) {
    if (bytes == null) {
      return new BitSet();
    }
    BitSet bits = new BitSet();
    for (int i = 0; i < bytes.length * 8; i++) {
      if ((bytes[(bytes.length) - (i / 8) - 1] & (1 << (i % 8))) > 0) {
        bits.set(i);
      }
    }
    return bits;
  }

  public static byte[] toByteArray(BitSet bits) {
    if (bits == null) {
      return null;
    }
    byte[] bytes = new byte[(bits.length() + 7) / 8];
    for (int i = 0; i < bits.length(); i++) {
      if (bits.get(i)) {
        bytes[(bytes.length) - (i / 8) - 1] |= 1 << (i % 8);
      }
    }
    return bytes;
  }

}
