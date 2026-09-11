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

package org.apache.tez.runtime.library.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.tez.common.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ConfigUtils {

  public static boolean shouldCompressIntermediateOutput(Configuration conf) {
    return conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, false);
  }

  public static <K> Class<K> getIntermediateInputKeyClass(Configuration conf) {
    Class<K> retv = (Class<K>) conf.getClass(
        TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS,
        null, Object.class);
    return retv;
  }

  public static <V> Class<V> getIntermediateInputValueClass(Configuration conf) {
    Class<V> retv = (Class<V>) conf.getClass(
        TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS,
        null, Object.class);
    return retv;
  }

  public static <V> RawComparator<V> getInputKeySecondaryGroupingComparator(
      Configuration conf) {
    Class<? extends RawComparator> theClass = conf.getClass(
        TezRuntimeConfiguration.TEZ_RUNTIME_KEY_SECONDARY_COMPARATOR_CLASS,
        null, RawComparator.class);
    if (theClass != null) {
      return ReflectionUtils.newInstance(theClass, conf);
    }
    return getIntermediateInputKeyComparator(conf);
  }

  private static <K> RawComparator<K> getIntermediateInputKeyComparator(Configuration conf) {
    Class<? extends RawComparator> theClass = conf.getClass(
        TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS,
        null, RawComparator.class);
    if (theClass != null)
      return ReflectionUtils.newInstance(theClass, conf);
    return WritableComparator.get(getIntermediateInputKeyClass(conf).asSubclass(
      WritableComparable.class), conf);
  }

  public static boolean useNewApi(Configuration conf) {
    return conf.getBoolean("mapred.mapper.new-api", false);
  }

  public static void addConfigMapToConfiguration(Configuration conf, Map<String, String> confMap) {
    Preconditions.checkArgument(conf != null, "Configuration cannot be null");
    Preconditions.checkArgument(confMap != null, "Configuration map cannot be null");
    for (Map.Entry<String, String> entry : confMap.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
  }

  public static Map<String, String> extractConfigurationMap(Map<String, String> confMap, Set<String> allowedKeys) {
    Preconditions.checkArgument(confMap != null, "ConfMap cannot be null");
    Preconditions.checkArgument(allowedKeys != null, "Valid key set cannot be empty");
    Map<String, String> map = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : confMap.entrySet()) {
      if (allowedKeys.contains(entry.getKey())) {
        map.put(entry.getKey(), entry.getValue());
      }
    }
    return map;
  }

  public static Map<String, String> extractConfigurationMap(Configuration conf,
                                                            Set<String> validKeySet1) {
    Map<String, String> localConfMap = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : conf) {
      if (validKeySet1.contains(entry.getKey())) {
        localConfMap.put(entry.getKey(), entry.getValue());
      }
    }
    return localConfMap;
  }

  public static boolean doesKeyQualify(String key, Set<String> validKeySet) {
    Preconditions.checkArgument(key != null, "key cannot be null");
    Preconditions.checkArgument(validKeySet != null, "Valid key set cannot be empty");
    if (validKeySet.contains(key)) {
      return true;
    }
    return false;
  }

  public static void mergeConfsWithExclusions(Configuration destConf, Map<String, String> srcConf, Set<String> excludedKeySet) {
    Preconditions.checkState(destConf != null, "Destination conf cannot be null");
    Preconditions.checkState(srcConf != null, "Source conf cannot be null");
    for (Map.Entry<String, String> entry : srcConf.entrySet()) {
      if (!excludedKeySet.contains(entry.getKey())) {
        destConf.set(entry.getKey(), entry.getValue());
      }
    }
  }
}
