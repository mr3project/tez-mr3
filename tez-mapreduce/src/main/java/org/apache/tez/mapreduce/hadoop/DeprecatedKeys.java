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

package org.apache.tez.mapreduce.hadoop;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

public class DeprecatedKeys {

  // This could be done via deprecation.
  /**
   * Keys used by the DAG - mainly the AM. 
   */
  private static Map<String, String> mrParamToDAGParamMap = new HashMap<String, String>();

  /**
   * Keys used by the Tez Runtime.
   */
  private static Map<String, String> mrParamToTezRuntimeParamMap =
      new HashMap<String, String>();

  static {
    populateMRToTezRuntimeParamMap();
  }

  public static void init() {
  }

  // Rule: do not include keys that are explicitly set in tez-site.xml of MR3
  // Rule: do not include keys that are no used
  private static void populateMRToTezRuntimeParamMap() {

    registerMRToRuntimeKeyTranslation(MRConfig.MAPRED_IFILE_READAHEAD, TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);

    registerMRToRuntimeKeyTranslation(MRConfig.MAPRED_IFILE_READAHEAD_BYTES, TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);

    registerMRToRuntimeKeyTranslation(MRJobConfig.IO_SORT_FACTOR, TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.MAP_SORT_SPILL_PERCENT, TezRuntimeConfiguration.TEZ_RUNTIME_SORT_SPILL_PERCENT);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.IO_SORT_MB, TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.INDEX_CACHE_MEMORY_LIMIT, TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.MAP_COMBINE_MIN_SPILLS, TezRuntimeConfiguration.TEZ_RUNTIME_COMBINE_MIN_SPILLS);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.REDUCE_INPUT_BUFFER_PERCENT, TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT);
    
    registerMRToRuntimeKeyTranslation("map.sort.class", TezRuntimeConfiguration.TEZ_RUNTIME_INTERNAL_SORTER_CLASS);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.GROUP_COMPARATOR_CLASS, TezRuntimeConfiguration.TEZ_RUNTIME_GROUP_COMPARATOR_CLASS);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.GROUP_COMPARATOR_CLASS, TezRuntimeConfiguration.TEZ_RUNTIME_KEY_SECONDARY_COMPARATOR_CLASS);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.KEY_COMPARATOR, TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS);

    registerMRToRuntimeKeyTranslation(MRJobConfig.MAP_OUTPUT_KEY_CLASS, TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS);

    registerMRToRuntimeKeyTranslation(MRJobConfig.MAP_OUTPUT_VALUE_CLASS, TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS);

    registerMRToRuntimeKeyTranslation(MRJobConfig.MAP_OUTPUT_COMPRESS, TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS);

    registerMRToRuntimeKeyTranslation(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
  }
  
  private static void registerMRToRuntimeKeyTranslation(String mrKey, String tezKey) {
    mrParamToTezRuntimeParamMap.put(mrKey, tezKey);
  }
  
  public static Map<String, String> getMRToDAGParamMap() {
    return Collections.unmodifiableMap(mrParamToDAGParamMap);
  }

  public static Map<String, String> getMRToTezRuntimeParamMap() {
    return Collections.unmodifiableMap(mrParamToTezRuntimeParamMap);
  }
}
