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

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.Constants;

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
  
  private static void populateMRToTezRuntimeParamMap() {

    registerMRToRuntimeKeyTranslation(MRConfig.MAPRED_IFILE_READAHEAD, TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);

    registerMRToRuntimeKeyTranslation(MRConfig.MAPRED_IFILE_READAHEAD_BYTES, TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);

    registerMRToRuntimeKeyTranslation(MRJobConfig.RECORDS_BEFORE_PROGRESS, TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS);

    registerMRToRuntimeKeyTranslation(MRJobConfig.IO_SORT_FACTOR, TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.MAP_SORT_SPILL_PERCENT, TezRuntimeConfiguration.TEZ_RUNTIME_SORT_SPILL_PERCENT);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.IO_SORT_MB, TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.INDEX_CACHE_MEMORY_LIMIT, TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.MAP_COMBINE_MIN_SPILLS, TezRuntimeConfiguration.TEZ_RUNTIME_COMBINE_MIN_SPILLS);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION);

    registerMRToRuntimeKeyTranslation(MRJobConfig.SHUFFLE_PARALLEL_COPIES, TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.SHUFFLE_FETCH_FAILURES, TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES_LIMIT);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.SHUFFLE_CONNECT_TIMEOUT, TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.SHUFFLE_READ_TIMEOUT, TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT);
    
    registerMRToRuntimeKeyTranslation(MRConfig.SHUFFLE_SSL_ENABLED_KEY, TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT, TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.SHUFFLE_MEMORY_LIMIT_PERCENT, TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.SHUFFLE_MERGE_PERCENT, TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.REDUCE_MEMTOMEM_THRESHOLD, TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMTOMEM_SEGMENTS);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.REDUCE_MEMTOMEM_ENABLED, TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.REDUCE_INPUT_BUFFER_PERCENT, TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT);
    
    registerMRToRuntimeKeyTranslation("map.sort.class", TezRuntimeConfiguration.TEZ_RUNTIME_INTERNAL_SORTER_CLASS);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.GROUP_COMPARATOR_CLASS, TezRuntimeConfiguration.TEZ_RUNTIME_GROUP_COMPARATOR_CLASS);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.GROUP_COMPARATOR_CLASS, TezRuntimeConfiguration.TEZ_RUNTIME_KEY_SECONDARY_COMPARATOR_CLASS);
    
    registerMRToRuntimeKeyTranslation(MRJobConfig.KEY_COMPARATOR, TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS);

    registerMRToRuntimeKeyTranslation(MRJobConfig.MAP_OUTPUT_KEY_CLASS, TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS);

    registerMRToRuntimeKeyTranslation(MRJobConfig.MAP_OUTPUT_VALUE_CLASS, TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS);

    registerMRToRuntimeKeyTranslation(MRJobConfig.MAP_OUTPUT_COMPRESS, TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS);

    registerMRToRuntimeKeyTranslation(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);

    registerMRToRuntimeKeyTranslation(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, TezConfiguration.TEZ_USER_CLASSPATH_FIRST);
  }
  
  private static void registerMRToRuntimeKeyTranslation(String mrKey,
      String tezKey) {
    mrParamToTezRuntimeParamMap.put(mrKey, tezKey);
  }
  
  public static Map<String, String> getMRToDAGParamMap() {
    return Collections.unmodifiableMap(mrParamToDAGParamMap);
  }

  public static Map<String, String> getMRToTezRuntimeParamMap() {
    return Collections.unmodifiableMap(mrParamToTezRuntimeParamMap);
  }
}
