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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.mapreduce.combine.MRCombiner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

/**
 * This class contains helper methods for frameworks which migrate from MapReduce to Tez, and need
 * to continue to work with existing MapReduce configurations.
 */
@Public
@Evolving
public class MRHelpers {

  /**
   * Translate MapReduce configuration keys to the equivalent Tez keys in the provided
   * configuration. The translation is done in place. </p>
   * This method is meant to be used by frameworks which rely upon existing MapReduce configuration
   * instead of setting up their own.
   *
   * @param conf mr based configuration to be translated to tez
   */
  public static void translateMRConfToTez(Configuration conf) {
    setupMRComponents(conf);
  }

  private static void setupMRComponents(Configuration conf) {
    if (conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINER_CLASS) == null) {
      boolean useNewApi = conf.getBoolean("mapred.mapper.new-api", false);
      if (useNewApi) {
        if (conf.get(MRJobConfig.COMBINE_CLASS_ATTR) != null) {
          conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINER_CLASS, MRCombiner.class.getName());
        }
      } else {
        if (conf.get("mapred.combiner.class") != null) {
          conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINER_CLASS, MRCombiner.class.getName());
        }
      }
    }
  }
}
