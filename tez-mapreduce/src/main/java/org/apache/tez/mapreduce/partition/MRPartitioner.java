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

package org.apache.tez.mapreduce.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.common.ConfigUtils;

/**
 * Provides an implementation of {@link Partitioner} that is compatible
 * with Map Reduce partitioners. 
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class MRPartitioner implements org.apache.tez.runtime.library.api.Partitioner {

  static final Logger LOG = LoggerFactory.getLogger(MRPartitioner.class);

  private final boolean useNewApi;

  private final org.apache.hadoop.mapreduce.Partitioner newPartitioner;
  private final org.apache.hadoop.mapred.Partitioner oldPartitioner;

  public MRPartitioner(Configuration conf) {
    this.useNewApi = ConfigUtils.useNewApi(conf);
    int partitions = conf.getInt(TezRuntimeFrameworkConfigs.TEZ_RUNTIME_NUM_EXPECTED_PARTITIONS, 1);

    if (useNewApi) {
      oldPartitioner = null;
      if (partitions > 1) {
        Class<? extends org.apache.hadoop.mapreduce.Partitioner<?, ?>> clazz =
            (Class<? extends org.apache.hadoop.mapreduce.Partitioner<?, ?>>) conf
                .getClass(MRJobConfig.PARTITIONER_CLASS_ATTR,
                    org.apache.hadoop.mapreduce.lib.partition.HashPartitioner.class);
        LOG.info("Using newApi, MRpartitionerClass=" + clazz.getName());
        newPartitioner = (org.apache.hadoop.mapreduce.Partitioner) ReflectionUtils
            .newInstance(clazz, conf);
      } else {
        newPartitioner = new org.apache.hadoop.mapreduce.Partitioner() {
          @Override
          public int getPartition(Object key, Object value, int numPartitions) {
            return numPartitions - 1;
          }
        };
      }
    } else {
      newPartitioner = null;
      if (partitions > 1) {
        // "mapred.partitioner.class" is set in DAGUtils.createConfiguration()
        Class<? extends org.apache.hadoop.mapred.Partitioner> clazz =
            (Class<? extends org.apache.hadoop.mapred.Partitioner>) conf.getClass(
                "mapred.partitioner.class", org.apache.hadoop.mapred.lib.HashPartitioner.class);
        LOG.info("Using oldApi, MRpartitionerClass=" + clazz.getName());
        // do not use 'new JobConf(conf)' because Partitioner is stateless in Hive-MR3
        oldPartitioner = (org.apache.hadoop.mapred.Partitioner) ReflectionUtils.newInstance(
            clazz, null);
      } else {
        oldPartitioner = new org.apache.hadoop.mapred.Partitioner() {
          @Override
          public void configure(JobConf job) {
          }

          @Override
          public int getPartition(Object key, Object value, int numPartitions) {
            return numPartitions - 1;
          }
        };
      }
    }
  }

  @Override
  public int getPartition(Object key, Object value, int numPartitions) {
    if (useNewApi) {
      return newPartitioner.getPartition(key, value, numPartitions);
    } else {
      return oldPartitioner.getPartition(key, value, numPartitions);
    }
  }
}
