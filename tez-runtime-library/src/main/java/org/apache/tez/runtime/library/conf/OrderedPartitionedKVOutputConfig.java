/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.tez.runtime.library.conf;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.apache.tez.common.Preconditions;
import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;

/**
 * Configure {@link org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput} </p>
 * 
 * Values will be picked up from tez-site if not specified, otherwise defaults from
 * {@link org.apache.tez.runtime.library.api.TezRuntimeConfiguration} will be used.
 */
public class OrderedPartitionedKVOutputConfig {

  /**
   * Currently supported sorter implementations
   */
  public enum SorterImpl {
    /** Pipeline sorter - a more efficient sorter that supports > 2 GB sort buffers */
    PIPELINED
  }

  Configuration conf;

  OrderedPartitionedKVOutputConfig() {
  }

  private OrderedPartitionedKVOutputConfig(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Get a UserPayload representation of the Configuration
   * @return a {@link org.apache.tez.dag.api.UserPayload} instance
   */
  public UserPayload toUserPayload() {
    try {
      return TezUtils.createUserPayloadFromConf(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void fromUserPayload(UserPayload payload) {
    try {
      this.conf = TezUtils.createConfFromUserPayload(payload);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  String toHistoryText() {
    return null;
  }

  public static Builder newBuilder(String keyClass, String valueClass, String partitionerClassName) {
    return newBuilder(keyClass, valueClass, partitionerClassName, null);
  }

  public static Builder newBuilder(String keyClass, String valueClass, String partitionerClassName,
                                   Map<String, String> partitionerConf) {
    return new Builder(keyClass, valueClass, partitionerClassName, partitionerConf);
  }

  public static class Builder {

    private final Configuration conf = new Configuration(false);

    /**
     * Create a configuration builder for {@link org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput}
     *
     * @param keyClassName         the key class name
     * @param valueClassName       the value class name
     * @param partitionerClassName the partitioner class name
     * @param partitionerConf      the partitioner configuration. This can be null, and is a {@link
     *                             java.util.Map} of key-value pairs. The keys should be limited to
     *                             the ones required by the partitioner.
     */
    Builder(String keyClassName, String valueClassName, String partitionerClassName,
                   @Nullable Map<String, String> partitionerConf) {
      this();
      Objects.requireNonNull(keyClassName, "Key class name cannot be null");
      Objects.requireNonNull(valueClassName, "Value class name cannot be null");
      Objects.requireNonNull(partitionerClassName, "Partitioner class name cannot be null");
      setKeyClassName(keyClassName);
      setValueClassName(valueClassName);
      setPartitioner(partitionerClassName, partitionerConf);
    }

    Builder() {
      Map<String, String> tezDefaults = ConfigUtils
          .extractConfigurationMap(TezRuntimeConfiguration.getTezRuntimeConfigDefaults(),
              OrderedPartitionedKVOutput.getConfigurationKeySet());
      ConfigUtils.addConfigMapToConfiguration(this.conf, tezDefaults);
      ConfigUtils.addConfigMapToConfiguration(this.conf, TezRuntimeConfiguration.getOtherConfigDefaults());
    }

    Builder setKeyClassName(String keyClassName) {
      Objects.requireNonNull(keyClassName, "Key class name cannot be null");
      this.conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, keyClassName);
      return this;
    }

    Builder setValueClassName(String valueClassName) {
      Objects.requireNonNull(valueClassName, "Value class name cannot be null");
      this.conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, valueClassName);
      return this;
    }

    Builder setPartitioner(String partitionerClassName, @Nullable Map<String, String> partitionerConf) {
      Objects.requireNonNull(partitionerClassName, "Partitioner class name cannot be null");
      this.conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, partitionerClassName);
      if (partitionerConf != null) {
        // Merging the confs for now. Change to be specific in the future.
        ConfigUtils.mergeConfsWithExclusions(this.conf, partitionerConf,
            TezRuntimeConfiguration.getRuntimeConfigKeySet());
      }
      return this;
    }

    public Builder setSortBufferSize(int sortBufferSize) {
      this.conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, sortBufferSize);
      return this;
    }

    public Builder setCombiner(String combinerClassName) {
      return this.setCombiner(combinerClassName, null);
    }

    public Builder setCombiner(String combinerClassName, Map<String, String> combinerConf) {
      this.conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINER_CLASS, combinerClassName);
      if (combinerConf != null) {
        // Merging the confs for now. Change to be specific in the future.
        ConfigUtils.mergeConfsWithExclusions(this.conf, combinerConf,
            TezRuntimeConfiguration.getRuntimeConfigKeySet());
      }
      return this;
    }

    public Builder setSorterNumThreads(int numThreads) {
      this.conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_SORT_THREADS, numThreads);
      return this;
    }

    public Builder setSorter(SorterImpl sorterImpl) {
      Objects.requireNonNull(sorterImpl, "Sorter cannot be null");
      this.conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS,
          sorterImpl.name());
      return this;
    }


    @SuppressWarnings("unchecked")
    public Builder setAdditionalConfiguration(String key, String value) {
      Objects.requireNonNull(key, "Key cannot be null");
      if (ConfigUtils.doesKeyQualify(key,
          Lists.newArrayList(OrderedPartitionedKVOutput.getConfigurationKeySet(),
              TezRuntimeConfiguration.getRuntimeAdditionalConfigKeySet()),
          TezRuntimeConfiguration.getAllowedPrefixes())) {
        if (value == null) {
          this.conf.unset(key);
        } else {
          this.conf.set(key, value);
        }
      }
      return this;
    }

    @SuppressWarnings("unchecked")
    public Builder setAdditionalConfiguration(Map<String, String> confMap) {
      Objects.requireNonNull(confMap, "ConfMap cannot be null");
      Map<String, String> map = ConfigUtils.extractConfigurationMap(confMap,
          Lists.newArrayList(OrderedPartitionedKVOutput.getConfigurationKeySet(),
              TezRuntimeConfiguration.getRuntimeAdditionalConfigKeySet()), TezRuntimeConfiguration.getAllowedPrefixes());
      ConfigUtils.addConfigMapToConfiguration(this.conf, map);
      return this;
    }

    @SuppressWarnings("unchecked")
    public Builder setFromConfiguration(Configuration conf) {
      // Maybe ensure this is the first call ? Otherwise this can end up overriding other parameters
      Preconditions.checkArgument(conf != null, "Configuration cannot be null");
      Map<String, String> map = ConfigUtils.extractConfigurationMap(conf,
          OrderedPartitionedKVOutput.getConfigurationKeySet(),
          TezRuntimeConfiguration.getRuntimeAdditionalConfigKeySet(), TezRuntimeConfiguration.getAllowedPrefixes());
      ConfigUtils.addConfigMapToConfiguration(this.conf, map);
      return this;
    }

    /**
     * Set the key comparator class
     *
     * @param comparatorClassName the key comparator class name
     * @return instance of the current builder
     */
    public Builder setKeyComparatorClass(String comparatorClassName) {
      return this.setKeyComparatorClass(comparatorClassName, null);
    }

    /**
     * Set the key comparator class and it's associated configuration. This method should only be
     * used if the comparator requires some specific configuration, which is typically not the
     * case. {@link #setKeyComparatorClass(String)} is the preferred method for setting a
     * comparator.
     *
     * @param comparatorClassName the key comparator class name
     * @param comparatorConf      the comparator configuration. This can be null, and is a {@link
     *                            java.util.Map} of key-value pairs. The keys should be limited to
     *                            the ones required by the comparator.
     * @return instance of the current builder
     */
    public Builder setKeyComparatorClass(String comparatorClassName,
                                         @Nullable Map<String, String> comparatorConf) {
      Objects.requireNonNull(comparatorClassName, "Comparator class name cannot be null");
      this.conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS,
          comparatorClassName);
      if (comparatorConf != null) {
        // Merging the confs for now. Change to be specific in the future.
        ConfigUtils.mergeConfsWithExclusions(this.conf, comparatorConf,
            TezRuntimeConfiguration.getRuntimeConfigKeySet());
      }
      return this;
    }

    public Builder setCompression(boolean enabled, @Nullable String compressionCodec,
                                  @Nullable Map<String, String> codecConf) {
      this.conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, enabled);
      if (enabled && compressionCodec != null) {
        this.conf
            .set(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC, compressionCodec);
      }
      if (codecConf != null) {
        // Merging the confs for now. Change to be specific in the future.
        ConfigUtils.mergeConfsWithExclusions(this.conf, codecConf,
            TezRuntimeConfiguration.getRuntimeConfigKeySet());
      }
      return this;
    }

    /**
     * Set serialization class and the relevant comparator to be used for sorting.
     * Providing custom serialization class could change the way, keys needs to be compared in
     * sorting. Providing invalid comparator here could create invalid results.
     *
     * @param serializationClassName
     * @param comparatorClassName
     * @param serializerConf         the serializer configuration. This can be null, and is a
     *                               {@link java.util.Map} of key-value pairs. The keys should be limited
     *                               to the ones required by the comparator.
     * @return this object for further chained method calls
     */
    public Builder setKeySerializationClass(String serializationClassName,
        String comparatorClassName, @Nullable Map<String, String> serializerConf) {
      Preconditions.checkArgument(serializationClassName != null,
          "serializationClassName cannot be null");
      Preconditions.checkArgument(comparatorClassName != null,
          "comparator cannot be null");
      this.conf.set(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY, serializationClassName + ","
          + conf.get(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY));
      setKeyComparatorClass(comparatorClassName, null);
      if (serializerConf != null) {
        // Merging the confs for now. Change to be specific in the future.
        ConfigUtils.mergeConfsWithExclusions(this.conf, serializerConf,
            TezRuntimeConfiguration.getRuntimeConfigKeySet());
      }
      return this;
    }

    /**
     * Set serialization class responsible for providing serializer/deserializer for values.
     *
     * @param serializationClassName
     * @param serializerConf         the serializer configuration. This can be null, and is a
     *                               {@link java.util.Map} of key-value pairs. The keys should be limited
     *                               to the ones required by the comparator.
     * @return this object for further chained method calls
     */
    public Builder setValueSerializationClass(String serializationClassName,
                                              @Nullable Map<String, String> serializerConf) {
      Preconditions.checkArgument(serializationClassName != null,
          "serializationClassName cannot be null");
      this.conf.set(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY, serializationClassName + ","
          + conf.get(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY));
      if (serializerConf != null) {
        // Merging the confs for now. Change to be specific in the future.
        ConfigUtils.mergeConfsWithExclusions(this.conf, serializerConf,
            TezRuntimeConfiguration.getRuntimeConfigKeySet());
      }
      return this;
    }

    /**
     * Create the actual configuration instance.
     *
     * @return an instance of the Configuration
     */
    public OrderedPartitionedKVOutputConfig build() {
      return new OrderedPartitionedKVOutputConfig(this.conf);
    }
  }
}

