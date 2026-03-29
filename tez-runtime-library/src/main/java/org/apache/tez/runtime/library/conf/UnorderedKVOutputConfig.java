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

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.apache.tez.common.Preconditions;
import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;

/**
 * Configure {@link org.apache.tez.runtime.library.output.UnorderedKVOutput} </p>
 *
 * Values will be picked up from tez-site if not specified, otherwise defaults from
 * {@link org.apache.tez.runtime.library.api.TezRuntimeConfiguration} will be used.
 */
public class UnorderedKVOutputConfig {

  Configuration conf;

  private UnorderedKVOutputConfig(Configuration conf) {
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

  public static Builder newBuilder(String keyClass, String valClass) {
    return new Builder(keyClass, valClass);
  }

  public static class Builder {

    private final Configuration conf = new Configuration(false);

    /**
     * Create a configuration builder for {@link org.apache.tez.runtime.library.output.UnorderedKVOutput}
     *
     * @param keyClassName         the key class name
     * @param valueClassName       the value class name
     */
    Builder(String keyClassName, String valueClassName) {
      this();
      Objects.requireNonNull(keyClassName, "Key class name cannot be null");
      Objects.requireNonNull(valueClassName, "Value class name cannot be null");
    }

    Builder() {
      Map<String, String> tezDefaults = ConfigUtils.extractConfigurationMap(
          TezRuntimeConfiguration.getTezRuntimeConfigDefaults(),
          UnorderedKVOutput.getConfigurationKeySet());
      ConfigUtils.addConfigMapToConfiguration(this.conf, tezDefaults);
      ConfigUtils.addConfigMapToConfiguration(this.conf, TezRuntimeConfiguration.getTezSiteXmlOtherConfigDefaults());
    }

    @SuppressWarnings("unchecked")
    public Builder setAdditionalConfiguration(String key, String value) {
      Objects.requireNonNull(key, "Key cannot be null");
      if (ConfigUtils.doesKeyQualify(key, UnorderedKVOutput.getConfigurationKeySet())) {
        if (value == null) {
          this.conf.unset(key);
        } else {
          this.conf.set(key, value);
        }
      }
      return this;
    }

    @SuppressWarnings("unchecked")
    public Builder setFromConfiguration(Configuration conf) {
      // Maybe ensure this is the first call ? Otherwise this can end up overriding other parameters
      Preconditions.checkArgument(conf != null, "Configuration cannot be null");
      Map<String, String> map = ConfigUtils.extractConfigurationMap(conf,
          UnorderedKVOutput.getConfigurationKeySet());
      ConfigUtils.addConfigMapToConfiguration(this.conf, map);
      return this;
    }

    /**
     * Create the actual configuration instance.
     *
     * @return an instance of the Configuration
     */
    public UnorderedKVOutputConfig build() {
      return new UnorderedKVOutputConfig(this.conf);
    }
  }
}
