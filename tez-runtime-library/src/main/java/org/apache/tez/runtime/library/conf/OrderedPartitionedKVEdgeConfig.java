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

package org.apache.tez.runtime.library.conf;

import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;

/**
 * Configure payloads for the OrderedPartitionedKVOutput and OrderedGroupedKVInput pair </p>
 *
 * Values will be picked up from tez-site if not specified, otherwise defaults from
 * {@link org.apache.tez.runtime.library.api.TezRuntimeConfiguration} will be used.
 */
public class OrderedPartitionedKVEdgeConfig extends KeyValuesBasedBaseEdgeConfig {

  private final OrderedPartitionedKVOutputConfig outputConf;
  private final OrderedGroupedKVInputConfig inputConf;

  private OrderedPartitionedKVEdgeConfig(
      OrderedPartitionedKVOutputConfig outputConfiguration,
      OrderedGroupedKVInputConfig inputConfiguration) {
    this.outputConf = outputConfiguration;
    this.inputConf = inputConfiguration;
  }

  /**
   * Create a builder to configure the relevant Input and Output. </p> This method should only be
   * used when using a custom Partitioner which requires specific Configuration. {@link
   * #newBuilder(String, String, String)} is the preferred method to crate an instance of the
   * Builder
   *
   * @param keyClassName         the key class name
   * @param valueClassName       the value class name
   * @param partitionerClassName the partitioner class name
   * @return a builder to configure the edge
   */
  public static Builder newBuilder(String keyClassName, String valueClassName,
                                   String partitionerClassName) {
    return new Builder(keyClassName, valueClassName, partitionerClassName);
  }

  @Override
  public UserPayload getOutputPayload() {
    return outputConf.toUserPayload();
  }

  @Override
  public String getOutputClassName() {
    return OrderedPartitionedKVOutput.class.getName();
  }

  @Override
  public UserPayload getInputPayload() {
    return inputConf.toUserPayload();
  }

  @Override
  public String getInputClassName() {
    return inputConf.getInputClassName();
  }

  /**
   * This is a convenience method for the typical usage of this edge, and creates an instance of
   * {@link org.apache.tez.dag.api.EdgeProperty} which is likely to be used. </p>
   * * In this case - DataMovementType.SCATTER_GATHER
   *
   * @return an {@link org.apache.tez.dag.api.EdgeProperty} instance
   */
  public EdgeProperty createDefaultEdgeProperty() {
    EdgeProperty edgeProperty = EdgeProperty.create(EdgeProperty.DataMovementType.SCATTER_GATHER,
        OutputDescriptor.create(
            getOutputClassName()).setUserPayload(getOutputPayload()),
        InputDescriptor.create(
            getInputClassName()).setUserPayload(getInputPayload()));
    return edgeProperty;
  }

  /**
   * This is a convenience method for creating an Edge descriptor based on the specified
   * EdgeManagerDescriptor.
   *
   * @param edgeManagerDescriptor the custom edge specification
   * @return an {@link org.apache.tez.dag.api.EdgeProperty} instance
   */
  public EdgeProperty createDefaultCustomEdgeProperty(EdgeManagerPluginDescriptor edgeManagerDescriptor) {
    Objects.requireNonNull(edgeManagerDescriptor, "EdgeManagerDescriptor cannot be null");
    EdgeProperty edgeProperty =
        EdgeProperty.create(edgeManagerDescriptor,
            OutputDescriptor.create(getOutputClassName()).setUserPayload(getOutputPayload()),
            InputDescriptor.create(getInputClassName()).setUserPayload(getInputPayload()));
    return edgeProperty;
  }

  public static class Builder implements BaseConfigBuilder<Builder> {

    private final OrderedPartitionedKVOutputConfig.Builder outputBuilder;
    private final OrderedGroupedKVInputConfig.Builder inputBuilder;

    Builder(String keyClassName, String valueClassName, String partitionerClassName) {
      outputBuilder = new OrderedPartitionedKVOutputConfig.Builder(
          keyClassName, valueClassName, partitionerClassName);
      inputBuilder = new OrderedGroupedKVInputConfig.Builder(keyClassName, valueClassName);
    }

    @Override
    public Builder setAdditionalConfiguration(String key, String value) {
      outputBuilder.setAdditionalConfiguration(key, value);
      inputBuilder.setAdditionalConfiguration(key, value);
      return this;
    }

    @Override
    /**
     * Edge config options are derived from client-side tez-site.xml (recommended).
     * Optionally invoke setFromConfiguration to override these config options via commandline arguments.
     *
     * @param conf
     * @return this object for further chained method calls
     */
    public Builder setFromConfiguration(Configuration conf) {
      outputBuilder.setFromConfiguration(conf);
      inputBuilder.setFromConfiguration(conf);
      return this;
    }

    /**
     * Build and return an instance of the configuration
     * @return an instance of the acatual configuration
     */
    public OrderedPartitionedKVEdgeConfig build() {
      return new OrderedPartitionedKVEdgeConfig(outputBuilder.build(), inputBuilder.build());
    }
  }
}
