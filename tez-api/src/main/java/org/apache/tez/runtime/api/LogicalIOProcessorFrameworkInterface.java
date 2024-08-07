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

package org.apache.tez.runtime.api;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Public;

/**
 * Represents a processor framework interface which consumes {@link LogicalInput}s and produces
 * {@link LogicalOutput}s.
 * Users are expected to implements {@link AbstractLogicalIOProcessor}
 */
@Public
public interface LogicalIOProcessorFrameworkInterface extends ProcessorFrameworkInterface {

  /**
   * Runs the {@link Processor}
   * 
   * @param inputs
   *          a map of the source vertex name to {@link LogicalInput} - one per
   *          incoming edge.
   * @param outputs
   *          a map of the destination vertex name to {@link LogicalOutput} -
   *          one per outgoing edge
   * @return true (limit, # of records), or null if unnecessary
   * @throws Exception TODO
   */
  public scala.Tuple2<java.lang.Integer, java.lang.Integer> run(Map<String, LogicalInput> inputs,
      Map<String, LogicalOutput> outputs) throws Exception;

}
