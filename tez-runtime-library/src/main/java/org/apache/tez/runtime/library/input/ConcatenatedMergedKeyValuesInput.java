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

package org.apache.tez.runtime.library.input;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.dag.api.GroupInputEdge;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.MergedLogicalInput;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.MergedInputContext;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.api.KeyValuesReaderEdge;
import org.apache.tez.runtime.library.api.LogicalInputEdge;

/**
 * Implements a {@link MergedLogicalInput} that merges the incoming inputs
 * (e.g. from a {@link GroupInputEdge} and provide a unified view of the 
 * input. It concatenates all the inputs to provide a unified view
 */

public class ConcatenatedMergedKeyValuesInput extends MergedLogicalInput implements LogicalInputEdge {

  public ConcatenatedMergedKeyValuesInput(MergedInputContext context,
                                          List<Input> inputs) {
    super(context, inputs);
  }

  public class ConcatenatedMergedKeyValuesReader extends KeyValuesReaderEdge {
    private int currentReaderIndex = 0;
    private KeyValuesReaderEdge currentReader;

    @Override
    public boolean next() throws IOException {
      while ((currentReader == null) || !currentReader.next()) {
        if (currentReaderIndex == getInputs().size()) {
          hasCompletedProcessing();
          completedProcessing = true;
          return false;
        }
        try {
          Reader reader = getInputs().get(currentReaderIndex).getReader();
          if (!(reader instanceof KeyValuesReaderEdge)) {
            throw new TezUncheckedException("Expected KeyValuesReader. "
                + "Got: " + reader.getClass().getName());
          }
          currentReader = (KeyValuesReaderEdge) reader;
          currentReaderIndex++;
        } catch (Exception e) {
          // An InterruptedException is not expected here since this works off of
          // underlying readers which take care of throwing IOInterruptedExceptions
          if (e instanceof IOException) {
            throw (IOException)e;
          } else {
            throw new IOException(e);
          }
        }
      }
      return true;
    }

    @Override
    public BytesWritable getCurrentKey() throws IOException {
      return currentReader.getCurrentKey();
    }

    @Override
    public Iterable<BytesWritable> getCurrentValues() throws IOException {
      return currentReader.getCurrentValues();
    }

    public float getProgress() throws IOException, InterruptedException {
      return (1.0f)*(currentReaderIndex + 1)/getInputs().size();
    }
  }
   
  /**
   * Provides a {@link KeyValuesReader} that iterates over the 
   * concatenated input data
   */
  @Override
  public KeyValuesReaderEdge getReader() throws Exception {
    return new ConcatenatedMergedKeyValuesReader();
  }

  @Override
  public void setConstituentInputIsReady(Input input) {
    informInputReady();
  }
}
