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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.tez.dag.api.GroupInputEdge;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.MergedLogicalInput;
import org.apache.tez.runtime.api.ProgressFailedException;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.MergedInputContext;
import org.apache.tez.runtime.library.api.KeyValueReader;

/**
 * Implements a {@link MergedLogicalInput} that merges the incoming inputs
 * (e.g. from a {@link GroupInputEdge} and provide a unified view of the 
 * input. It concatenates all the inputs to provide a unified view
 */
@Public
public class ConcatenatedMergedKeyValueInput extends MergedLogicalInput {
  private ConcatenatedMergedKeyValueReader concatenatedMergedKeyValueReader;

  public ConcatenatedMergedKeyValueInput(MergedInputContext context,
                                         List<Input> inputs) {
    super(context, inputs);
  }

  public class ConcatenatedMergedKeyValueReader extends KeyValueReader {
    private int currentReaderIndex = 0;
    private KeyValueReader currentReader;

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
          if (!(reader instanceof KeyValueReader)) {
            throw new TezUncheckedException("Expected KeyValueReader. "
                + "Got: " + reader.getClass().getName());
          }
          currentReader = (KeyValueReader) reader;
          currentReaderIndex++;
        } catch (Exception e) {
          // An InterruptedException is not expected here since this works off of
          // underlying readers which take care of throwing IOInterruptedExceptions
          if (e instanceof IOException) {
            throw (IOException) e;
          } else {
            throw new IOException(e);
          }
        }
      }
      return true;
    }

    @Override
    public Object getCurrentKey() throws IOException {
      return currentReader.getCurrentKey();
    }

    @Override
    public Object getCurrentValue() throws IOException {
      return currentReader.getCurrentValue();
    }

    public float getProgress() throws IOException, InterruptedException {
      return (1.0f)*(currentReaderIndex + 1)/getInputs().size();
    }
  }

  /**
   * Provides a {@link KeyValueReader} that iterates over the 
   * concatenated input data
   */
  @Override
  public KeyValueReader getReader() throws Exception {
    concatenatedMergedKeyValueReader = new ConcatenatedMergedKeyValueReader();
    return concatenatedMergedKeyValueReader;
  }

  @Override
  public void setConstituentInputIsReady(Input input) {
    informInputReady();
  }

  @Override
  public float getProgress() throws ProgressFailedException, InterruptedException {
    try {
      return concatenatedMergedKeyValueReader.getProgress();
    } catch (IOException e) {
      throw new ProgressFailedException("getProgress encountered IOException ", e);
    }
  }
}
