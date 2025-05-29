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

import java.io.IOException;

import org.apache.hadoop.fs.Path;

/**
 * Manipulate the working area for the transient store for components in tez-runtime-library
 *
 * This class is used by Inputs and Outputs in tez-runtime-library to identify the directories
 * that they need to write to / read from for intermediate files.
 */
public interface TezTaskOutput {

  /**
   * Create a local output file name.
   *
   * @param size the size of the file
   * @return path the path to write to
   * @throws IOException
   */
  public Path getOutputFileForWrite(long size) throws IOException;

  /**
   * Create a local output file name. This method is meant to be used *only* if
   * the size of the file is not know up front.
   * 
   * @return path the path to write to
   * @throws IOException
   */
  public Path getOutputFileForWrite() throws IOException;

  /**
   * Create a local output file name on the same volume.
   * This is only meant to be used to rename temporary files to their final destination within the
   * same volume.
   *
   * @return path the path of the output file within the same volume
   */
  public Path getOutputFileForWriteInVolume(Path existing);

  /**
   * Create a local output index file name.
   *
   * @param size the size of the file
   * @return path the path to write the index file to
   * @throws IOException
   */
  public Path getOutputIndexFileForWrite(long size) throws IOException;

  /**
   * Create a local output index file name on the same volume.
   * The intended usage of this method is to write the index file on the same volume as the
   * associated data file.
   * @return path the path of the index file within the same volume
   */
  public Path getOutputIndexFileForWriteInVolume(Path existing);

  /**
   * Create a local output spill file name.
   *
   * @param spillNumber the spill number
   * @param size the size of the file
   * @return path the path to write the spill file for the specific spillNumber
   * @throws IOException
   */
  public Path getSpillFileForWrite(int spillNumber, long size)
      throws IOException;


  /**
   * Create a local output spill index file name.
   *
   * @param spillNumber the spill number
   * @param size the size of the spill file
   * @return path the path to write the spill index file for the specific spillNumber
   * @throws IOException
   */
  public Path getSpillIndexFileForWrite(int spillNumber, long size)
      throws IOException;

  /**
   * Create a local input file name.
   *
   * @param srcIdentifier The identifier for the source
   * @param spillNum
   * @param size the size of the file  @return path the path to the input file.
   * @throws IOException
   */
  public Path getInputFileForWrite(int srcIdentifier,
      int spillNum, long size) throws IOException;

  /**
   * Construct a spill file name, given a spill number
   *
   * @param srcId
   * @param spillNum
   * @return a spill file name independent of the unique identifier and local directories
   */
  public String getSpillFileName(int srcId, int spillNum);

}
