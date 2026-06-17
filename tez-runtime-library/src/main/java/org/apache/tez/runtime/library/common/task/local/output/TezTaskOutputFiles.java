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

package org.apache.tez.runtime.library.common.task.local.output;

import java.io.IOException;

import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.runtime.api.TezTaskOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.tez.runtime.library.common.Constants;

/**
 * Manipulate the working area for the transient store for components in tez-runtime-library
 *
 * This class is used by Inputs and Outputs in tez-runtime-library to identify the directories
 * that they need to write to / read from for intermediate files.
 */
/*
=== tez_shuffle ===
+-----------------------------------------------+---------------------------------------------------------------+
| File kind                                     | Relative local-disk path under ${appDir}                      |
+-----------------------------------------------+---------------------------------------------------------------+
| Final output data                             | dag_<dagId>/<containerId>/vertex_<vertexId>/<uniqueId>/       |
|                                               |   file.out                                                    |
+-----------------------------------------------+---------------------------------------------------------------+
| Final output index                            | dag_<dagId>/<containerId>/vertex_<vertexId>/<uniqueId>/       |
|                                               |   file.out.index                                              |
+-----------------------------------------------+---------------------------------------------------------------+
| Spill data file                               | dag_<dagId>/<containerId>/vertex_<vertexId>/<uniqueId>/       |
|                                               |   spill_<spillNumber>.out                                     |
+-----------------------------------------------+---------------------------------------------------------------+
| Spill index file                              | dag_<dagId>/<containerId>/vertex_<vertexId>/<uniqueId>/       |
|                                               |   spill_<spillNumber>.out.index                               |
+-----------------------------------------------+---------------------------------------------------------------+
| Intermediate merge file created by TezMerger  | dag_<dagId>/<containerId>/vertex_<vertexId>/<uniqueId>/       |
|                                               |   merge_<mergeId>_<passNo>.out                                |
+-----------------------------------------------+---------------------------------------------------------------+
| MergeManager memory-to-disk merge output      | dag_<dagId>/<containerId>/vertex_<vertexId>/<uniqueId>/       |
|                                               |   src_<srcId>_spill_<spillNum>.out.merged                     |
+-----------------------------------------------+---------------------------------------------------------------+
| MergeManager on-disk merge output             | dag_<dagId>/<containerId>/vertex_<vertexId>/<uniqueId>/       |
|                                               |   src_<srcId>_spill_<spillNum>.merged<N>                      |
+-----------------------------------------------+---------------------------------------------------------------+

=== mapreduce_shuffle ===
+-----------------------------------------------+---------------------------------------------------------------+
| File kind                                     | Relative local-disk path under ${appDir}                      |
+-----------------------------------------------+---------------------------------------------------------------+
| Spill data file                               | output/<uniqueId>_<spillNumber>/file.out                      |
+-----------------------------------------------+---------------------------------------------------------------+
| Spill index file                              | output/<uniqueId>_<spillNumber>/file.out.index                |
+-----------------------------------------------+---------------------------------------------------------------+
| Intermediate merge file created by TezMerger  | output/<uniqueId>/merge_<mergeId>_<passNo>.out                |
+-----------------------------------------------+---------------------------------------------------------------+
| MergeManager memory-to-disk merge output      | <uniqueId>_src_<srcId>_spill_<spillNum>.out.merged            |
+-----------------------------------------------+---------------------------------------------------------------+
| MergeManager on-disk merge output             | <uniqueId>_src_<srcId>_spill_<spillNum>.merged<N>             |
+-----------------------------------------------+---------------------------------------------------------------+
 */
public class TezTaskOutputFiles implements TezTaskOutput {

  private static final Logger LOG = LoggerFactory.getLogger(TezTaskOutputFiles.class);

  private static final String SPILL_FILE_SRC_SEPARATOR = "_src_";
  private static final String SPILL_FILE_SPILL_SEPARATOR = "_spill_";
  private static final String SPILL_FILE_EXTENSION = ".out";
  private static final String COMPOSITE_SPILL_FILE_PREFIX = "spill_";
  private static final String SRC_SPILL_FILE_PREFIX = "src_";

  private final Configuration conf;
  private final String uniqueId;
  private final String outputDir;
  private final String dagId;   // = dag_${dagId}/${containerId}/
  private final boolean compositeFetch;

  /*
  Under YARN, this defaults to one or more of the local directories, along with the appId in the path.
  Note: The containerId is not part of this.
  ${yarnLocalDir}/usercache/${user}/appcache/${applicationId}. (Referred to as ${appDir} later in the docs
   */
  private final LocalDirAllocator lDirAlloc = new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);;

  /**
   * @param conf     the configuration from which local-dirs will be picked up
   * @param uniqueIdForOutputFiles a unique identifier for the specific input / output. This is expected to be
   *                 unique for all the Inputs / Outputs within a container - i.e. even if the
   *                 container is used for multiple tasks, this id should be unique for inputs /
   *                 outputs spanning across tasks. This is also expected to be unique across all
   *                 tasks for a vertex.
   * @param dagID    DAG identifier for the specific job
   */
  public TezTaskOutputFiles(Configuration conf, String uniqueIdForOutputFiles, int dagID,
                            String containerId, int vertexId,
                            boolean compositeFetch) {
    this.conf = conf;
    this.uniqueId = uniqueIdForOutputFiles;
    this.outputDir = compositeFetch ?
        Constants.VERTEX_PREFIX + vertexId : Constants.TEZ_RUNTIME_TASK_OUTPUT_DIR;
    this.dagId = compositeFetch ?
        Constants.DAG_PREFIX + dagID + Path.SEPARATOR + containerId + Path.SEPARATOR :
        Constants.DAG_PREFIX + dagID + Path.SEPARATOR;
    this.compositeFetch = compositeFetch;
  }

  /**
   * Create a local output file name. This should *only* be used if the size
   * of the file is not known. Otherwise use the equivalent which accepts a size
   * parameter.
   *
   * ${appDir}/output/${uniqueId}/file.out
   * e.g. application_1418684642047_0006/output/attempt_1418684642047_0006_1_00_000000_0_10003/file.out
   *
   * The structure of this file name is critical, to be served by the MapReduce ShuffleHandler.
   *
   * @return path the path to write to
   * @throws IOException
   */
  // size unknown
  @Override
  public Path getOutputFileForWrite() throws IOException {
    Path attemptOutput =
      new Path(getAttemptOutputDir(), Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING);
    return lDirAlloc.getLocalPathForWrite(attemptOutput.toString(), 0L, conf, false);
  }

  /**
   * Create a local output index file name.
   *
   * ${appDir}/output/${uniqueId}/file.out.index
   * e.g. application_1418684642047_0006/output/attempt_1418684642047_0006_1_00_000000_0_10003/file.out.index
   *
   * The structure of this file name is critical, to be served by the MapReduce ShuffleHandler.
   *
   * @return path the path to write the index file to
   * @throws IOException
   */
  // for index files, do not bother with size
  @Override
  public Path getOutputIndexFileForWrite() throws IOException {
    Path attemptIndexOutput =
      new Path(getAttemptOutputDir(), Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING +
                                      Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING);
    return lDirAlloc.getLocalPathForWrite(attemptIndexOutput.toString(), 0L, conf, false);
  }

  @Override
  public Path getFileForWrite(String uniqueName) throws IOException {
    Path outputPath;
    if (!compositeFetch
        && uniqueName.startsWith(COMPOSITE_SPILL_FILE_PREFIX)
        && uniqueName.endsWith(SPILL_FILE_EXTENSION)) {
      String spillNumber = uniqueName.substring(
          COMPOSITE_SPILL_FILE_PREFIX.length(), uniqueName.length() - SPILL_FILE_EXTENSION.length());
      outputPath = new Path(getDagOutputDir(this.outputDir),
          uniqueId + '_' + spillNumber + Path.SEPARATOR + Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING);
    } else {
      outputPath = new Path(getAttemptOutputDir(), uniqueName);
    }
    return lDirAlloc.getLocalPathForWrite(outputPath.toString(), 0L, conf, false);
  }

  /**
   * Create a local output spill index file name.
   *
   * ${appDir}/output/${uniqueId}_${spillNumber}/file.out.index
   * e.g. application_1422270854961_0027/output/attempt_1422270854961_0027_1_00_000001_0_10003_1/file.out.index
   *
   * @param spillNumber the spill number
   * @return path the path to write the spill index file for the specific spillNumber
   * @throws IOException
   */
  // for index files, do not bother with size
  @Override
  public Path getSpillIndexFileForWrite(int spillNumber) throws IOException {
    assert spillNumber >= 0;
    String outputDirStr;
    if (compositeFetch) {
      outputDirStr = new Path(getAttemptOutputDir(),
          getSpillFileName(spillNumber) + Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING).toString();
    } else {
      String dagPath = getDagOutputDir(this.outputDir);
      outputDirStr = dagPath + Path.SEPARATOR + uniqueId + '_' + spillNumber
        + Path.SEPARATOR + Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING
        + Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING;
    }
    return lDirAlloc.getLocalPathForWrite(outputDirStr, 0L, conf, false);
  }

  /**
   * Create a local input file name.
   *
   * For non-composite fetch:
   * ${appDir}/${uniqueId}_src_{$srcId}_spill_${spillNumber}.out
   *
   * For composite fetch:
   * ${appDir}/dag_${dagId}/${containerId}/vertex_${vertexId}/${uniqueId}/src_${srcId}_spill_${spillNumber}.out
   *
   * Files are not clobbered due to the uniqueId along with spillId being different for Outputs /
   * Inputs within the same task (and across tasks)
   *
   * @param srcIdentifier The identifier for the source
   * @param spillNum
   * @param size the size of the file  @return path the path to the input file.
   * @throws IOException
   */
  @Override
  public Path getInputFileForWrite(int srcIdentifier, int spillNum, long size) throws IOException {
    String fileName = getSpillFileName(srcIdentifier, spillNum);
    String dagPath = compositeFetch ?
        new Path(getAttemptOutputDir(), fileName).toString() :
        getDagOutputDir(fileName);
    return lDirAlloc.getLocalPathForWrite(dagPath, size, conf, false);
  }

  // do not use size because we can get only approximate size
  @Override
  public Path getMergedFileForWrite(String fileName, int mergeNumber) throws IOException {
    String namePart = removeFinalExtension(fileName);
    String outputPathString = getDagOutputDir(namePart);
    Path outputPathInit = lDirAlloc.getLocalPathForWrite(outputPathString, 0L, conf, false);
    return outputPathInit.suffix(Constants.MERGED_OUTPUT_PREFIX + mergeNumber);
  }

  /**
   * Construct a spill file name, given a spill number and src id
   *
   * For non-composite fetch:
   * ${uniqueId}_src_${srcId}_spill_${spillNumber}.out
   *
   * For composite fetch:
   * src_${srcId}_spill_${spillNumber}.out
   *
   *
   * @return a spill file name independent of the unique identifier and local directories
   */
  @Override
  public String getSpillFileName(int spillNumber) {
    return COMPOSITE_SPILL_FILE_PREFIX + spillNumber + SPILL_FILE_EXTENSION;
  }

  @Override
  public String getSpillFileName(int srcId, int spillNum) {
    String prefix = compositeFetch ? SRC_SPILL_FILE_PREFIX : uniqueId + SPILL_FILE_SRC_SEPARATOR;
    return prefix + srcId + SPILL_FILE_SPILL_SEPARATOR + spillNum + SPILL_FILE_EXTENSION;
  }

  public String getDagOutputDir(String child) {
    if (!compositeFetch) {
      return child;
    }
    if (child.startsWith(SRC_SPILL_FILE_PREFIX)) {
      return new Path(getAttemptOutputDir(), child).toString();
    }
    return dagId.concat(child);
  }

  /*
   * if service_id = mapreduce_shuffle  then "${appDir}/output/${uniqueId}"
   * if service_id = tez_shuffle  then "${appDir}/dagId/output/${uniqueId}"
                                   --> "${appDir}/dagId/containerId/vertexId/${uniqueId}"
                                   while shuffle map ids omit the containerId prefix:
                                   "vertexId/${uniqueId}"
   */
  private Path getAttemptOutputDir() {
    String dagPath = getDagOutputDir(this.outputDir);
    return new Path(dagPath, uniqueId);
  }

  private static String removeFinalExtension(String fileName) {
    int extensionIndex = fileName.lastIndexOf('.');
    return extensionIndex == -1 ? fileName : fileName.substring(0, extensionIndex);
  }
}
