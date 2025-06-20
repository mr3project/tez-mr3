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

public interface MRJobConfig {

  static final String MR_TEZ_PREFIX = "mapreduce.tez.";
  
  // Put all of the attribute names in here so that Job and JobContext are
  // consistent.
  public static final String INPUT_FORMAT_CLASS_ATTR = "mapreduce.job.inputformat.class";
  
  public static final String NEW_API_MAPPER_CONFIG = "mapred.mapper.new-api";
  
  public static final String NEW_API_REDUCER_CONFIG = "mapred.reducer.new-api";

  public static final String COMBINE_CLASS_ATTR = "mapreduce.job.combine.class";

  public static final String OUTPUT_FORMAT_CLASS_ATTR = "mapreduce.job.outputformat.class";

  public static final String PARTITIONER_CLASS_ATTR = "mapreduce.job.partitioner.class";

  public static final String MAP_MEMORY_MB = "mapreduce.map.memory.mb";
  public static final int DEFAULT_MAP_MEMORY_MB = 1024;

  public static final String MAP_CPU_VCORES = "mapreduce.map.cpu.vcores";
  public static final int DEFAULT_MAP_CPU_VCORES = 1;

  public static final String REDUCE_MEMORY_MB = "mapreduce.reduce.memory.mb";
  public static final int DEFAULT_REDUCE_MEMORY_MB = 1024;

  public static final String REDUCE_CPU_VCORES = "mapreduce.reduce.cpu.vcores";
  public static final int DEFAULT_REDUCE_CPU_VCORES = 1;

  public static final String NUM_MAPS = "mapreduce.job.maps";
  public static final String NUM_REDUCES = "mapreduce.job.reduces";

  public static final String SPLIT_METAINFO_MAXSIZE = "mapreduce.job.split.metainfo.maxsize";
  public static final long DEFAULT_SPLIT_METAINFO_MAXSIZE = 10000000L;

  public static final String JOB_LOCAL_DIR = "mapreduce.job.local.dir";

  public static final String CACHE_LOCALFILES = "mapreduce.job.cache.local.files";

  public static final String CACHE_LOCALARCHIVES = "mapreduce.job.cache.local.archives";

  /**
   * Used by committers to set a job-wide UUID.
   */
  public static final String JOB_COMMITTER_UUID = "job.committer.uuid";

  public static String LAZY_OUTPUTFORMAT_OUTPUTFORMAT =
      "mapreduce.output.lazyoutputformat.outputformat";

  public static String FILEOUTPUTFORMAT_BASE_OUTPUT_NAME =
      "mapreduce.output.basename";

  public static final String TASK_ATTEMPT_ID = "mapreduce.task.attempt.id";

  public static final String TASK_ISMAP = "mapreduce.task.ismap";

  public static final String JOB_UBERTASK_ENABLE =
    "mapreduce.job.ubertask.enable";

  public static final String MR_PREFIX = "yarn.app.mapreduce.";

  public static final String MR_AM_PREFIX = MR_PREFIX + "am.";

  /** The staging directory for map reduce.*/
  public static final String MR_AM_STAGING_DIR = MR_AM_PREFIX+"staging-dir";

  /** The amount of memory the MR app master needs.*/
  public static final String MR_AM_VMEM_MB = MR_AM_PREFIX + "resource.mb";
  public static final int DEFAULT_MR_AM_VMEM_MB = 1536;

  public static final String JOB_SPLIT = "job.split";

  public static final String JOB_SPLIT_METAINFO = "job.splitmetainfo";

  public static final String APPLICATION_ATTEMPT_ID =
      "mapreduce.job.application.attempt.id";

  public static final String MROUTPUT_FILE_NAME_PREFIX
      = MR_TEZ_PREFIX + "mroutput.file-name.prefix";

  public static final String VERTEX_NAME = "mapreduce.task.vertex.name";
  public static final String VERTEX_ID = "mapreduce.task.vertex.id";

  public static final String MR_TEZ_SPLITS_VIA_EVENTS = MR_TEZ_PREFIX + "splits.via.events";
  public static final boolean MR_TEZ_SPLITS_VIA_EVENTS_DEFAULT = true;

  public static final String MR_TEZ_INPUT_INITIALIZER_SERIALIZE_EVENT_PAYLOAD = MR_TEZ_PREFIX
      + "input.initializer.serialize.event.payload";
  public static final boolean MR_TEZ_INPUT_INITIALIZER_SERIALIZE_EVENT_PAYLOAD_DEFAULT = true;
  
}
