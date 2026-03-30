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

  String MR_TEZ_PREFIX = "mapreduce.tez.";
  
  // Put all of the attribute names in here so that Job and JobContext are consistent.
  String INPUT_FORMAT_CLASS_ATTR = "mapreduce.job.inputformat.class";
  
  String NEW_API_MAPPER_CONFIG = "mapred.mapper.new-api";
  String NEW_API_REDUCER_CONFIG = "mapred.reducer.new-api";

  String OUTPUT_FORMAT_CLASS_ATTR = "mapreduce.job.outputformat.class";
  String PARTITIONER_CLASS_ATTR = "mapreduce.job.partitioner.class";

  String SPLIT_METAINFO_MAXSIZE = "mapreduce.job.split.metainfo.maxsize";
  long DEFAULT_SPLIT_METAINFO_MAXSIZE = 10000000L;

  String JOB_LOCAL_DIR = "mapreduce.job.local.dir";

  String CACHE_LOCALFILES = "mapreduce.job.cache.local.files";
  String CACHE_LOCALARCHIVES = "mapreduce.job.cache.local.archives";

  /**
   * Used by committers to set a job-wide UUID.
   */
  String JOB_COMMITTER_UUID = "job.committer.uuid";

  String LAZY_OUTPUTFORMAT_OUTPUTFORMAT =
      "mapreduce.output.lazyoutputformat.outputformat";

  String FILEOUTPUTFORMAT_BASE_OUTPUT_NAME =
      "mapreduce.output.basename";

  /** The staging directory for map reduce.*/
  String MR_PREFIX = "yarn.app.mapreduce.";
  String MR_AM_PREFIX = MR_PREFIX + "am.";
  String MR_AM_STAGING_DIR = MR_AM_PREFIX + "staging-dir";

  String JOB_SPLIT = "job.split";
  String JOB_SPLIT_METAINFO = "job.splitmetainfo";

  String APPLICATION_ATTEMPT_ID = "mapreduce.job.application.attempt.id";
  String VERTEX_NAME = "mapreduce.task.vertex.name";
  String VERTEX_ID = "mapreduce.task.vertex.id";
  String TASK_ATTEMPT_ID = "mapreduce.task.attempt.id";
  String TASK_ISMAP = "mapreduce.task.ismap";

  String MR_TEZ_SPLITS_VIA_EVENTS = MR_TEZ_PREFIX + "splits.via.events";
  boolean MR_TEZ_SPLITS_VIA_EVENTS_DEFAULT = true;

  String MR_TEZ_INPUT_INITIALIZER_SERIALIZE_EVENT_PAYLOAD = MR_TEZ_PREFIX
      + "input.initializer.serialize.event.payload";
  boolean MR_TEZ_INPUT_INITIALIZER_SERIALIZE_EVENT_PAYLOAD_DEFAULT = true;

  // used by MR3 test code
  String MAP_MEMORY_MB = "mapreduce.map.memory.mb";
  String REDUCE_MEMORY_MB = "mapreduce.reduce.memory.mb";
  String NUM_REDUCES = "mapreduce.job.reduces";
}
