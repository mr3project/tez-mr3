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
package org.apache.tez.runtime.library.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.annotation.ConfigurationProperty;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;

import static org.apache.tez.dag.api.TezConfiguration.TEZ_AM_PREFIX;

public class TezRuntimeConfiguration {

  private static final String TEZ_RUNTIME_PREFIX = "tez.runtime.";

  // We allow only keys in tezRuntimeKeys[] to be updated at runtime by users.
  // tezRuntimeKeys[] = sum of confKeys[] in:
  //   OrderedGroupedKVInput, UnorderedKVInput, OrderedPartitionedKVOutput, UnorderedKVOutput, UnorderedPartitionedKVOutput
  private static final Set<String> tezRuntimeKeys = new HashSet<String>();
  private static final Set<String> umnodifiableTezRuntimeKeySet;

  // from tez-site.xml, in tezRuntimeKeys
  private static final Map<String, String> tezSiteXmlRuntimeConfMap = new HashMap<String, String>();

  //
  // constants
  //

  // only TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT is used
  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS = TEZ_RUNTIME_PREFIX +
      "merge.progress.records";
  public static final long TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT = 10000;

  //
  // CLASS keys
  //

  // fixed as BytesWritable, but keep for compatibility
  @ConfigurationProperty
  public static final String TEZ_RUNTIME_KEY_CLASS = TEZ_RUNTIME_PREFIX + "key.class";

  // fixed as BytesWritable, but keep for compatibility
  @ConfigurationProperty
  public static final String TEZ_RUNTIME_VALUE_CLASS = TEZ_RUNTIME_PREFIX + "value.class";

  // fixed as TezBytesComparator, but keep for compatibility
  @ConfigurationProperty
  public static final String TEZ_RUNTIME_KEY_COMPARATOR_CLASS =
    TEZ_RUNTIME_PREFIX + "key.comparator.class";

  @ConfigurationProperty
  public static final String TEZ_RUNTIME_KEY_SECONDARY_COMPARATOR_CLASS =
    TEZ_RUNTIME_PREFIX + "key.secondary.comparator.class";

  /**
   * Specifies a partitioner class
   */
  @ConfigurationProperty
  public static final String TEZ_RUNTIME_PARTITIONER_CLASS =
    TEZ_RUNTIME_PREFIX + "partitioner.class";

  //
  // The following keys are configurable for LogicalInput/Output of Edge in each DAG.
  // Thus they are added to tezRuntimeKeys[].
  // These keys are NOT consumed by Vertex and MRInput/Output.
  // Note: TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID is included.
  //

  /**
   * String value. Specifies the name of the shuffle auxiliary service.
   */
  @ConfigurationProperty
  public static final String TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID =
      TEZ_AM_PREFIX + "shuffle.auxiliary-service.id";
  public static final String TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT =
      TezConstants.TEZ_SHUFFLE_HANDLER_SERVICE_ID;

  /**
   * Configuration key to enable/disable IFile readahead.
   */
  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_IFILE_READAHEAD =
      TEZ_RUNTIME_PREFIX + "ifile.readahead";
  public static final boolean TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT = true;

  /**
   * Configuration key to set the IFile readahead length in bytes.
   */
  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_IFILE_READAHEAD_BYTES =
      TEZ_RUNTIME_PREFIX + "ifile.readahead.bytes";
  public static final int TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT =
      4 * 1024 * 1024;

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_IO_SORT_FACTOR =
      TEZ_RUNTIME_PREFIX + "io.sort.factor";
  public static final int TEZ_RUNTIME_IO_SORT_FACTOR_DEFAULT = 100;

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_IO_SORT_MB =
      TEZ_RUNTIME_PREFIX + "io.sort.mb";
  public static final int TEZ_RUNTIME_IO_SORT_MB_DEFAULT = 100;

  /**
   * Tries to allocate @link{#TEZ_RUNTIME_IO_SORT_MB} in chunks specified in
   * this parameter.
   */
  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB =
      TEZ_RUNTIME_PREFIX + "pipelined.sorter.min-block.size.in.mb";
  public static final int
      TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB_DEFAULT = 2000;

  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_PIPELINED_SORTER_USE_SOFT_REFERENCE =
      TEZ_RUNTIME_PREFIX + "pipelined.sorter.use.soft.reference";
  public static final boolean
      TEZ_RUNTIME_PIPELINED_SORTER_USE_SOFT_REFERENCE_DEFAULT = false;

  /**
   * Setting this to true would enable sorter
   * to auto-allocate memory on need basis in progressive fashion.
   *
   * Setting to false would allocate all available memory during
   * initialization of sorter. In such cases,@link{#TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB}
   * would be honored and memory specified in @link{#TEZ_RUNTIME_IO_SORT_MB}
   * would be initialized upfront.
   */
  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY =
      TEZ_RUNTIME_PREFIX + "pipelined.sorter.lazy-allocate.memory";
  public static final boolean TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY_DEFAULT = false;

  @ConfigurationProperty(type = "float")
  public static final String TEZ_RUNTIME_PIPELINED_SORTER_RLE_THRESHOLD_FRACTION =
      TEZ_RUNTIME_PREFIX + "pipelined.sorter.rle.threshold";
  public static final float TEZ_RUNTIME_PIPELINED_SORTER_RLE_THRESHOLD_FRACTION_DEFAULT = 0.1f;

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_UNORDERED_PARTITIONED_NON_PIPELINED_NUM_BUFFERS =
      TEZ_RUNTIME_PREFIX + "unordered.partitioned.non.pipelined.num.buffers";
  public static final int TEZ_RUNTIME_UNORDERED_PARTITIONED_NON_PIPELINED_NUM_BUFFERS_DEFAULT =
      4;

  /**
   * Size of the buffer to use if not writing directly to disk.
   */
  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB =
      TEZ_RUNTIME_PREFIX + "unordered.output.buffer.size-mb";
  public static final int TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB_DEFAULT = 100;

  /**
   * Report partition statistics (e.g better scheduling in ShuffleVertexManager). TEZ-2496
   * {@link org.apache.tez.runtime.library.api.TezRuntimeConfiguration.ReportPartitionStats}
   * defines the list of values that can be specified.
   */
  @ConfigurationProperty
  public static final String TEZ_RUNTIME_REPORT_PARTITION_STATS =
      TEZ_RUNTIME_PREFIX + "report.partition.stats";
  public static final String TEZ_RUNTIME_REPORT_PARTITION_STATS_DEFAULT =
      ReportPartitionStats.MEMORY_OPTIMIZED.getType();

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES =
      TEZ_RUNTIME_PREFIX + "shuffle.parallel.copies";
  public static final int TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES_DEFAULT = 20;

  @ConfigurationProperty(type = "float")
  public static final String TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT =
      TEZ_RUNTIME_PREFIX + "shuffle.fetch.buffer.percent";
  public static final float TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT_DEFAULT = 0.90f;

  @ConfigurationProperty(type = "float")
  public static final String TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT =
      TEZ_RUNTIME_PREFIX + "shuffle.memory.limit.percent";
  public static final float TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT_DEFAULT = 0.25f;

  // Rename to fraction
  @ConfigurationProperty(type = "float")
  public static final String TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT =
      TEZ_RUNTIME_PREFIX + "shuffle.merge.percent";
  public static final float TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT_DEFAULT = 0.90f;

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_MEMTOMEM_SEGMENTS =
      TEZ_RUNTIME_PREFIX + "shuffle.memory-to-memory.segments";

  // do not change the default value because local mode assumes 'false'.
  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM =
      TEZ_RUNTIME_PREFIX + "shuffle.memory-to-memory.enable";
  public static final boolean TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM_DEFAULT =
      false;

  @ConfigurationProperty(type = "float")
  public static final String TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT =
      TEZ_RUNTIME_PREFIX + "task.input.post-merge.buffer.percent";
  public static final float TEZ_RUNTIME_INPUT_BUFFER_PERCENT_DEFAULT = 0.9f;

  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED =
      TEZ_RUNTIME_PREFIX + "empty.partitions.info-via-events.enabled";
  public static final boolean TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED_DEFAULT = true;

  public static final String TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_ENABLED =
      TEZ_RUNTIME_PREFIX + "transfer.data-via-events.enabled";
  public static final boolean TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_ENABLED_DEFAULT = true;

  // FileBackedInMemIFileWriter.cacheSize == TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_MAX_SIZE
  // so we should have IFile.WRITER_BUFFER_SIZE_DEFAULT > TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_MAX_SIZE
  public static final String TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_MAX_SIZE =
      TEZ_RUNTIME_PREFIX + "transfer.data-via-events.max-size";
  public static final int TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_MAX_SIZE_DEFAULT = 2048;

  /**
   * Expert level setting. Enable pipelined shuffle in ordered outputs and in unordered
   * partitioned outputs. In ordered cases, it works with PipelinedSorter.
   * set tez.runtime.sort.threads to greater than 1 to enable PipelinedSorter.
   * Ensure to set tez.runtime.enable.final-merge.in.output=false.
   * Speculative execution needs to be turned off when using this parameter. --> Not the case in MR3
   */
  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED =
      TEZ_RUNTIME_PREFIX + "pipelined-shuffle.enabled";
  public static final boolean TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED_DEFAULT = false;

  public static final String TEZ_RUNTIME_PIPELINED_SHUFFLE_ORDERED_ENABLED =
    TEZ_RUNTIME_PREFIX + "pipelined-shuffle.ordered.enabled";
  public static final boolean TEZ_RUNTIME_PIPELINED_SHUFFLE_ORDERED_ENABLED_DEFAULT = false;

  /**
   * Expert level setting. Enable final merge in ordered (defaultsorter/pipelinedsorter) outputs.
   * Speculative execution needs to be turned off when disabling this parameter. //TODO: TEZ-2132
   */
  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT =
      TEZ_RUNTIME_PREFIX + "enable.final-merge.in.output";
  public static final boolean TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT_DEFAULT = true;

  /**
   * Used only for internal testing. Strictly not recommended to be used elsewhere. This
   * parameter could be changed/dropped later.
   */
  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT =
      TEZ_RUNTIME_PREFIX + "cleanup.files.on.interrupt";
  public static final boolean TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT_DEFAULT = false;

  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_USE_FREE_MEMORY_FETCHED_INPUT =
      TEZ_RUNTIME_PREFIX + "use.free.memory.fetched.input";
  public static final boolean TEZ_RUNTIME_USE_FREE_MEMORY_FETCHED_INPUT_DEFAULT = false;

  @ConfigurationProperty(type = "float")
  public static final String TEZ_RUNTIME_FREE_MEMORY_FACTOR_FOR_FETCHED_INPUT =
    TEZ_RUNTIME_PREFIX + "free.memory.factor.for.fetched.input";
  public static final float TEZ_RUNTIME_FREE_MEMORY_FACTOR_FOR_FETCHED_INPUT_DEFAULT = 1.0f;

  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_SHUFFLE_UNORDERED_MEMORY_STREAMING =
      TEZ_RUNTIME_PREFIX + "shuffle.unordered.memory.streaming";
  public static final boolean TEZ_RUNTIME_SHUFFLE_UNORDERED_MEMORY_STREAMING_DEFAULT = false;

  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT =
      TEZ_RUNTIME_PREFIX + "use.free.memory.writer.output";
  public static final boolean TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT_DEFAULT = false;

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_FREE_MEMORY_WRITER_OUTPUT_THRESHOLD_MB =
      TEZ_RUNTIME_PREFIX + "free.memory.writer.output.threshold.mb";
  public static final int TEZ_RUNTIME_FREE_MEMORY_WRITER_OUTPUT_THRESHOLD_MB_DEFAULT = 6 * 1024;

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_SPECULATIVE_FETCH_WAIT_MILLIS =
      TEZ_RUNTIME_PREFIX + "shuffle.speculative.fetch.wait.millis";
  public static final int TEZ_RUNTIME_SHUFFLE_SPECULATIVE_FETCH_WAIT_MILLIS_DEFAULT = 12500;

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_STUCK_FETCHER_THRESHOLD_MILLIS =
      TEZ_RUNTIME_PREFIX + "shuffle.stuck.fetcher.threshold.millis";
  public static final int TEZ_RUNTIME_SHUFFLE_STUCK_FETCHER_THRESHOLD_MILLIS_DEFAULT = 2500;

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_STUCK_FETCHER_RELEASE_MILLIS =
      TEZ_RUNTIME_PREFIX + "shuffle.stuck.fetcher.release.millis";
  public static final int TEZ_RUNTIME_SHUFFLE_STUCK_FETCHER_RELEASE_MILLIS_DEFAULT = 10000;

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_MAX_SPECULATIVE_FETCH_ATTEMPTS =
      TEZ_RUNTIME_PREFIX + "shuffle.max.speculative.fetch.attempts";
  public static final int TEZ_RUNTIME_SHUFFLE_MAX_SPECULATIVE_FETCH_ATTEMPTS_DEFAULT = 2;

  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_UNORDERED_NON_PIPELINED_SPILL_COMPRESS =
      TEZ_RUNTIME_PREFIX + "unordered.non.pipelined.spill.compress";
  public static final boolean TEZ_RUNTIME_UNORDERED_NON_PIPELINED_SPILL_COMPRESS_DEFAULT = false;

  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_COMPRESS = TEZ_RUNTIME_PREFIX + "compress";

  @ConfigurationProperty
  public static final String TEZ_RUNTIME_COMPRESS_CODEC = TEZ_RUNTIME_PREFIX + "compress.codec";

  //
  // The following keys are not configurable for LogicalInput/Output in each DAG.
  // They are consumed when starting ShuffleServer and fixed for all DAGs.
  // Thus they are not added to tezRuntimeKeys[].
  //

  // read only when constructing ShuffleServer, so not included in tezRuntimeKeys[]
  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_TOTAL_PARALLEL_COPIES =
      TEZ_RUNTIME_PREFIX + "shuffle.total.parallel.copies";
  public static final int TEZ_RUNTIME_SHUFFLE_TOTAL_PARALLEL_COPIES_DEFAULT = 40;

  // read only when constructing ShuffleServer, so not included in tezRuntimeKeys[]
  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE =
      TEZ_RUNTIME_PREFIX + "shuffle.fetch.max.task.output.at.once";
  public final static int TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE_DEFAULT = 20;

  // "first", "max" - used in ShuffleServer
  // read only when constructing ShuffleServer, so not included in tezRuntimeKeys[]
  public static final String TEZ_RUNTIME_SHUFFLE_RANGES_SCHEME =
      TEZ_RUNTIME_PREFIX + "shuffle.ranges.scheme";
  public static final String TEZ_RUNTIME_SHUFFLE_RANGES_SCHEME_DEFAULT = "priority";

  // read only in constructFetcherConfigCommon() from ShuffleServer, so not included in tezRuntimeKeys[]
  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_SHUFFLE_CONNECTION_FAIL_ALL_INPUT =
      TEZ_RUNTIME_PREFIX + "shuffle.connection.fail.all.input";
  public static final boolean TEZ_RUNTIME_SHUFFLE_CONNECTION_FAIL_ALL_INPUT_DEFAULT = false;

  // read only in constructFetcherConfigCommon() from ShuffleServer, so not included in tezRuntimeKeys[]
  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT =
      TEZ_RUNTIME_PREFIX + "shuffle.connect.timeout";
  public static final int TEZ_RUNTIME_SHUFFLE_STALLED_COPY_TIMEOUT_DEFAULT = 27500;

  // read only in constructFetcherConfigCommon() from ShuffleServer, so not included in tezRuntimeKeys[]
  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_ENABLED =
      TEZ_RUNTIME_PREFIX + "shuffle.keep-alive.enabled";
  public static final boolean TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_ENABLED_DEFAULT = false;

  // read only in constructFetcherConfigCommon() from ShuffleServer, so not included in tezRuntimeKeys[]
  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_MAX_CONNECTIONS =
      TEZ_RUNTIME_PREFIX + "shuffle.keep-alive.max.connections";
  public static final int TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_MAX_CONNECTIONS_DEFAULT = 20;

  // read only in constructFetcherConfigCommon() from ShuffleServer, so not included in tezRuntimeKeys[]
  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT =
      TEZ_RUNTIME_PREFIX + "shuffle.read.timeout";
  public final static int TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT_DEFAULT = 2 * 60 * 1000;

  // read only in constructFetcherConfigCommon() from ShuffleServer, so not included in tezRuntimeKeys[]
  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE =
      TEZ_RUNTIME_PREFIX + "shuffle.buffersize";
  public final static int TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE_DEFAULT = 8 * 1024;

  // read only in constructFetcherConfigCommon() from ShuffleServer, so not included in tezRuntimeKeys[]
  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_SHUFFLE_ENABLE_SSL =
      TEZ_RUNTIME_PREFIX + "shuffle.ssl.enable";
  public static final boolean TEZ_RUNTIME_SHUFFLE_ENABLE_SSL_DEFAULT = false;

  /**
   * Controls verification of data checksums when fetching data directly to
   * disk. Enabling verification allows the fetcher to detect corrupted data
   * and report the failure against the upstream task before the data reaches
   * the Processor and causes the fetching task to fail.
   */
  // read only in constructFetcherConfigCommon() from ShuffleServer, so not included in tezRuntimeKeys[]
  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_SHUFFLE_FETCH_VERIFY_DISK_CHECKSUM =
      TEZ_RUNTIME_PREFIX + "shuffle.fetch.verify-disk-checksum";
  public static final boolean TEZ_RUNTIME_SHUFFLE_FETCH_VERIFY_DISK_CHECKSUM_DEFAULT = true;

  // TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_ORDERED
  // Setting it to true is usually less efficient than setting it to false
  // because IFile.Reader (implementing KeyValueReaderBytesWritable) allocates a new byte array to store a key/value.
  // Thus, it is more efficient to set it to false so that the entire stream is decompressed at once into memory.

  /**
   * If the shuffle input is on the local host bypass the http fetch and access the files directly
   * only for unordered fetch
   */
  // read only in constructFetcherConfigCommon() from ShuffleServer, so not included in tezRuntimeKeys[]
  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH = TEZ_RUNTIME_PREFIX +
      "optimize.local.fetch";
  public static final boolean TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_DEFAULT = true;

  // only for ordered fetch
  // read only in constructFetcherConfigCommon() from ShuffleServer, so not included in tezRuntimeKeys[]
  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_ORDERED = TEZ_RUNTIME_PREFIX +
      "optimize.local.fetch.ordered";
  public static final boolean TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_ORDERED_DEFAULT = true;

  static {
    tezRuntimeKeys.add(TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID);
    tezRuntimeKeys.add(TEZ_RUNTIME_IFILE_READAHEAD);
    tezRuntimeKeys.add(TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
    tezRuntimeKeys.add(TEZ_RUNTIME_IO_SORT_FACTOR);
    tezRuntimeKeys.add(TEZ_RUNTIME_IO_SORT_MB);
    tezRuntimeKeys.add(TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB);
    tezRuntimeKeys.add(TEZ_RUNTIME_PIPELINED_SORTER_USE_SOFT_REFERENCE);
    tezRuntimeKeys.add(TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY);
    tezRuntimeKeys.add(TEZ_RUNTIME_PIPELINED_SORTER_RLE_THRESHOLD_FRACTION);
    tezRuntimeKeys.add(TEZ_RUNTIME_UNORDERED_PARTITIONED_NON_PIPELINED_NUM_BUFFERS);
    tezRuntimeKeys.add(TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB);
    tezRuntimeKeys.add(TEZ_RUNTIME_REPORT_PARTITION_STATS);
    tezRuntimeKeys.add(TEZ_RUNTIME_PARTITIONER_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_MEMTOMEM_SEGMENTS);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM);
    tezRuntimeKeys.add(TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT);
    tezRuntimeKeys.add(TEZ_RUNTIME_KEY_COMPARATOR_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_KEY_SECONDARY_COMPARATOR_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED);
    tezRuntimeKeys.add(TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_ENABLED);
    tezRuntimeKeys.add(TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_MAX_SIZE);
    tezRuntimeKeys.add(TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED);
    tezRuntimeKeys.add(TEZ_RUNTIME_PIPELINED_SHUFFLE_ORDERED_ENABLED);
    tezRuntimeKeys.add(TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT);
    tezRuntimeKeys.add(TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT);
    tezRuntimeKeys.add(TEZ_RUNTIME_USE_FREE_MEMORY_FETCHED_INPUT);
    tezRuntimeKeys.add(TEZ_RUNTIME_FREE_MEMORY_FACTOR_FOR_FETCHED_INPUT);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_UNORDERED_MEMORY_STREAMING);
    tezRuntimeKeys.add(TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT);
    tezRuntimeKeys.add(TEZ_RUNTIME_FREE_MEMORY_WRITER_OUTPUT_THRESHOLD_MB);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_SPECULATIVE_FETCH_WAIT_MILLIS);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_STUCK_FETCHER_THRESHOLD_MILLIS);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_STUCK_FETCHER_RELEASE_MILLIS);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_MAX_SPECULATIVE_FETCH_ATTEMPTS);
    tezRuntimeKeys.add(TEZ_RUNTIME_UNORDERED_NON_PIPELINED_SPILL_COMPRESS);
    tezRuntimeKeys.add(TEZ_RUNTIME_COMPRESS);
    tezRuntimeKeys.add(TEZ_RUNTIME_COMPRESS_CODEC);

    // Do not keep defaultConf as a static member because it holds a reference to ClassLoader
    // of the Thread that is active at the time of loading this class. The active Thread usually
    // belongs to a running DAG, so keeping defaultConf gives rise to memory leak of DAGClassLoader.
    Configuration defaultConf = new Configuration(false);

    // Tez runtime uses only Tez configurations.
    //  - do not include core-site.xml.
    //  - do not use 'allowed prefixes'
    defaultConf.addResource(TezConfiguration.TEZ_SITE_XML);

    for (Map.Entry<String, String> confEntry : defaultConf) {
      if (tezRuntimeKeys.contains(confEntry.getKey())) {
        tezSiteXmlRuntimeConfMap.put(confEntry.getKey(), confEntry.getValue());
      }
    }
    umnodifiableTezRuntimeKeySet = Collections.unmodifiableSet(tezRuntimeKeys);
  }

  public static Set<String> getTezRuntimeConfigKeySet() {
    return umnodifiableTezRuntimeKeySet;
  }

  public static Map<String, String> getTezRuntimeConfigDefaults() {
    return Collections.unmodifiableMap(tezSiteXmlRuntimeConfMap);
  }

  public enum ReportPartitionStats {
    /**
     * Don't report partition stats.
     */
    NONE("none"),

    /**
     * Report partition stats with less precision to reduce
     * memory and CPU overhead
     */
    MEMORY_OPTIMIZED("memory_optimized"),

    /**
     * Report precise partition stats in MB.
     */
    PRECISE("precise");

    private final String type;

    ReportPartitionStats(String type) {
      this.type = type;
    }

    public final String getType() {
      return type;
    }

    public boolean isEnabled() {
      return !equals(ReportPartitionStats.NONE);
    }

    public boolean isPrecise() {
      return equals(ReportPartitionStats.PRECISE);
    }

    public static ReportPartitionStats fromString(String type) {
      if (type != null) {
        for (ReportPartitionStats b : ReportPartitionStats.values()) {
          if (type.equalsIgnoreCase(b.type)) {
            return b;
          }
        }
      }
      throw new IllegalArgumentException("Invalid type " + type);
    }
  }
}
