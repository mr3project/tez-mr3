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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.annotation.ConfigurationClass;
import org.apache.tez.common.annotation.ConfigurationProperty;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVOutputConfig.SorterImpl;

/**
 * Meant for user configurable job properties.
 * <p/>
 * Note for developers: Whenever a new key is added to this file, it must also be added to the set of
 * known tezRuntimeKeys.
 * @see <a href="../../../../../../configs/TezRuntimeConfiguration.html">Detailed Configuration Information</a>
 * @see <a href="../../../../../configs/tez-runtime-default-template.xml">XML-based Config Template</a>
 */

@ConfigurationClass(templateFileName = "tez-runtime-default-template.xml")
public class TezRuntimeConfiguration {

  private static final String TEZ_RUNTIME_PREFIX = "tez.runtime.";

  private static final Set<String> tezRuntimeKeys = new HashSet<String>();
  private static final Set<String> umnodifiableTezRuntimeKeySet;
  private static final Set<String> otherKeys = new HashSet<String>();
  private static final Set<String> unmodifiableOtherKeySet;
  private static final Map<String, String> tezRuntimeConfMap = new HashMap<String, String>();
  private static final Map<String, String> otherConfMap = new HashMap<String, String>();

  /**
   * Prefixes from Hadoop configuration which are allowed.
   */
  private static final List<String> allowedPrefixes = new ArrayList<String>();

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

  // TODO Use the default value
  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_COMBINE_MIN_SPILLS =
      TEZ_RUNTIME_PREFIX + "combine.min.spills";
  public static final int TEZ_RUNTIME_COMBINE_MIN_SPILLS_DEFAULT = 3;

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

  /**
   * String value.
   * Which sorter implementation to use.
   * Valid values:
   *    - LEGACY
   *    - PIPELINED ( default )
   *    {@link org.apache.tez.runtime.library.conf.OrderedPartitionedKVOutputConfig.SorterImpl}
   */
  @ConfigurationProperty
  public static final String TEZ_RUNTIME_SORTER_CLASS =
      TEZ_RUNTIME_PREFIX + "sorter.class";
  public static final String TEZ_RUNTIME_SORTER_CLASS_DEFAULT = SorterImpl.PIPELINED.name();

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_PIPELINED_SORTER_SORT_THREADS =
      TEZ_RUNTIME_PREFIX + "pipelined.sorter.sort.threads";
  public static final int TEZ_RUNTIME_PIPELINED_SORTER_SORT_THREADS_DEFAULT = 2;

  /**
   * Integer value. Percentage of buffer to be filled before we spill to disk. Default value is 0,
   * which will spill for every buffer.
   */
  @ConfigurationProperty(type="int")
  public static final String TEZ_RUNTIME_UNORDERED_PARTITIONED_KVWRITER_BUFFER_MERGE_PERCENT =
      TEZ_RUNTIME_PREFIX + "unordered-partitioned-kvwriter.buffer-merge-percent";
  public static final int TEZ_RUNTIME_UNORDERED_PARTITIONED_KVWRITER_BUFFER_MERGE_PERCENT_DEFAULT =
      0;

  /**
   * Size of the buffer to use if not writing directly to disk.
   */
  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB =
      TEZ_RUNTIME_PREFIX + "unordered.output.buffer.size-mb";
  public static final int TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB_DEFAULT = 100;

  /**
   * Report partition statistics (e.g better scheduling in ShuffleVertexManager). TEZ-2496
   * This can be enabled/disabled at vertex level.
   * {@link org.apache.tez.runtime.library.api.TezRuntimeConfiguration.ReportPartitionStats}
   * defines the list of values that can be specified.
   * TODO TEZ-3303 Given ShuffleVertexManager doesn't consume precise stats
   * yet. So do not set the value to "precise" yet when ShuffleVertexManager is used.
   */
  @ConfigurationProperty
  public static final String TEZ_RUNTIME_REPORT_PARTITION_STATS =
      TEZ_RUNTIME_PREFIX + "report.partition.stats";
  public static final String TEZ_RUNTIME_REPORT_PARTITION_STATS_DEFAULT =
      ReportPartitionStats.MEMORY_OPTIMIZED.getType();

  /**
   * Specifies a partitioner class, which is used in Tez Runtime components
   * like OnFileSortedOutput
   */
  @ConfigurationProperty
  public static final String TEZ_RUNTIME_PARTITIONER_CLASS =
      TEZ_RUNTIME_PREFIX + "partitioner.class";

  /**
   * Specifies a combiner class (primarily for Shuffle)
   */
  @ConfigurationProperty
  public static final String TEZ_RUNTIME_COMBINER_CLASS =
      TEZ_RUNTIME_PREFIX + "combiner.class";

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES =
      TEZ_RUNTIME_PREFIX + "shuffle.parallel.copies";
  public static final int TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES_DEFAULT = 20;

  @ConfigurationProperty(type = "float")
  public static final String TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT =
      TEZ_RUNTIME_PREFIX + "shuffle.fetch.buffer.percent";
  public static final float TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT_DEFAULT =
      0.90f;

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
  public static final float TEZ_RUNTIME_INPUT_BUFFER_PERCENT_DEFAULT = 0.0f;

  @ConfigurationProperty
  public static final String TEZ_RUNTIME_INTERNAL_SORTER_CLASS =
      TEZ_RUNTIME_PREFIX + "internal.sorter.class";

  @ConfigurationProperty
  public static final String TEZ_RUNTIME_KEY_COMPARATOR_CLASS =
      TEZ_RUNTIME_PREFIX + "key.comparator.class";

  @ConfigurationProperty
  public static final String TEZ_RUNTIME_KEY_CLASS =
      TEZ_RUNTIME_PREFIX + "key.class";

  @ConfigurationProperty
  public static final String TEZ_RUNTIME_VALUE_CLASS =
      TEZ_RUNTIME_PREFIX + "value.class";

  // TODO Move this key to MapReduce
  @ConfigurationProperty
  public static final String TEZ_RUNTIME_KEY_SECONDARY_COMPARATOR_CLASS =
      TEZ_RUNTIME_PREFIX + "key.secondary.comparator.class";

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
   * set tez.runtime.sort.threads to greater than 1 to enable pipelinedsorter.
   * Ensure to set tez.runtime.enable.final-merge.in.output=false.
   * Speculative execution needs to be turned off when using this parameter. //TODO: TEZ-2132
   */
  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED =
      TEZ_RUNTIME_PREFIX + "pipelined-shuffle.enabled";
  public static final boolean TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED_DEFAULT = false;

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

  // if set to true, automatically set:
  //   1. tez.runtime.optimize.local.fetch = false
  //   2. tez.runtime.optimize.local.fetch.ordered = false
  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT =
      TEZ_RUNTIME_PREFIX + "use.free.memory.writer.output";
  public static final boolean TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT_DEFAULT = false;

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

  //
  // The following keys are not configurable for LogicalInput/Ouput in each DAG.
  // They are supposed to be fixed for all DAGs, similarly to keys in TezConfiguration.
  // Thus they are not added to tezRuntimeKeys[].
  //

  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_COMPRESS = TEZ_RUNTIME_PREFIX + "compress";

  @ConfigurationProperty
  public static final String TEZ_RUNTIME_COMPRESS_CODEC = TEZ_RUNTIME_PREFIX + "compress.codec";

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_TOTAL_PARALLEL_COPIES =
      TEZ_RUNTIME_PREFIX + "shuffle.total.parallel.copies";
  public static final int TEZ_RUNTIME_SHUFFLE_TOTAL_PARALLEL_COPIES_DEFAULT = 40;

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE =
      TEZ_RUNTIME_PREFIX + "shuffle.fetch.max.task.output.at.once";
  public final static int TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE_DEFAULT = 20;

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_MAX_INPUT_HOSTPORTS =
      TEZ_RUNTIME_PREFIX + "shuffle.max.input.hostports";
  public final static int TEZ_RUNTIME_SHUFFLE_MAX_INPUT_HOSTPORTS_DEFAULT = 10000;

  // "first", "max" - used in ShuffleServer
  public static final String TEZ_RUNTIME_SHUFFLE_RANGES_SCHEME =
      TEZ_RUNTIME_PREFIX + "shuffle.ranges.scheme";
  public static final String TEZ_RUNTIME_SHUFFLE_RANGES_SCHEME_DEFAULT = "first";

  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_SHUFFLE_CONNECTION_FAIL_ALL_INPUT =
      TEZ_RUNTIME_PREFIX + "shuffle.connection.fail.all.input";
  public static final boolean TEZ_RUNTIME_SHUFFLE_CONNECTION_FAIL_ALL_INPUT_DEFAULT = false;

  // TODO: move to tezRuntimeKeys[]
  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT =
      TEZ_RUNTIME_PREFIX + "shuffle.connect.timeout";
  public static final int TEZ_RUNTIME_SHUFFLE_STALLED_COPY_TIMEOUT_DEFAULT = 27500;

  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_ENABLED =
      TEZ_RUNTIME_PREFIX + "shuffle.keep-alive.enabled";
  public static final boolean TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_ENABLED_DEFAULT = false;

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_MAX_CONNECTIONS =
      TEZ_RUNTIME_PREFIX + "shuffle.keep-alive.max.connections";
  public static final int TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_MAX_CONNECTIONS_DEFAULT = 20;

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT =
      TEZ_RUNTIME_PREFIX + "shuffle.read.timeout";
  public final static int TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT_DEFAULT = 2 * 60 * 1000;

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE =
      TEZ_RUNTIME_PREFIX + "shuffle.buffersize";
  public final static int TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE_DEFAULT = 8 * 1024;

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
  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_SHUFFLE_FETCH_VERIFY_DISK_CHECKSUM =
      TEZ_RUNTIME_PREFIX + "shuffle.fetch.verify-disk-checksum";
  public static final boolean TEZ_RUNTIME_SHUFFLE_FETCH_VERIFY_DISK_CHECKSUM_DEFAULT = true;

  /**
   * If the shuffle input is on the local host bypass the http fetch and access the files directly
   * only for unordered fetch
   */
  // do not change the default value because local mode assumes 'true'.
  // do not change the default value because tez-site.xml does not set it.
  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH = TEZ_RUNTIME_PREFIX +
      "optimize.local.fetch";
  public static final boolean TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_DEFAULT = true;

  // for ordered fetched
  // set to false when tez.runtime.shuffle.memory-to-memory.enable=true.
  // do not change the default value because local mode assumes 'true'.
  // do not change the default value because tez-site.xml does not set it.
  @ConfigurationProperty(type = "boolean")
  public static final String TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_ORDERED = TEZ_RUNTIME_PREFIX +
      "optimize.local.fetch.ordered";
  public static final boolean TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_ORDERED_DEFAULT = true;

  @ConfigurationProperty(type = "integer")
  public static final String TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS = TEZ_RUNTIME_PREFIX +
      "merge.progress.records";
  public static final long TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT = 10000;

  static {
    // tezRuntimeKeys[] = sum of confKeys[] in:
    //   OrderedGroupedKVInput, UnorderedKVInput, OrderedPartitionedKVOutput, UnorderedKVOutput, UnorderedPartitionedKVOuptut
    tezRuntimeKeys.add(TEZ_RUNTIME_IFILE_READAHEAD);
    tezRuntimeKeys.add(TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
    tezRuntimeKeys.add(TEZ_RUNTIME_IO_SORT_FACTOR);
    tezRuntimeKeys.add(TEZ_RUNTIME_IO_SORT_MB);
    tezRuntimeKeys.add(TEZ_RUNTIME_COMBINE_MIN_SPILLS);
    tezRuntimeKeys.add(TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB);
    tezRuntimeKeys.add(TEZ_RUNTIME_PIPELINED_SORTER_USE_SOFT_REFERENCE);
    tezRuntimeKeys.add(TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY);
    tezRuntimeKeys.add(TEZ_RUNTIME_SORTER_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_PIPELINED_SORTER_SORT_THREADS);
    tezRuntimeKeys.add(TEZ_RUNTIME_UNORDERED_PARTITIONED_KVWRITER_BUFFER_MERGE_PERCENT);
    tezRuntimeKeys.add(TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB);
    tezRuntimeKeys.add(TEZ_RUNTIME_REPORT_PARTITION_STATS);
    tezRuntimeKeys.add(TEZ_RUNTIME_PARTITIONER_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_COMBINER_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_MEMTOMEM_SEGMENTS);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM);
    tezRuntimeKeys.add(TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT);
    tezRuntimeKeys.add(TEZ_RUNTIME_INTERNAL_SORTER_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_KEY_COMPARATOR_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_KEY_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_VALUE_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_KEY_SECONDARY_COMPARATOR_CLASS);
    tezRuntimeKeys.add(TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED);
    tezRuntimeKeys.add(TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_ENABLED);
    tezRuntimeKeys.add(TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_MAX_SIZE);
    tezRuntimeKeys.add(TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED);
    tezRuntimeKeys.add(TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT);
    tezRuntimeKeys.add(TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT);
    tezRuntimeKeys.add(TEZ_RUNTIME_USE_FREE_MEMORY_FETCHED_INPUT);
    tezRuntimeKeys.add(TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_SPECULATIVE_FETCH_WAIT_MILLIS);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_STUCK_FETCHER_THRESHOLD_MILLIS);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_STUCK_FETCHER_RELEASE_MILLIS);
    tezRuntimeKeys.add(TEZ_RUNTIME_SHUFFLE_MAX_SPECULATIVE_FETCH_ATTEMPTS);

    // Do not keep defaultConf as a static member because it holds a reference to ClassLoader
    // of the Thread that is active at the time of loading this class. The active Thread usually
    // belongs to a running DAG, so keeping defaultConf gives rise to memory leak of DAGClassLoader.
    Configuration defaultConf = new Configuration(false);

    defaultConf.addResource("core-default.xml");
    defaultConf.addResource("core-site.xml");
    defaultConf.addResource("tez-site.xml");

    for (Map.Entry<String, String> confEntry : defaultConf) {
      if (tezRuntimeKeys.contains(confEntry.getKey())) {
        tezRuntimeConfMap.put(confEntry.getKey(), confEntry.getValue());
      } else {
        // TODO TEZ-1232 Filter out parameters from TezConfiguration, and Task specific confs
        otherConfMap.put(confEntry.getKey(), confEntry.getValue());
        otherKeys.add(confEntry.getKey());
      }
    }

    // Do NOT need all prefixes from the following list. Only specific ones are allowed
    // "hadoop.", "hadoop.security", "io.", "fs.", "ipc.", "net.", "file.", "dfs.", "ha.", "s3.", "nfs3.", "rpc.", "ssl."
    allowedPrefixes.add("io.");
    allowedPrefixes.add("file.");
    allowedPrefixes.add("fs.");
    allowedPrefixes.add("ssl.");

    umnodifiableTezRuntimeKeySet = Collections.unmodifiableSet(tezRuntimeKeys);
    unmodifiableOtherKeySet = Collections.unmodifiableSet(otherKeys);
  }

  public static Set<String> getRuntimeConfigKeySet() {
    return umnodifiableTezRuntimeKeySet;
  }

  public static Set<String> getRuntimeAdditionalConfigKeySet() {
    return unmodifiableOtherKeySet;
  }

  public static List<String> getAllowedPrefixes() {
    return allowedPrefixes;
  }

  public static Map<String, String> getTezRuntimeConfigDefaults() {
    return Collections.unmodifiableMap(tezRuntimeConfMap);
  }

  public static Map<String, String> getOtherConfigDefaults() {
    return Collections.unmodifiableMap(otherConfMap);
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

    private ReportPartitionStats(String type) {
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
