---
title: Configuring Tez Runtime
sidebar_position: 2
---

The behavior of Tez runtime is specified by the configuration file `tez-site.xml` in the classpath.
MR3 inherits many configuration keys for Tez runtime from original Tez.
For example, `tez.runtime.io.sort.mb` specifies the amount of memory required for sorting the output,
and `tez.runtime.shuffle.merge.percent` specifies the fraction of shuffle data that should be merged at a time.

MR3 also introduces additional configuration keys which are specific to new features of MR3,
and may interpret existing configuration keys in a different way.

Some runtime keys can be set for each LogicalInput or LogicalOutput in a DAG.
Keys used to start ShuffleServer and shuffle handlers are fixed for all DAGs in the same application.

Below we describe the configuration keys for Tez runtime in MR3.

## Vertex management and auto parallelism

|**Name**|**Default value**|Description|
|--------|:----------------|:----------|
|tez.shuffle-vertex-manager.desired-task-input-size|104857600|Desired input size in bytes per Task when auto parallelism is enabled.|
|tez.shuffle-vertex-manager.enable.auto-parallel|false|**true**: enable auto parallelism for ShuffleVertexManager. **false**: disable auto parallelism. For more details, see [Auto Parallelism](/docs/features/hivemr3/auto-parallelism/).|
|tez.shuffle-vertex-manager.auto-parallel.min.num.tasks|20|Minimum number of Tasks to trigger auto parallelism. For example, if the value is set to 20, only those Vertexes with at least 20 Tasks are considered for auto parallelism. The user can effectively disable auto parallelism by setting this configuration key to a large value.|
|tez.shuffle-vertex-manager.auto-parallel.max.reduction.percentage|10|Percentage of Tasks that can be kept after applying auto parallelism. For example, if the value is set to 10, the number of Tasks can be reduced by up to 90 percent, thereby leaving 10 percent of Tasks.|
|tez.shuffle-vertex-manager.min-task-parallelism|1|Minimum parallelism after applying auto parallelism.|
|tez.shuffle-vertex-manager.min-src-fraction|0.25|Fraction of source Tasks that should complete before scheduling Tasks in a Vertex with a ScatterGather edge.|
|tez.shuffle-vertex-manager.max-src-fraction|0.75|Fraction of source Tasks at which all Tasks in a Vertex with a ScatterGather edge can be scheduled. Between the minimum and maximum fractions, the number of Tasks ready for scheduling increases linearly.|
|tez.shuffle-vertex-manager.use-stats-auto-parallelism|false|**true**: analyze input statistics when applying auto parallelism. **false**: do not use input statistics.|
|tez.shuffle.vertex.manager.auto.parallelism.min.percent|20|Lower limit when normalizing input statistics. For example, if the value is set to 20, input statistics are normalized between 20 and 100. That is, an input size of zero is normalized to 20 while the maximum input size is mapped to 100.|

## Runtime data comparison and partitioning

|**Name**|**Default value**|Description|
|--------|:----------------|:----------|
|tez.runtime.key.secondary.comparator.class|none|Secondary comparator class for grouped keys. If unset, the key comparator is used.|
|tez.runtime.partitioner.class|none|Partitioner class for partitioned output.|
|tez.runtime.report.partition.stats|memory_optimized|Method for reporting partition statistics to ShuffleVertexManager. Valid values are **none**, **memory_optimized**, and **precise**.|

## Output sorting, buffering, and spilling

|**Name**|**Default value**|Description|
|--------|:----------------|:----------|
|tez.runtime.io.sort.factor|100|Maximum number of streams to merge at a time while sorting output.|
|tez.runtime.io.sort.mb|100|Amount of memory in MB for sorting output.|
|tez.runtime.pipelined.sorter.min-block.size.in.mb|2000|Minimum size in MB of a block allocated by PipelinedSorter.|
|tez.runtime.pipelined.sorter.use.soft.reference|false|**true**: use soft references for ByteBuffers allocated in PipelinedSorter. These soft references are reused across TaskAttempts running in the same ContainerWorker. **false**: do not use soft references.|
|tez.runtime.pipelined.sorter.lazy-allocate.memory|false|**true**: allocate sorter memory progressively when needed. **false**: allocate all sorter memory during initialization.|
|tez.runtime.pipelined.sorter.rle.threshold|0.1|Fraction used as the threshold for run-length encoding in PipelinedSorter.|
|tez.runtime.unordered.partitioned.non.pipelined.num.buffers|4|Number of buffers for non-pipelined unordered partitioned output.|
|tez.runtime.unordered.output.buffer.size-mb|100|Size in MB of the buffer for unordered output when data is not written directly to disk.|
|tez.runtime.unordered.non.pipelined.spill.compress|false|**true**: compress spills from non-pipelined unordered output. **false**: do not compress these spills.|

## Input buffering, merging, and IFile

|**Name**|**Default value**|Description|
|--------|:----------------|:----------|
|tez.runtime.ifile.readahead|true|**true**: enable readahead while reading IFile data. **false**: disable readahead.|
|tez.runtime.ifile.readahead.bytes|4194304|Number of bytes to read ahead from an IFile.|
|tez.runtime.shuffle.fetch.buffer.percent|0.9|Fraction of assigned memory used to buffer shuffle input.|
|tez.runtime.shuffle.memory.limit.percent|0.25|Maximum fraction of assigned memory used by a single shuffle input.|
|tez.runtime.shuffle.merge.percent|0.9|Fraction of shuffle memory at which merging starts.|
|tez.runtime.shuffle.memory-to-memory.segments|tez.runtime.io.sort.factor|Maximum number of in-memory segments to merge at a time. If unset, the value of `tez.runtime.io.sort.factor` is used.|
|tez.runtime.shuffle.memory-to-memory.enable|false|**true**: enable memory-to-memory merging for ordered shuffle input. **false**: disable memory-to-memory merging.|
|tez.runtime.task.input.post-merge.buffer.percent|0.9|Fraction of assigned memory reserved for shuffle input after the final merge.|

## Compression

|**Name**|**Default value**|Description|
|--------|:----------------|:----------|
|tez.runtime.compress|false|**true**: compress intermediate output. **false**: do not compress intermediate output.|
|tez.runtime.compress.codec|none|Compression codec class for intermediate output.|

## Pipelined shuffle and event-based transfer

|**Name**|**Default value**|Description|
|--------|:----------------|:----------|
|tez.runtime.pipelined-shuffle.enabled|false|**true**: use pipelined shuffling for unordered output. **false**: do not use pipelined shuffling for unordered output. If set to true/false, `tez.runtime.enable.final-merge.in.output` is automatically set to false/true, respectively. Using speculative execution with pipelined shuffling is not recommended.|
|tez.runtime.pipelined-shuffle.ordered.enabled|false|**true**: use pipelined shuffling for ordered output. **false**: do not use pipelined shuffling for ordered output.|
|tez.runtime.transfer.data-via-events.enabled|true|**true**: embed unordered data directly in messages of type `DataMovementEvent`. **false**: do not embed unordered data. Effective only for Vertexes with a single output partition.|
|tez.runtime.transfer.data-via-events.max-size|2048|Maximum size in bytes of unordered data that can be embedded directly in a `DataMovementEvent`.|

## Memory-backed input and output

|**Name**|**Default value**|Description|
|--------|:----------------|:----------|
|tez.runtime.use.free.memory.fetched.input|false|**true**: if enough free memory is available, fetchers keep shuffle input in memory instead of spilling to local disks. **false**: fetchers do not consider the size of free memory.|
|tez.runtime.free.memory.factor.for.fetched.input|1.0|Multiplier used in calculating the total amount of free memory for storing shuffle input data per LogicalInput.|
|tez.runtime.shuffle.unordered.memory.streaming|false|**true**: fetchers do not write unordered data to local disks. **false**: fetchers may write unordered data to local disks.|
|tez.runtime.use.free.memory.writer.output|false|**true**: if enough free memory is available, Tasks store their output in memory instead of writing to local disks. **false**: Tasks write their output to local disks. If set to true, set `hive.mr3.delete.vertex.local.directory` to true in `hive-site.xml`. Effective only with pipelined shuffling.|
|tez.runtime.free.memory.writer.output.threshold.mb|6144|Free memory threshold in MB for writing output in memory when `tez.runtime.use.free.memory.writer.output` is set to true.|

## ShuffleServer and fetchers

|**Name**|**Default value**|Description|
|--------|:----------------|:----------|
|tez.am.shuffle.auxiliary-service.id|mapreduce_shuffle|Service ID for the external shuffle service. Set to `tez_shuffle` to use MR3 shuffle handlers. Must be set to `tez_shuffle` on Kubernetes and in standalone mode.|
|tez.runtime.shuffle.parallel.copies|20|Maximum number of fetchers per LogicalInput. A single RuntimeTask can create several LogicalInputs.|
|tez.runtime.shuffle.total.parallel.copies|40|Maximum number of fetchers per ContainerWorker.|
|tez.runtime.shuffle.fetch.max.task.output.at.once|20|Maximum number of Task output files to fetch per request. A large value can cause HTTP 400 errors.|
|tez.runtime.shuffle.ranges.scheme|priority|Scheme used by ShuffleServer to select a LogicalInput for shuffling.|
|tez.runtime.shuffle.connection.fail.all.input|false|**true**: fail all inputs in a fetch request when the connection fails. **false**: do not fail all inputs.|
|tez.runtime.shuffle.connect.timeout|27500|Maximum time in milliseconds for trying to connect to the shuffle service or the built-in shuffle handler before reporting fetch failures. For more details, see [Fault Tolerance](/docs/features/mr3/fault-tolerance/).|
|tez.runtime.shuffle.keep-alive.enabled|false|**true**: keep connections alive for reuse in fetchers. **false**: do not reuse connections.|
|tez.runtime.shuffle.keep-alive.max.connections|20|Maximum number of keep-alive connections used by fetchers.|
|tez.runtime.shuffle.read.timeout|120000|Maximum time in milliseconds for reading shuffle data before a timeout.|
|tez.runtime.shuffle.buffersize|8192|Size in bytes of the network buffer used by fetchers.|
|tez.runtime.shuffle.ssl.enable|false|**true**: use SSL for shuffle transfers. **false**: do not use SSL.|
|tez.runtime.shuffle.fetch.verify-disk-checksum|true|**true**: verify checksums when fetching data directly to disk. **false**: do not verify checksums.|
|tez.runtime.optimize.local.fetch|true|**true**: read unordered data on local disks directly. **false**: read unordered data through fetchers. Automatically set to false when using memory-to-memory shuffling.|
|tez.runtime.optimize.local.fetch.ordered|true|**true**: read ordered data on local disks directly. **false**: read ordered data through fetchers. Automatically set to false when using memory-to-memory shuffling.|

## Shuffle handlers

|**Name**|**Default value**|Description|
|--------|:----------------|:----------|
|tez.shuffle.connection-keep-alive.enable|false|**true**: keep connections alive for reuse. **false**: do not reuse connections.|
|tez.shuffle.connection-keep-alive.timeout|5|Timeout in seconds for keep-alive connections.|
|tez.shuffle.max.connections|0|Maximum number of shuffle connections. The default value of 0 sets no limit.|
|tez.shuffle.max.threads|0|Number of threads for each shuffle handler. The default value of 0 sets the number of threads to twice the number of cores.|
|tez.shuffle.transfer.buffer.size|131072|Size in bytes of the shuffle transfer buffer.|
|tez.shuffle.transferTo.allowed|true|**true**: allow `transferTo` for shuffle transfers. **false**: do not use `transferTo`. The default is false on Windows.|
|tez.shuffle.max.session-open-files|3|Maximum number of files that a single shuffle request can open at the same time.|
|tez.shuffle.listen.queue.size|128|Size of the listening queue. Can be set to the value in `/proc/sys/net/core/somaxconn`.|
|tez.shuffle.port|13563|Port number for shuffle handlers. If a ContainerWorker fails to secure the port, it chooses a random port instead.|
|tez.shuffle.mapoutput-info.meta.cache.size|1000|Number of map output metadata entries kept in the cache.|
|tez.shuffle.ssl.file.buffer.size|61440|Size in bytes of the SSL file buffer.|
|tez.shuffle.skip.verify.request|false|**true**: MR3 shuffle handlers skip checking the validity of shuffle requests. **false**: MR3 shuffle handlers check the validity of shuffle requests. Effective only for MR3 shuffle handlers using `tez_shuffle`.|

## Backpressure and speculative fetching

|**Name**|**Default value**|Description|
|--------|:----------------|:----------|
|tez.shuffle.max.block.requests.thread.multiple|2|Multiplier used in calculating the maximum number of active shuffle requests allowed per ContainerWorker before backpressure is triggered.|
|tez.runtime.shuffle.speculative.fetch.wait.millis|12500|Elapsed time in milliseconds for a fetcher before triggering speculative fetching.|
|tez.runtime.shuffle.stuck.fetcher.threshold.millis|2500|Elapsed time in milliseconds for a fetcher before triggering backpressure and blocking further connections to the shuffle handler.|
|tez.runtime.shuffle.stuck.fetcher.release.millis|10000|Elapsed time in milliseconds after which backpressure is lifted, resuming the creation of fetchers that contact the previously blocked shuffle handler.|
|tez.runtime.shuffle.max.speculative.fetch.attempts|2|Maximum number of speculative fetchers for each fetch attempt.|
