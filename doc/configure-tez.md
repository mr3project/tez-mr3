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

Below we describe the configuration keys for Tez runtime in MR3.

## VertexManager

|**Name**|**Default value**|Description|
|--------|:----------------|:----------|
|tez.shuffle-vertex-manager.enable.auto-parallel|false|**true**: enable auto parallelism for ShuffleVertexManager.  **false**: disable auto parallelism. For more details, see [Auto Parallelism](/docs/features/hivemr3/auto-parallelism/).|
|tez.shuffle-vertex-manager.auto-parallel.min.num.tasks|20|Minimum number of Tasks to trigger auto parallelism. For example, if the value is set to 20, only those Vertexes with at least 20 Tasks are considered for auto parallelism. The user can effectively disable auto parallelism by setting this configuration key to a large value.|
|tez.shuffle-vertex-manager.auto-parallel.max.reduction.percentage|10|Percentage of Tasks that can be kept after applying auto parallelism. For example, if the value is set to 10, the number of Tasks can be reduced by up to 100 - 10 = 90 percent, thereby leaving 10 percent of Tasks.|
|tez.shuffle-vertex-manager.use-stats-auto-parallelism|false|**true**: analyze input statistics when applying auto parallelism.  **false**: do not use input statistics.|
|tez.shuffle.vertex.manager.auto.parallelism.min.percent|20|Lower limit when normalizing input statistics. For example, if the value is set to 20, input statistics are normalized between 20 and 100. That is, an input size of zero is normalized to 20 while the maximum input size is mapped to 100.|

## Runtime

|**Name**|**Default value**|Description|
|--------|:----------------|:----------|
|tez.runtime.use.free.memory.fetched.input|false|**true**: If the size of free memory exceeds the size of memory allocated to a single Task, fetchers use MemoryFetchedInput (for unordered data) and InMemoryMapOutput and (for ordered data) instead of spilling to local disks. **false**: Fetchers do not consider the size of free memory.|
|tez.runtime.free.memory.factor.for.fetched.input|2|Multiplier used in calculating the total amount of free memory for storing shuffle input data per LogicalInput.|
|tez.runtime.shuffle.unordered.memory.streaming|false|**true**: Fetchers do not write unordered data to local disks. **false**: Fetchers may write unordered data to local disks.|
|tez.runtime.use.free.memory.writer.output|false|**true**: If the size of free memory exceeds the memory size specified by `tez.runtime.free.memory.writer.output.threshold.mb`, tasks store their output in memory instead of writing to local disks. **false**: Tasks write their output to local disk. If set to true, set `hive.mr3.delete.vertex.local.directory` to true in `hive-site.xml`. Effective only with pipelined shuffling.|
|tez.runtime.free.memory.writer.output.threshold.mb|6144|Free memory threshold in MB for writing output in memory when `tez.runtime.use.free.memory.writer.output` is set to true.|
|tez.runtime.pipelined-shuffle.enabled|false|**true**: Use pipelined shuffling. **false**: Do not use pipelined shuffling. If set to true/false, another configuration key `tez.runtime.enable.final-merge.in.output` is automatically set to false/true, respectively. Using speculative execution with pipelined shuffling is not recommended.|
|tez.runtime.pipelined.sorter.use.soft.reference|false|**true**: use soft references for ByteBuffers allocated in PipelinedSorter. These soft references are reused across TaskAttempts running in the same ContainerWorker. **false**: do not use soft references.| 
|tez.runtime.transfer.data-via-events.enabled|true|**true**: Embed unordered data directly in messages (of type `DataMovementEvent`). **false**: Do not embed. Effective only for Vertexes with a single output partition.|
|tez.runtime.transfer.data-via-events.max-size|2048|Maximum size of unordered data that can be directly embedded in messages and thus avoids creating fetchers.|

## Shuffle handlers

|**Name**|**Default value**|Description|
|--------|:----------------|:----------|
|tez.shuffle.connection-keep-alive.enable|false|**true**: keep connections alive for reuse. **false**: do not reuse.|
|tez.shuffle.max.threads|0|Number of threads for each shuffle handler. The default value of 0 sets the number of threads to 2 * the number of cores.|
|tez.shuffle.listen.queue.size|128|Size of the listening queue. Can be set to the value in `/proc/sys/net/core/somaxconn`.|
|tez.shuffle.port|13563|port number for shuffle handlers. If a ContainerWorker fails to secure a port number for a shuffle handler, it chooses a random port number instead.|
|tez.shuffle.mapoutput-info.meta.cache.size|1000|Size of meta data of the output of mappers.|

## ShuffleServer and fetchers

|**Name**|**Default value**|Description|
|--------|:----------------|:----------|
|tez.am.shuffle.auxiliary-service.id|mapreduce_shuffle|Service ID for the external shuffle service. Set to `tez_shuffle` to use MR3 shuffle handlers. Must be set to `tez_shuffle` on Kubernetes and in standalone mode.|
|tez.shuffle.skip.verify.request|false|**true**: MR3 shuffle handlers skip checking the validity of shuffle requests. **false**: MR3 shuffle handlers check the validity of shuffle requests. Effective only for MR3 shuffle handlers (with `tez_shuffle`).|
|tez.runtime.shuffle.keep-alive.enabled|false|**true**: keep connections alive for reuse in fetchers. **false**: do not reuse.|
|tez.runtime.shuffle.connect.timeout|27500|Maximum time in milliseconds for trying to connect to the shuffle service or the built-in shuffle handler before reporting fetch-failures. For more details, see [Fault Tolerance](/docs/features/mr3/fault-tolerance/).|
|tez.runtime.shuffle.parallel.copies|20|Maximum number of fetchers per LogicalInput. Note that a single RuntimeTask can create several LogicalInputs.|
|tez.runtime.shuffle.total.parallel.copies|40|Maximum number of fetchers per ContainerWorker.|
|tez.runtime.shuffle.fetch.max.task.output.at.once|20|Maximum number of task output files to fetch per fetch request. A large value can cause HTTP 400 errors.|
|tez.runtime.shuffle.ranges.scheme|first|**first**: ShuffleServer selects randomly LogicalInput for shuffling. **max** (experimental): ShuffleServer selects LogicalInput with the most number of pending inputs.|
|tez.runtime.optimize.local.fetch|true|**true**: Unordered data stored on local disks is directly read. **false**: Unordered data stored on local disks is read via fetchers. Automatically set to false when using memory-to-memory shuffling.|
|tez.runtime.optimize.local.fetch.ordered|true|**true**: Ordered data stored on local disks is directly read. **false**: Ordered data stored on local disks is read via fetchers. Automatically set to false when using memory-to-memory shuffling.|

## Backpressure and speculative fetching

|**Name**|**Default value**|Description|
|--------|:----------------|:----------|
|tez.shuffle.max.block.requests.thread.multiple|2|Multiplier used in calculating the maximum number of active shuffle requests (or Netty channels) allowed per ContainerWorker before backpressure is triggered.|
|tez.runtime.shuffle.speculative.fetch.wait.millis|30000|Elapsed time threshold for a fetcher before triggering speculative fetching.|
|tez.runtime.shuffle.stuck.fetcher.threshold.millis|3000|Elapsed time threshold for a fetcher before triggering backpressure and blocking further connections to the shuffle handler.|
|tez.runtime.shuffle.stuck.fetcher.release.millis|15000|Elapsed time threshold after which backpressure is lifted, resuming the creation of fetchers that contact the previously blocked shuffle handler.|
|tez.runtime.shuffle.max.speculative.fetch.attempts|2|Maximum number of speculative fetchers for each fetch attempt.|

