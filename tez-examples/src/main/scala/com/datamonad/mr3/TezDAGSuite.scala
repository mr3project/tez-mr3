/*
 * Copyright (c) 2020, DataMonad.
 * All rights reserved.
 */

package com.datamonad.mr3

import com.google.protobuf.ByteString
import org.apache.hadoop.examples.terasort.{TeraGen, TeraInputFormat, TeraOutputFormat, TeraSortConfigKeys}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, LongWritable, NullWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.split.{JobSplitWriter, SplitMetaInfoReader}
import org.apache.hadoop.mapreduce.{InputFormat, InputSplit, Job}
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.yarn.api.records.{LocalResource, LocalResourceType, LocalResourceVisibility}
import com.datamonad.mr3.DAGAPI.{DAGProto, UserPayloadProto}
import com.datamonad.mr3.api.common.{MR3Conf, MR3ConfBuilder, MR3Constants, Utils}
import com.datamonad.mr3.api.dag.{TaskLocationHint, TaskLocationHintHostRack}
import com.datamonad.mr3.builder.DAGSetup.EntityDescriptorMapType.EntityDescriptorMapType
import com.datamonad.mr3.builder.DAGSetup.{BaseDagSetup, BaseMRDagSetup, CrossDagReuseWithLocalResourcesSetup, EntityDescriptorMapType, SimulateMRRExtendedSetup}
import com.datamonad.mr3.builder.tez.InputInitializerBuilder.{BaseInputInitializer, InputInitializerSendingInputDataInformationEvent, InputInitializerWaitingForInputInitializerEvent, SimulateInputInitializer}
import com.datamonad.mr3.builder.tez.LogicalInputBuilder.{BaseLogicalInput, LogicalInputWaitingForInputDataInformationEvent, LogicalInputWaitingForPid, SimulateMapLogicalInput, SimulateReduceLogicalInput}
import com.datamonad.mr3.builder.tez.LogicalOutputBuilder.{BaseLogicalOutput, LogicalOutputSendingPid, SimulateLogicalOutput, SleepMROutput}
import com.datamonad.mr3.builder.tez.OutputCommitterBuilder.BaseOutputCommitter
import com.datamonad.mr3.builder.tez.ProcessorBuilder.{BaseProcessor, CopyFileProcessor, ListFileSummationProcessor, ListFileTokenizerProcessor, NoOpFailProcessor, ProcessorSendingInputInitializerEvent, ProcessorSendingVertexManagerEvent, RecoveryProcessor, SleepAndAccessingHdfsProcessor, SleepProcessor, SorterProcessor, SummationForSorterProcessor, SummationProcessor, TeraGenProcessor, TokenizerProcessor}
import com.datamonad.mr3.builder.tez.TezProcessorBuilder.{GenDataProcessor, HashJoinProcessor, JoinValidateProcessor, SortMergeJoinProcessor}
import com.datamonad.mr3.builder.tez.TezUtilsForBuilder
import com.datamonad.mr3.builder.tez.TezUtilsForBuilder.{TeraSortSplitComparator, TeraSortTotalOrderPartitioner}
import com.datamonad.mr3.builder.tez.VertexManagerBuilder.BaseDaemonVertexManager
import com.datamonad.mr3.builder.{DAGBuilder, UtilsForBuilder}
import com.datamonad.mr3.common.security.TokenRenewer
import org.apache.tez.common.MRFrameworkConfigs
import org.apache.tez.dag.app.dag.impl.{ImmediateStartVertexManager, RootInputVertexManager, ScatterGatherEdgeManager}
import org.apache.tez.dag.library.vertexmanager.{InputReadyVertexManager, ShuffleVertexManager}
import org.apache.tez.examples.HashJoinExample.ForwardingProcessor
import org.apache.tez.mapreduce.committer.MROutputCommitter
import org.apache.tez.mapreduce.common.MRInputAMSplitGenerator
import org.apache.tez.mapreduce.hadoop.MRJobConfig
import org.apache.tez.mapreduce.input.{MRInput, MRInputLegacy}
import org.apache.tez.mapreduce.output.{MROutput, MROutputLegacy}
import org.apache.tez.mapreduce.partition.MRPartitioner
import org.apache.tez.mapreduce.processor.map.MapProcessor
import org.apache.tez.mapreduce.processor.reduce.ReduceProcessor
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration
import org.apache.tez.runtime.library.input.{OrderedGroupedInputLegacy, OrderedGroupedKVInput, OrderedGroupedMergedKVInput, UnorderedKVInput}
import org.apache.tez.runtime.library.output.{OrderedPartitionedKVOutput, UnorderedKVOutput, UnorderedPartitionedKVOutput}
import org.apache.tez.runtime.library.partitioner.HashPartitioner

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._

trait TezDAGSuite
  extends DAGSuite {

  //
  // base dags
  //

  def baseDag(mr3Conf: MR3Conf, numTasks: Int, name: String): DAGProto = {
    buildBaseDag(mr3Conf, numTasks, name, withInput = false, withOutput = false)
  }

  def baseDagWithInput(mr3Conf: MR3Conf, numTasks: Int, name: String): DAGProto = {
    buildBaseDag(mr3Conf, numTasks, name, withInput = true, withOutput = false)
  }

  def baseDagWithOutput(mr3Conf: MR3Conf, numTasks: Int, name: String): DAGProto = {
    buildBaseDag(mr3Conf, numTasks, name, withInput = false, withOutput = true)
  }

  def baseMRDag(mr3Conf: MR3Conf, numSrcTasks: Int, numDestTasks: Int, name: String): DAGProto = {
    buildBaseMRDag(mr3Conf, numSrcTasks, numDestTasks, name, withInput = false, withOutput = false)
  }

  private def buildBaseDag(
      mr3Conf: MR3Conf, numTasks: Int, name: String, withInput: Boolean, withOutput: Boolean): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapRootInput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapLeafOutput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        vertexManagerClassName = getMapVertexManagerClassName(withInput))

    val numInitTasks = setupEntityDescriptorMapRootInput(
        entityDescriptorMapRootInput, mr3Conf, numTasks, withInput)
    SetupEntityDescriptorMap.baseLeafOutput(entityDescriptorMapLeafOutput, mr3Conf)

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.rootInput -> entityDescriptorMapRootInput.toMap,
        EntityDescriptorMapType.leafOutput -> entityDescriptorMapLeafOutput.toMap)
    DAGBuilder.baseDag(entityDescriptorMap, mr3Conf, numInitTasks, name, withInput, withOutput)
  }

  private def buildBaseMRDag(
      mr3Conf: MR3Conf, numSrcTasks: Int, numDestTasks: Int, name: String,
      withInput: Boolean, withOutput: Boolean): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapRootInput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapLeafOutput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapEdge = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        vertexManagerClassName = getMapVertexManagerClassName(withInput))

    val numInitSrcTasks = setupEntityDescriptorMapRootInput(
        entityDescriptorMapRootInput, mr3Conf, numSrcTasks, withInput)
    SetupEntityDescriptorMap.baseLeafOutput(entityDescriptorMapLeafOutput, mr3Conf)

    SetupEntityDescriptorMap.baseRootInput(
        entityDescriptorMapRootInput, mr3Conf,
        inputInitializerPayload = getBaseInputInitializerPayload(mr3Conf, numSrcTasks, withInput))

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        vertexManagerClassName = classOf[ShuffleVertexManager].getName,
        vertexManagerPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)),
        vPos = 1)
    SetupEntityDescriptorMap.baseLeafOutput(entityDescriptorMapLeafOutput, mr3Conf)
    SetupEntityDescriptorMap.baseEdge(entityDescriptorMapEdge, mr3Conf)

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.rootInput -> entityDescriptorMapRootInput.toMap,
        EntityDescriptorMapType.leafOutput -> entityDescriptorMapLeafOutput.toMap,
        EntityDescriptorMapType.edge -> entityDescriptorMapEdge.toMap)
    DAGBuilder.baseMRDag(
        entityDescriptorMap, mr3Conf, numInitSrcTasks, numDestTasks, name, withInput, withOutput)
  }

  private def getMapVertexManagerClassName(withInput: Boolean = true): String = {
    if (withInput)
      classOf[RootInputVertexManager].getName
    else
      classOf[ImmediateStartVertexManager].getName
  }

  private def getBaseInputInitializerPayload(
      mr3Conf: MR3Conf,
      numTasks: Int = 1,
      withInput: Boolean = true): Option[UserPayloadProto] = {
    if (withInput) {
      val inputInitializerConf = mr3Conf.createHadoopConf
      inputInitializerConf.setInt(BaseDagSetup.NUM_TASKS, numTasks)
      val inputInitializerPayload = UtilsForBuilder.createUserPayloadFromConf(inputInitializerConf)
      Some(inputInitializerPayload)
    } else
      None
  }

  private def setupEntityDescriptorMapRootInput(
      entityDescriptorMap: mutable.Map[String, (String, Option[UserPayloadProto])],
      mr3Conf: MR3Conf, numTasks: Int, withInput: Boolean) = {
    if (withInput) {
      SetupEntityDescriptorMap.baseRootInput(
          entityDescriptorMap, mr3Conf,
          inputInitializerPayload = getBaseInputInitializerPayload(mr3Conf, numTasks, withInput))
      -1
    } else
      numTasks
  }

  //
  // map-only dags
  //

  def sleepDag(mr3Conf: MR3Conf, sleepTime: Duration, numTasks: Int, name: String): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[SleepProcessor].getName,
        processorPayload = getSleepTimePayload(sleepTime))

    val entityDescriptorMap = Map(EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap)
    DAGBuilder.sleepDag(entityDescriptorMap, mr3Conf, numTasks, name)
  }

  private def getSleepTimePayload(sleepTime: Duration = 0.milli): Option[UserPayloadProto] = {
    if (sleepTime > 0.milli) {
      Some(UtilsForBuilder.createUserPayloadFromByteString(ByteString.copyFromUtf8(
          sleepTime.toMillis.toString)))
    } else
      None
  }

  def dagWithRootInputVertexAndSleepVertex(mr3Conf: MR3Conf, name: String): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapRootInput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    // rootInputVertex
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        vertexManagerClassName = classOf[RootInputVertexManager].getName)
    SetupEntityDescriptorMap.baseRootInput(
        entityDescriptorMapRootInput, mr3Conf,
        inputInitializerPayload = getBaseInputInitializerPayload(mr3Conf))
    // sleepVertex
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[SleepProcessor].getName,
        processorPayload = getSleepTimePayload(100.millis),
        hPos = 1)

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.rootInput -> entityDescriptorMapRootInput.toMap)
    DAGBuilder.dagWithRootInputVertexAndSleepVertex(entityDescriptorMap, mr3Conf, name)
  }

  def dagWithBaseVertexAndBaseDaemonVertex(mr3Conf: MR3Conf, name: String): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapDaemonVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    SetupEntityDescriptorMap.baseVertex(entityDescriptorMapVertex, mr3Conf)
    SetupEntityDescriptorMap.baseDaemonVertex(entityDescriptorMapDaemonVertex, mr3Conf)

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.daemonVertex -> entityDescriptorMapDaemonVertex.toMap)
    DAGBuilder.dagWithBaseVertexAndBaseDaemonVertex(entityDescriptorMap, mr3Conf, name)
  }

  def dagWithSleepVertexAndSleepDaemonVertex(
      mr3Conf: MR3Conf, sleepTime: Duration, name: String): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapDaemonVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    // sleepVertex
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[SleepProcessor].getName,
        processorPayload = getSleepTimePayload(sleepTime))
    // sleepDaemonVertex
    SetupEntityDescriptorMap.baseDaemonVertex(
        entityDescriptorMapDaemonVertex, mr3Conf,
        daemonProcessorClassName = classOf[SleepProcessor].getName,
        daemonProcessorPayload = getSleepTimePayload(sleepTime))

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.daemonVertex -> entityDescriptorMapDaemonVertex.toMap)
    DAGBuilder.dagWithSleepVertexAndSleepDaemonVertex(entityDescriptorMap, mr3Conf, name)
  }

  def dagWithBaseVertexAndSleepDaemonVertices(
      mr3Conf: MR3Conf, numWorkerTasks: Int, sleepTimes: Seq[Duration], name: String): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapDaemonVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    SetupEntityDescriptorMap.baseVertex(entityDescriptorMapVertex, mr3Conf)
    sleepTimes.zipWithIndex.foreach{ case (sleepTime, idx) =>
      SetupEntityDescriptorMap.baseDaemonVertex(
          entityDescriptorMapDaemonVertex, mr3Conf,
          daemonProcessorClassName = classOf[SleepProcessor].getName,
          daemonProcessorPayload = getSleepTimePayload(sleepTime),
          hPos = idx)
    }

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.daemonVertex -> entityDescriptorMapDaemonVertex.toMap)
    DAGBuilder.dagWithBaseVertexAndSleepDaemonVertices(
        entityDescriptorMap, mr3Conf, numWorkerTasks, numDaemonVertices = sleepTimes.size, name)
  }

  //
  // wrong dags
  //

  def wrongDagWithDuplicateContainerGroupNames(mr3Conf: MR3Conf, name: String): DAGProto = {
    val entityDescriptorMap = buildEntityDescriptorMapVertex(mr3Conf)
    DAGBuilder.wrongDagWithDuplicateContainerGroupNames(entityDescriptorMap, mr3Conf, name)
  }

  def wrongDagWithUnknownContainerGroupNameInVertex(mr3Conf: MR3Conf, name: String): DAGProto = {
    val entityDescriptorMap = buildEntityDescriptorMapVertex(mr3Conf)
    DAGBuilder.wrongDagWithUnknownContainerGroupNameInVertex(entityDescriptorMap, mr3Conf, name)
  }

  def wrongDagWithDuplicateWorkerVertexNames(mr3Conf: MR3Conf, name: String): DAGProto = {
    val entityDescriptorMap = buildEntityDescriptorMapVertex(mr3Conf)
    DAGBuilder.wrongDagWithDuplicateWorkerVertexNames(entityDescriptorMap, mr3Conf, name)
  }

  def wrongDagWithDuplicateDaemonVertexNames(mr3Conf: MR3Conf, name: String): DAGProto = {
    val entityDescriptorMap = buildEntityDescriptorMapVertexAndDaemonVertex(mr3Conf)
    DAGBuilder.wrongDagWithDuplicateDaemonVertexNames(entityDescriptorMap, mr3Conf, name)
  }

  def wrongDagWithDuplicateVertexNames(mr3Conf: MR3Conf, name: String): DAGProto = {
    val entityDescriptorMap = buildEntityDescriptorMapVertexAndDaemonVertex(mr3Conf)
    DAGBuilder.wrongDagWithDuplicateVertexNames(entityDescriptorMap, mr3Conf, name)
  }

  def wrongDagWithDuplicateVertexGroupNames(mr3Conf: MR3Conf, name: String): DAGProto = {
    val entityDescriptorMap = buildEntityDescriptorMapVertex(mr3Conf)
    DAGBuilder.wrongDagWithDuplicateVertexGroupNames(entityDescriptorMap, mr3Conf, name)
  }

  def wrongDagWithWrongTaskLocationHints(mr3Conf: MR3Conf, name: String): DAGProto = {
    val entityDescriptorMap = buildEntityDescriptorMapVertex(mr3Conf)
    DAGBuilder.wrongDagWithWrongTaskLocationHints(entityDescriptorMap, mr3Conf, numTasks = 10, name)
  }

  def wrongDagWithInsufficientContainerResource(mr3Conf: MR3Conf, name: String): DAGProto = {
    val entityDescriptorMap = buildEntityDescriptorMapVertexAndDaemonVertex(mr3Conf)
    DAGBuilder.wrongDagWithInsufficientContainerResource(entityDescriptorMap, mr3Conf, name)
  }

  private def buildEntityDescriptorMapVertex(
      mr3Conf: MR3Conf): Map[EntityDescriptorMapType, Map[String, (String, Option[UserPayloadProto])]] = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    SetupEntityDescriptorMap.baseVertex(entityDescriptorMapVertex, mr3Conf)
    Map(EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap)
  }

  private def buildEntityDescriptorMapVertexAndDaemonVertex(
      mr3Conf: MR3Conf): Map[EntityDescriptorMapType, Map[String, (String, Option[UserPayloadProto])]] = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapDaemonVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    SetupEntityDescriptorMap.baseVertex(entityDescriptorMapVertex, mr3Conf)
    SetupEntityDescriptorMap.baseDaemonVertex(entityDescriptorMapDaemonVertex, mr3Conf)

    Map(EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.daemonVertex -> entityDescriptorMapDaemonVertex.toMap)
  }

  def dagWithNoVertex(name: String): DAGProto = {
    DAGBuilder.dagWithNoVertex(name)
  }

  def dagForRecovery(name: String, mr3Conf: MR3Conf): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[RecoveryProcessor].getName,
        processorPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)),
        vertexManagerClassName = getMapVertexManagerClassName(withInput = false))

    val entityDescriptorMap = Map(EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap)
    DAGBuilder.baseDag(
        entityDescriptorMap, mr3Conf, numTasks = 1, name, withInput = false, withOutput = false)
  }

  //
  // map-reduce dags
  //

  def mrDagWithMixingResourceScheduler(mr3Conf: MR3Conf, numLocalTasks: Int, numYarnTasks: Int, name: String): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapEdge = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    SetupEntityDescriptorMap.baseVertex(entityDescriptorMapVertex, mr3Conf)
    SetupEntityDescriptorMap.baseEdge(
        entityDescriptorMapEdge, mr3Conf,
        srcVertexLogicalOutputClassName = classOf[LogicalOutputSendingPid].getName,
        destVertexLogicalInputClassName = classOf[LogicalInputWaitingForPid].getName,
        destVertexLogicalInputPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)))

    SetupEntityDescriptorMap.baseVertex(entityDescriptorMapVertex, mr3Conf, hPos = 1)
    SetupEntityDescriptorMap.baseEdge(
        entityDescriptorMapEdge, mr3Conf,
        srcVertexLogicalOutputClassName = classOf[LogicalOutputSendingPid].getName,
        destVertexLogicalInputClassName = classOf[LogicalInputWaitingForPid].getName,
        destVertexLogicalInputPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)),
        hPos = 1)

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        vertexManagerClassName = classOf[ShuffleVertexManager].getName,
        vertexManagerPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)),
        vPos = 1)

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.edge -> entityDescriptorMapEdge.toMap)
    DAGBuilder.mrDagWithMixingResourceScheduler(entityDescriptorMap, mr3Conf, numLocalTasks, numYarnTasks, name)
  }

  def mrDagWithVertexGroup(mr3Conf: MR3Conf, numMapVerticesInGroup: Int, name: String): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapEdge = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapMergedInputEdge = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    val edgePayload = getEdgePayload(
      mr3Conf, classOf[Text].getName, classOf[IntWritable].getName)
    (0 until numMapVerticesInGroup).foreach{ idx =>
      SetupEntityDescriptorMap.baseVertex(entityDescriptorMapVertex, mr3Conf, hPos = idx)
      SetupEntityDescriptorMap.baseEdge(entityDescriptorMapEdge, mr3Conf,
          srcVertexLogicalOutputClassName = classOf[OrderedPartitionedKVOutput].getName,
          srcVertexLogicalOutputPayload = Some(edgePayload),
          destVertexLogicalInputClassName = classOf[OrderedGroupedKVInput].getName,
          destVertexLogicalInputPayload = Some(edgePayload),
          hPos = idx)
    }
    SetupEntityDescriptorMap.baseMergedInputEdge(entityDescriptorMapMergedInputEdge, mr3Conf)
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        vertexManagerClassName = classOf[ShuffleVertexManager].getName,
        vertexManagerPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)),
        vPos = 1)

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.edge -> entityDescriptorMapEdge.toMap,
        EntityDescriptorMapType.mergedInputEdge -> entityDescriptorMapMergedInputEdge.toMap)
    DAGBuilder.mrDagWithVertexGroup(
        entityDescriptorMap, mr3Conf, numMapVerticesInGroup, numSrcTasks = 1,
        withInput = false, withOutput = false, name)
  }

  //
  // dags only for tez
  //

  def dagForTestingInputInitializerEvent(
      mr3Conf: MR3Conf,
      name: String = "dagForTestingInputInitializerEvent"): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapRootInput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    // vertexHandlingInputInitializerEvent
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        vertexManagerClassName = classOf[RootInputVertexManager].getName)
    SetupEntityDescriptorMap.baseRootInput(
        entityDescriptorMapRootInput, mr3Conf,
        inputInitializerClassName = classOf[InputInitializerWaitingForInputInitializerEvent].getName,
        inputInitializerPayload = getBaseInputInitializerPayload(mr3Conf))
    // vertexSendingInputInitializerEvent
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[ProcessorSendingInputInitializerEvent].getName,
        hPos = 1)

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.rootInput -> entityDescriptorMapRootInput.toMap)
    DAGBuilder.dagWithRootInputVertexAndSleepVertex(entityDescriptorMap, mr3Conf, name)
  }

  def dagForTestingInputDataInformationEvent(
      mr3Conf: MR3Conf,
      name: String = "dagForTestingInputDataInformationEvent"): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapRootInput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        vertexManagerClassName = classOf[RootInputVertexManager].getName)
    SetupEntityDescriptorMap.baseRootInput(
        entityDescriptorMapRootInput, mr3Conf,
        logicalInputRootClassName = classOf[LogicalInputWaitingForInputDataInformationEvent].getName,
        inputInitializerClassName = classOf[InputInitializerSendingInputDataInformationEvent].getName,
        inputInitializerPayload = getBaseInputInitializerPayload(mr3Conf))

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.rootInput -> entityDescriptorMapRootInput.toMap)
    DAGBuilder.baseDag(
        entityDescriptorMap, mr3Conf, numTasks = -1, name, withInput = true, withOutput = false)
  }

  def dagForTestingVertexManagerEvent(
      mr3Conf: MR3Conf,
      name: String = "dagForTestingVertexManagerEvent"): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapRootInput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    // vertexHandlingVertexManagerEvent
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        vertexManagerClassName = classOf[RootInputVertexManager].getName)
    SetupEntityDescriptorMap.baseRootInput(
        entityDescriptorMapRootInput, mr3Conf,
        inputInitializerPayload = getBaseInputInitializerPayload(mr3Conf))
    // vertexSendingVertexManagerEvent
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[ProcessorSendingVertexManagerEvent].getName,
        hPos = 1)

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.rootInput -> entityDescriptorMapRootInput.toMap)
    DAGBuilder.dagWithRootInputVertexAndSleepVertex(entityDescriptorMap, mr3Conf, name)
  }

  //
  // dags for examples
  //

  def noOpFailDag(
      mr3Conf: MR3Conf,
      numFails: Int,
      name: String = "noOpFailDag"): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val numFailsPayload =
      Some(UtilsForBuilder.createUserPayloadFromByteString(ByteString.copyFromUtf8(numFails.toString)))
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[NoOpFailProcessor].getName,
        processorPayload = numFailsPayload)

    val entityDescriptorMap = Map(EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap)
    DAGBuilder.noOpFailDag(entityDescriptorMap, mr3Conf, name)
  }

  def readWriteDag(
      mr3Conf: MR3Conf, input: String, output: String, name: String): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapRootInput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapLeafOutput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    setupEntityDescriptorMapForTokenVertex(
        entityDescriptorMapVertex,
        entityDescriptorMapRootInput,
        entityDescriptorMapLeafOutput,
        mr3Conf, input, output)

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.rootInput -> entityDescriptorMapRootInput.toMap,
        EntityDescriptorMapType.leafOutput -> entityDescriptorMapLeafOutput.toMap)
    DAGBuilder.readWriteDag(entityDescriptorMap, mr3Conf, name)
  }

  def wordCountDag(
      mr3Conf: MR3Conf, input: String, output: String, numReducers: Int, name: String,
      isAllInOneScheme: Boolean): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapRootInput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapLeafOutput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapEdge = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    setupEntityDescriptorMapForTokenVertex(
        entityDescriptorMapVertex,
        entityDescriptorMapRootInput,
        entityDescriptorMapLeafOutput,
        mr3Conf, input, output)
    setupEntityDescriptorMapForSumVertex(
        entityDescriptorMapEdge, entityDescriptorMapVertex, mr3Conf,
        classOf[SummationProcessor].getName)

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.rootInput -> entityDescriptorMapRootInput.toMap,
        EntityDescriptorMapType.leafOutput -> entityDescriptorMapLeafOutput.toMap,
        EntityDescriptorMapType.edge -> entityDescriptorMapEdge.toMap)
    DAGBuilder.wordCountDag(entityDescriptorMap, mr3Conf, numReducers, name, isAllInOneScheme)
  }

  def orderedWordCountDag(
      mr3Conf: MR3Conf, input: String, output: String, numReducers: Int, name: String,
      isAllInOneScheme: Boolean): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapRootInput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapLeafOutput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapEdge = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    setupEntityDescriptorMapForTokenVertex(
        entityDescriptorMapVertex,
        entityDescriptorMapRootInput,
        entityDescriptorMapLeafOutput,
        mr3Conf, input, output)
    setupEntityDescriptorMapForSumVertex(
        entityDescriptorMapEdge, entityDescriptorMapVertex, mr3Conf,
        classOf[SummationForSorterProcessor].getName)
    setupEntityDescriptorMapForSorterVertex(
        entityDescriptorMapEdge, entityDescriptorMapVertex, mr3Conf)

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.rootInput -> entityDescriptorMapRootInput.toMap,
        EntityDescriptorMapType.leafOutput -> entityDescriptorMapLeafOutput.toMap,
        EntityDescriptorMapType.edge -> entityDescriptorMapEdge.toMap)
    DAGBuilder.orderedWordCountDag(entityDescriptorMap, mr3Conf, numReducers, name, isAllInOneScheme)
  }

  def unionDag(mr3Conf: MR3Conf, inputs: Seq[String], output: String, name: String): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapRootInput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapLeafOutput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapEdge = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapMergedInputEdge = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    val tokenToSumEdgePayload = getEdgePayload(
        mr3Conf, classOf[Text].getName, classOf[IntWritable].getName)
    inputs.zipWithIndex.foreach{ case (input, idx) =>
      setupEntityDescriptorMapForTokenVertex(
          entityDescriptorMapVertex,
          entityDescriptorMapRootInput,
          entityDescriptorMapLeafOutput,
          mr3Conf, input, output, hPos = idx)
      SetupEntityDescriptorMap.baseEdge(
          entityDescriptorMapEdge, mr3Conf,
          srcVertexLogicalOutputClassName = classOf[OrderedPartitionedKVOutput].getName,
          srcVertexLogicalOutputPayload = Some(tokenToSumEdgePayload),
          destVertexLogicalInputClassName = classOf[OrderedGroupedKVInput].getName,
          destVertexLogicalInputPayload = Some(tokenToSumEdgePayload),
          hPos = idx)
    }

    SetupEntityDescriptorMap.baseMergedInputEdge(entityDescriptorMapMergedInputEdge, mr3Conf)
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[SummationProcessor].getName,
        vertexManagerClassName = classOf[ShuffleVertexManager].getName,
        vertexManagerPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)),
        vPos = 1)

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.rootInput -> entityDescriptorMapRootInput.toMap,
        EntityDescriptorMapType.leafOutput -> entityDescriptorMapLeafOutput.toMap,
        EntityDescriptorMapType.edge -> entityDescriptorMapEdge.toMap,
        EntityDescriptorMapType.mergedInputEdge -> entityDescriptorMapMergedInputEdge.toMap)
    DAGBuilder.mrDagWithVertexGroup(
        entityDescriptorMap, mr3Conf, inputs.size, numSrcTasks = -1,
        withInput = true, withOutput = true, name)
  }

  private def setupEntityDescriptorMapForTokenVertex(
      entityDescriptorMapVertex: mutable.Map[String, (String, Option[UserPayloadProto])],
      entityDescriptorMapRootInput: mutable.Map[String, (String, Option[UserPayloadProto])],
      entityDescriptorMapLeafOutput: mutable.Map[String, (String, Option[UserPayloadProto])],
      mr3Conf: MR3Conf, input: String, output: String,
      hPos: Int = 0): Unit = {
    val inputPayload = getRootInputPayload(mr3Conf, input)
    val outputPayload = getLeafOutputPayload(mr3Conf, output)

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[TokenizerProcessor].getName,
        vertexManagerClassName = classOf[RootInputVertexManager].getName,
        hPos = hPos)
    SetupEntityDescriptorMap.baseRootInput(
        entityDescriptorMapRootInput, mr3Conf,
        logicalInputRootClassName = classOf[MRInput].getName,
        logicalInputRootPayload = Some(inputPayload),
        inputInitializerClassName = classOf[MRInputAMSplitGenerator].getName,
        hPos = hPos)
    SetupEntityDescriptorMap.baseLeafOutput(
        entityDescriptorMapLeafOutput, mr3Conf,
        logicalOutputLeafClassName = classOf[MROutput].getName,
        logicalOutputRootPayload = Some(outputPayload),
        outputCommitterClassName = classOf[MROutputCommitter].getName,
        hPos = hPos)
  }

  private def setupEntityDescriptorMapForSumVertex(
      entityDescriptorMapEdge: mutable.Map[String, (String, Option[UserPayloadProto])],
      entityDescriptorMapVertex: mutable.Map[String, (String, Option[UserPayloadProto])],
      mr3Conf: MR3Conf, processorClassName: String): Unit = {
    val tokenToSumEdgePayload = getEdgePayload(
        mr3Conf, classOf[Text].getName, classOf[IntWritable].getName)
    SetupEntityDescriptorMap.baseEdge(
        entityDescriptorMapEdge, mr3Conf,
        srcVertexLogicalOutputClassName = classOf[OrderedPartitionedKVOutput].getName,
        srcVertexLogicalOutputPayload = Some(tokenToSumEdgePayload),
        destVertexLogicalInputClassName = classOf[OrderedGroupedKVInput].getName,
        destVertexLogicalInputPayload = Some(tokenToSumEdgePayload))
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = processorClassName,
        vertexManagerClassName = classOf[ShuffleVertexManager].getName,
        vertexManagerPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)),
        vPos = 1)
  }

  private def setupEntityDescriptorMapForSorterVertex(
      entityDescriptorMapEdge: mutable.Map[String, (String, Option[UserPayloadProto])],
      entityDescriptorMapVertex: mutable.Map[String, (String, Option[UserPayloadProto])],
      mr3Conf: MR3Conf): Unit = {
    val sumToSorterEdgePayload = getEdgePayload(
        mr3Conf, classOf[IntWritable].getName, classOf[Text].getName)
    SetupEntityDescriptorMap.baseEdge(
        entityDescriptorMapEdge, mr3Conf,
        srcVertexLogicalOutputClassName = classOf[OrderedPartitionedKVOutput].getName,
        srcVertexLogicalOutputPayload = Some(sumToSorterEdgePayload),
        destVertexLogicalInputClassName = classOf[OrderedGroupedKVInput].getName,
        destVertexLogicalInputPayload = Some(sumToSorterEdgePayload),
        vPos = 1)
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[SorterProcessor].getName,
        vertexManagerClassName = classOf[ShuffleVertexManager].getName,
        vertexManagerPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)),
        vPos = 2)
  }

  private def getRootInputPayload(
      mr3Conf: MR3Conf, input: String,
      keyClassName: String = classOf[LongWritable].getName,
      valueClassName: String = classOf[Text].getName,
      inputFormatClassName: String = classOf[TextInputFormat].getName,
      useInputInitializer: Boolean = true): UserPayloadProto = {
    val inputConf = mr3Conf.createHadoopConf
    inputConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, keyClassName)
    inputConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, valueClassName)
    inputConf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, inputFormatClassName)
    inputConf.set(FileInputFormat.INPUT_DIR, input)
    inputConf.setBoolean(MRJobConfig.MR_TEZ_SPLITS_VIA_EVENTS, useInputInitializer)
    val mrInputPayload = MRInputUserPayloadProto.newBuilder
      .setConfigurationBytes(UtilsForBuilder.createByteStringFromConf(inputConf))
      .setGroupingEnabled(false)
      .build
    val inputPayload = UtilsForBuilder.createUserPayloadFromByteString(mrInputPayload.toByteString)
    inputPayload
  }

  private def getLeafOutputPayload(
      mr3Conf: MR3Conf, output: String,
      outputFormatClassName: String = classOf[TextOutputFormat[_, _]].getName): UserPayloadProto = {
    val outputConf = mr3Conf.createHadoopConf
    outputConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, outputFormatClassName)
    outputConf.set(FileOutputFormat.OUTDIR, output)
    val outputPayload = UtilsForBuilder.createUserPayloadFromConf(outputConf)
    outputPayload
  }

  private def getEdgePayload(
      mr3Conf: MR3Conf, keyClassName: String, valueClassname: String,
      partitionerClassName: String = classOf[HashPartitioner].getName): UserPayloadProto = {
    val edgeConf = mr3Conf.createHadoopConf
    edgeConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, keyClassName)
    edgeConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, valueClassname)
    edgeConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, partitionerClassName)
    val edgePayload = UtilsForBuilder.createUserPayloadFromConf(edgeConf)
    edgePayload
  }

  // TODO: refactor join jobs

  def joinDataGenDag(
      mr3Conf: MR3Conf,
      largeOutPath: String, largeOutSize: Long, smallOutPath: String, smallOutSize: Long,
      expectedOutPath: String, numTasks: Int, name: String): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapLeafOutput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    val largeOutSizePerReducer = largeOutSize / numTasks
    val smallOutSizePerReducer = smallOutSize / numTasks
    val processorPayload = UtilsForBuilder.createUserPayloadFromByteString(ByteString.copyFrom(
        GenDataProcessor.createConfiguration(largeOutSizePerReducer, smallOutSizePerReducer)))
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[GenDataProcessor].getName,
        processorPayload = Some(processorPayload))

    Seq(largeOutPath, smallOutPath, expectedOutPath).zipWithIndex.foreach{ case (path, idx) =>
      val outputPayload = getLeafOutputPayload(mr3Conf, path)
      SetupEntityDescriptorMap.baseLeafOutput(
          entityDescriptorMapLeafOutput, mr3Conf,
          logicalOutputLeafClassName = classOf[MROutput].getName,
          logicalOutputRootPayload = Some(outputPayload),
          outputCommitterClassName = classOf[MROutputCommitter].getName,
          hPos = idx)
    }

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.leafOutput -> entityDescriptorMapLeafOutput.toMap)
    DAGBuilder.joinDataGenDag(entityDescriptorMap, mr3Conf, numTasks = numTasks, name)
  }

  def hashJoinDag(
      mr3Conf: MR3Conf,
      streamInputPath: String, hashInputPath: String, outPath: String,
      numReducers: Int, doBroadcast: Boolean, name: String): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapRootInput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapLeafOutput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapEdge = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[ForwardingProcessor].getName,
        vertexManagerClassName = classOf[RootInputVertexManager].getName)
    val streamInputPayload = getRootInputPayload(mr3Conf, streamInputPath)
    SetupEntityDescriptorMap.baseRootInput(
        entityDescriptorMapRootInput, mr3Conf,
        logicalInputRootClassName = classOf[MRInput].getName,
        logicalInputRootPayload = Some(streamInputPayload),
        inputInitializerClassName = classOf[MRInputAMSplitGenerator].getName)
    val streamEdgePayload = getEdgePayload(
        mr3Conf, classOf[Text].getName, classOf[NullWritable].getName)
    SetupEntityDescriptorMap.baseEdge(
        entityDescriptorMapEdge, mr3Conf,
        srcVertexLogicalOutputClassName = classOf[UnorderedPartitionedKVOutput].getName,
        srcVertexLogicalOutputPayload = Some(streamEdgePayload),
        destVertexLogicalInputClassName = classOf[UnorderedKVInput].getName,
        destVertexLogicalInputPayload = Some(streamEdgePayload))

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[ForwardingProcessor].getName,
        vertexManagerClassName = classOf[RootInputVertexManager].getName, hPos = 1)
    val hashInputPayload = getRootInputPayload(mr3Conf, hashInputPath)
    SetupEntityDescriptorMap.baseRootInput(
        entityDescriptorMapRootInput, mr3Conf,
        logicalInputRootClassName = classOf[MRInput].getName,
        logicalInputRootPayload = Some(hashInputPayload),
        inputInitializerClassName = classOf[MRInputAMSplitGenerator].getName, hPos = 1)
    val hashEdgePayload = getEdgePayload(
        mr3Conf, classOf[Text].getName, classOf[NullWritable].getName)
    val srcVertexLogicalOutputClassName =
      if (doBroadcast) classOf[UnorderedKVOutput].getName
      else classOf[UnorderedPartitionedKVOutput].getName
    SetupEntityDescriptorMap.baseEdge(
        entityDescriptorMapEdge, mr3Conf,
        srcVertexLogicalOutputClassName = srcVertexLogicalOutputClassName,
        srcVertexLogicalOutputPayload = Some(hashEdgePayload),
        destVertexLogicalInputClassName = classOf[UnorderedKVInput].getName,
        destVertexLogicalInputPayload = Some(hashEdgePayload), hPos = 1)

    val outputPayload = getLeafOutputPayload(mr3Conf, outPath)
    SetupEntityDescriptorMap.baseLeafOutput(
        entityDescriptorMapLeafOutput, mr3Conf,
        logicalOutputLeafClassName = classOf[MROutput].getName,
        logicalOutputRootPayload = Some(outputPayload),
        outputCommitterClassName = classOf[MROutputCommitter].getName)

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[HashJoinProcessor].getName,
        vertexManagerClassName = classOf[ShuffleVertexManager].getName,
        vertexManagerPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)),
        vPos = 1)

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.rootInput -> entityDescriptorMapRootInput.toMap,
        EntityDescriptorMapType.leafOutput -> entityDescriptorMapLeafOutput.toMap,
        EntityDescriptorMapType.edge -> entityDescriptorMapEdge.toMap)
    DAGBuilder.hashJoinDag(entityDescriptorMap, mr3Conf, numReducers, doBroadcast, name)
  }

  def sortMergeJoinDag(
      mr3Conf: MR3Conf,
      streamInputPath: String, hashInputPath: String, outPath: String,
      numReducers: Int, name: String): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapRootInput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapLeafOutput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapEdge = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[ForwardingProcessor].getName,
        vertexManagerClassName = classOf[RootInputVertexManager].getName)
    val streamInputPayload = getRootInputPayload(mr3Conf, streamInputPath)
    SetupEntityDescriptorMap.baseRootInput(
        entityDescriptorMapRootInput, mr3Conf,
        logicalInputRootClassName = classOf[MRInput].getName,
        logicalInputRootPayload = Some(streamInputPayload),
        inputInitializerClassName = classOf[MRInputAMSplitGenerator].getName)
    val streamEdgePayload = getEdgePayload(
        mr3Conf, classOf[Text].getName, classOf[NullWritable].getName)
    SetupEntityDescriptorMap.baseEdge(
        entityDescriptorMapEdge, mr3Conf,
        srcVertexLogicalOutputClassName = classOf[OrderedPartitionedKVOutput].getName,
        srcVertexLogicalOutputPayload = Some(streamEdgePayload),
        destVertexLogicalInputClassName = classOf[OrderedGroupedKVInput].getName,
        destVertexLogicalInputPayload = Some(streamEdgePayload))

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[ForwardingProcessor].getName,
        vertexManagerClassName = classOf[RootInputVertexManager].getName, hPos = 1)
    val hashInputPayload = getRootInputPayload(mr3Conf, hashInputPath)
    SetupEntityDescriptorMap.baseRootInput(
        entityDescriptorMapRootInput, mr3Conf,
        logicalInputRootClassName = classOf[MRInput].getName,
        logicalInputRootPayload = Some(hashInputPayload),
        inputInitializerClassName = classOf[MRInputAMSplitGenerator].getName, hPos = 1)
    val hashEdgePayload = getEdgePayload(
        mr3Conf, classOf[Text].getName, classOf[NullWritable].getName)
    SetupEntityDescriptorMap.baseEdge(
        entityDescriptorMapEdge, mr3Conf,
        srcVertexLogicalOutputClassName = classOf[OrderedPartitionedKVOutput].getName,
        srcVertexLogicalOutputPayload = Some(hashEdgePayload),
        destVertexLogicalInputClassName = classOf[OrderedGroupedKVInput].getName,
        destVertexLogicalInputPayload = Some(hashEdgePayload), hPos = 1)

    val outputPayload = getLeafOutputPayload(mr3Conf, outPath)
    SetupEntityDescriptorMap.baseLeafOutput(
        entityDescriptorMapLeafOutput, mr3Conf,
        logicalOutputLeafClassName = classOf[MROutput].getName,
        logicalOutputRootPayload = Some(outputPayload),
        outputCommitterClassName = classOf[MROutputCommitter].getName)

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[SortMergeJoinProcessor].getName,
        vertexManagerClassName = classOf[ShuffleVertexManager].getName,
        vertexManagerPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)),
        vPos = 1)

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.rootInput -> entityDescriptorMapRootInput.toMap,
        EntityDescriptorMapType.leafOutput -> entityDescriptorMapLeafOutput.toMap,
        EntityDescriptorMapType.edge -> entityDescriptorMapEdge.toMap)
    DAGBuilder.sortMergeDag(entityDescriptorMap, mr3Conf, numReducers, name)
  }

  def joinValidateDag(
      mr3Conf: MR3Conf,
      expectedOutPath: String, outPath: String,
      numReducers: Int, name: String): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapRootInput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapEdge = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[ForwardingProcessor].getName,
        vertexManagerClassName = classOf[RootInputVertexManager].getName)
    val streamInputPayload = getRootInputPayload(mr3Conf, expectedOutPath)
    SetupEntityDescriptorMap.baseRootInput(
        entityDescriptorMapRootInput, mr3Conf,
        logicalInputRootClassName = classOf[MRInput].getName,
        logicalInputRootPayload = Some(streamInputPayload),
        inputInitializerClassName = classOf[MRInputAMSplitGenerator].getName)
    val streamEdgePayload = getEdgePayload(
        mr3Conf, classOf[Text].getName, classOf[NullWritable].getName)
    SetupEntityDescriptorMap.baseEdge(
        entityDescriptorMapEdge, mr3Conf,
        srcVertexLogicalOutputClassName = classOf[OrderedPartitionedKVOutput].getName,
        srcVertexLogicalOutputPayload = Some(streamEdgePayload),
        destVertexLogicalInputClassName = classOf[OrderedGroupedKVInput].getName,
        destVertexLogicalInputPayload = Some(streamEdgePayload))

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[ForwardingProcessor].getName,
        vertexManagerClassName = classOf[RootInputVertexManager].getName, hPos = 1)
    val hashInputPayload = getRootInputPayload(mr3Conf, outPath)
    SetupEntityDescriptorMap.baseRootInput(
        entityDescriptorMapRootInput, mr3Conf,
        logicalInputRootClassName = classOf[MRInput].getName,
        logicalInputRootPayload = Some(hashInputPayload),
        inputInitializerClassName = classOf[MRInputAMSplitGenerator].getName, hPos = 1)
    val hashEdgePayload = getEdgePayload(
        mr3Conf, classOf[Text].getName, classOf[NullWritable].getName)
    SetupEntityDescriptorMap.baseEdge(
        entityDescriptorMapEdge, mr3Conf,
        srcVertexLogicalOutputClassName = classOf[OrderedPartitionedKVOutput].getName,
        srcVertexLogicalOutputPayload = Some(hashEdgePayload),
        destVertexLogicalInputClassName = classOf[OrderedGroupedKVInput].getName,
        destVertexLogicalInputPayload = Some(hashEdgePayload), hPos = 1)

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[JoinValidateProcessor].getName,
        vertexManagerClassName = classOf[ShuffleVertexManager].getName,
        vertexManagerPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)),
        vPos = 1)

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.rootInput -> entityDescriptorMapRootInput.toMap,
        EntityDescriptorMapType.edge -> entityDescriptorMapEdge.toMap)
    DAGBuilder.joinValidateDag(entityDescriptorMap, mr3Conf, numReducers, name)
  }

  //
  // terasort
  //

  def teraSortDag(
      srcMr3Conf: MR3Conf,
      input: String, output: String, numReducers: Int, useInputInitializer: Boolean,
      name: String): DAGProto = {
    val localResourcesPath = s"$output-lrs"
    val (numMappers, locationHints, localResources, mr3Conf) =
        setupTeraSortInputs(input, output, localResourcesPath, numReducers, useInputInitializer, srcMr3Conf)

    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapRootInput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapLeafOutput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapEdge = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    val inputPayload = getRootInputPayload(
        mr3Conf, input,
        keyClassName = classOf[Text].getName,
        valueClassName = classOf[Text].getName,
        inputFormatClassName = classOf[TeraInputFormat].getName,
        useInputInitializer = useInputInitializer)
    val outputPayload = getLeafOutputPayload(
        mr3Conf, output,
        outputFormatClassName = classOf[TeraOutputFormat].getName)

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[MapProcessor].getName,
        processorPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)),
        vertexManagerClassName = getMapVertexManagerClassName(useInputInitializer))
    SetupEntityDescriptorMap.baseRootInput(
        entityDescriptorMapRootInput, mr3Conf,
        logicalInputRootClassName = classOf[MRInputLegacy].getName,
        logicalInputRootPayload = Some(inputPayload),
        inputInitializerClassName = classOf[MRInputAMSplitGenerator].getName)
    SetupEntityDescriptorMap.baseLeafOutput(
        entityDescriptorMapLeafOutput, mr3Conf,
        logicalOutputLeafClassName = classOf[MROutputLegacy].getName,
        logicalOutputRootPayload = Some(outputPayload),
        outputCommitterClassName = classOf[MROutputCommitter].getName)

    val edgePayload = getEdgePayload(
        mr3Conf, classOf[Text].getName, classOf[Text].getName, classOf[MRPartitioner].getName)
    SetupEntityDescriptorMap.baseEdge(
        entityDescriptorMapEdge, mr3Conf,
        srcVertexLogicalOutputClassName = classOf[OrderedPartitionedKVOutput].getName,
        srcVertexLogicalOutputPayload = Some(edgePayload),
        destVertexLogicalInputClassName = classOf[OrderedGroupedInputLegacy].getName,
        destVertexLogicalInputPayload = Some(edgePayload))
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[ReduceProcessor].getName,
        processorPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)),
        vertexManagerClassName = classOf[ShuffleVertexManager].getName,
        vertexManagerPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)),
        vPos = 1)

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.rootInput -> entityDescriptorMapRootInput.toMap,
        EntityDescriptorMapType.leafOutput -> entityDescriptorMapLeafOutput.toMap,
        EntityDescriptorMapType.edge -> entityDescriptorMapEdge.toMap)
    DAGBuilder.teraSortDag(
        entityDescriptorMap, mr3Conf, numMappers, numReducers, localResources,
        useInputInitializer, locationHints, name)
  }

  private def setupTeraSortInputs(
      input: String, output: String, localResourcesPath: String, numReducers: Int,
      useInputInitializer: Boolean, srcMr3Conf: MR3Conf)
    : (Int, Seq[TaskLocationHint], Map[String, LocalResource], MR3Conf) = {
    val mr3ConfBuilder = new MR3ConfBuilder(false)
      .addResource(new JobConf)
      .addResource(srcMr3Conf.toHadoopConf)
      .set(FileInputFormat.INPUT_DIR, input)
      .setInt(MRJobConfig.NUM_REDUCES, numReducers)
      .set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, classOf[Text].getName)
      .set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, classOf[Text].getName)
      .set(MRJobConfig.PARTITIONER_CLASS_ATTR, classOf[TeraSortTotalOrderPartitioner].getName)
      .setInt("dfs.replication", 1)

    // In AMLocalThread mode, DAGAppMaster's PWD is MR3Client's PWD, not DAGAppMaster's workingDir.
    // job.splitmetainfo is localized to workingDir, MRInput will read job.splitmetainfo from
    // TASK_LOCAL_RESOURCE_DIR, if TASK_LOCAL_RESOURCE_DIR is not set, MRInput will try to read from PWD,
    // which is only safe in cluster mode because PWD == workingDir in cluster mode.
    // In local mode, TASK_LOCAL_RESOURCE_DIR should be set to the directory of LocalResources.
    if (srcMr3Conf.getMasterMode == MR3Constants.MR3_MASTER_MODE_LOCAL_THREAD) {
      mr3ConfBuilder.set(MRFrameworkConfigs.TASK_LOCAL_RESOURCE_DIR, localResourcesPath)
    }

    val mr3Conf = mr3ConfBuilder.build
    val conf = mr3Conf.toHadoopConf

    // generate _partition.lst
    val job = Job.getInstance(conf)
    TeraInputFormat.writePartitionFile(job, new Path(localResourcesPath, TezUtilsForBuilder.PARTITION_FILENAME))

    val fs = FileSystem.get(conf)
    val partitionLR = Utils.createLocalResource(
        fs, new Path(localResourcesPath, TezUtilsForBuilder.PARTITION_FILENAME),
        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION)
    val partitionLRs = Map(TezUtilsForBuilder.PARTITION_FILENAME -> partitionLR)

    val (numMappers, locationHints, localResources) =
      if (useInputInitializer) {
        (-1, Seq.empty, partitionLRs)
      } else {
        // generate job.split, job.splitmetainfo
        val inputFormat: InputFormat[_, _] = ReflectionUtils.newInstance(classOf[TeraInputFormat], conf)
        val splits = inputFormat.getSplits(job)
        val splitsArr = splits.toArray(new Array[InputSplit](splits.size))
        java.util.Arrays.sort(splitsArr, new TeraSortSplitComparator)
        JobSplitWriter.createSplitFiles(new Path(localResourcesPath), conf, fs, splitsArr)

        val splitsInfo = SplitMetaInfoReader.readSplitMetaInfo(
            job.getJobID, fs, conf, new Path(localResourcesPath))
        val locationHints = splitsInfo map { split =>
          TaskLocationHintHostRack(split.getLocations.toSeq, Seq.empty, anyHost = true)
        }

        val jobSplitMetaInfoLR = Utils.createLocalResource(
            fs, new Path(localResourcesPath, MRJobConfig.JOB_SPLIT_METAINFO),
            LocalResourceType.FILE, LocalResourceVisibility.APPLICATION)
        val jobSplitLR = Utils.createLocalResource(
            fs, new Path(localResourcesPath, MRJobConfig.JOB_SPLIT),
            LocalResourceType.FILE, LocalResourceVisibility.APPLICATION)
        val jobSplitLRs = Map(
            MRJobConfig.JOB_SPLIT_METAINFO -> jobSplitMetaInfoLR, MRJobConfig.JOB_SPLIT -> jobSplitLR)

        (splitsArr.length, locationHints.toSeq, partitionLRs ++ jobSplitLRs)
      }
    (numMappers, locationHints, localResources, mr3Conf)
  }

  def teraGenDag(
      mr3Conf: MR3Conf,
      numTotalRecords: Int, teraGenPath: String,
      name: String = "teraGenDag"): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapRootInput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapLeafOutput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    val inputConf = mr3Conf.createHadoopConf
    inputConf.setLong(TeraSortConfigKeys.NUM_ROWS.key, numTotalRecords)
    inputConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, classOf[LongWritable].getName)
    inputConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, classOf[NullWritable].getName)
    inputConf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, classOf[TeraGen].getName + "$RangeInputFormat")
    inputConf.setBoolean(MRJobConfig.MR_TEZ_SPLITS_VIA_EVENTS, true)
    val mrInputPayload = MRInputUserPayloadProto.newBuilder
      .setConfigurationBytes(UtilsForBuilder.createByteStringFromConf(inputConf))
      .setGroupingEnabled(false)
      .build
    val inputPayload = UtilsForBuilder.createUserPayloadFromByteString(mrInputPayload.toByteString)

    val outputConf = mr3Conf.createHadoopConf
    outputConf.set(FileOutputFormat.OUTDIR, teraGenPath)
    outputConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, classOf[TeraOutputFormat].getName)
    outputConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, classOf[Text].getName)
    outputConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, classOf[Text].getName)
    val outputPayload = UtilsForBuilder.createUserPayloadFromConf(outputConf)

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[TeraGenProcessor].getName,
        vertexManagerClassName = classOf[RootInputVertexManager].getName)
    SetupEntityDescriptorMap.baseRootInput(
        entityDescriptorMapRootInput, mr3Conf,
        logicalInputRootClassName = classOf[MRInput].getName,
        logicalInputRootPayload = Some(inputPayload),
        inputInitializerClassName = classOf[MRInputAMSplitGenerator].getName)
    SetupEntityDescriptorMap.baseLeafOutput(
        entityDescriptorMapLeafOutput, mr3Conf,
        logicalOutputLeafClassName = classOf[MROutput].getName,
        logicalOutputRootPayload = Some(outputPayload),
        outputCommitterClassName = classOf[MROutputCommitter].getName)

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.rootInput -> entityDescriptorMapRootInput.toMap,
        EntityDescriptorMapType.leafOutput -> entityDescriptorMapLeafOutput.toMap)
    DAGBuilder.readWriteDag(entityDescriptorMap, mr3Conf, name, addLeafOutput = true)
  }

  def crossDagReuseWithLocalResources(
      mr3Conf: MR3Conf, dagLR: (String, LocalResource), dagCredentials: Credentials,
      name: String): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    val tokenRenewalInterval = TokenRenewer.getHdfsTokenRenewalInterval(mr3Conf)
    val numRounds = 3
    val sleepTimePerRound = (tokenRenewalInterval.get / (numRounds - 1)) max 100.millis

    val workerConf = mr3Conf.add(Seq(
        (CrossDagReuseWithLocalResourcesSetup.NUM_ACCESSING_HDFS_ROUNDS, (numRounds * 2).toString),
        (CrossDagReuseWithLocalResourcesSetup.SLEEP_PER_ROUND_MS, sleepTimePerRound.toMillis.toString)
    ))
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[SleepAndAccessingHdfsProcessor].getName,
        processorPayload = Some(UtilsForBuilder.createUserPayloadFromConf(workerConf)))

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap)
    DAGBuilder.crossDagReuseWithLocalResources(entityDescriptorMap, mr3Conf, dagLR, dagCredentials, name)
  }

  def simulateMRRExtendedDag(mr3Conf: MR3Conf, name: String): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapRootInput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapLeafOutput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapEdge = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapDaemonVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    val height = mr3Conf.get(SimulateMRRExtendedSetup.HEIGHT).toInt
    val inputDegree = mr3Conf.get(SimulateMRRExtendedSetup.INPUT_DEGREE).toInt
    val containerGroupType = mr3Conf.get(SimulateMRRExtendedSetup.CONTAINERGROUP_TYPE)
    val numSleepDaemonVertices = mr3Conf.get(SimulateMRRExtendedSetup.NUM_SLEEP_DAEMON_VERTICES).toInt
    val maxDaemonSleepTimeMS = mr3Conf.get(SimulateMRRExtendedSetup.MAX_DAEMON_SLEEP_TIME_MS).toInt

    require { height > 0 && (!(height > 1) || inputDegree > 0) }

    val shuffleScheme = mr3Conf.get(SimulateMRRExtendedSetup.SHUFFLE_SCHEME)
    val destVertexVertexManagerClassName = shuffleScheme match {
      case SimulateMRRExtendedSetup.SCATTER_GATHER_SHUFFLE_SCHEME => classOf[ShuffleVertexManager].getName
      case SimulateMRRExtendedSetup.BROADCAST_SHUFFLE_SCHEME => classOf[ImmediateStartVertexManager].getName
      case SimulateMRRExtendedSetup.ONE_TO_ONE_SHUFFLE_SCHEME => classOf[InputReadyVertexManager].getName
    }
    val mr3ConfPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf))

    val logicalOutputConf = new MR3ConfBuilder(false)
      .set(SimulateMRRExtendedSetup.NUM_SPILLS, mr3Conf.get(SimulateMRRExtendedSetup.NUM_SPILLS))
      .set(SimulateMRRExtendedSetup.SPILL_INTERVAL_MS, mr3Conf.get(SimulateMRRExtendedSetup.SPILL_INTERVAL_MS))
      .set(SimulateMRRExtendedSetup.DATA_MOVEMENT_EVENT_PAYLOAD_SIZE_KB,
           mr3Conf.get(SimulateMRRExtendedSetup.DATA_MOVEMENT_EVENT_PAYLOAD_SIZE_KB))
      .set(SimulateMRRExtendedSetup.SHUFFLE_SCHEME, mr3Conf.get(SimulateMRRExtendedSetup.SHUFFLE_SCHEME))
      .build
    val logicalOutputPayload = Some(UtilsForBuilder.createUserPayloadFromConf(logicalOutputConf))

    val reduceLogicalInputConf = new MR3ConfBuilder(false)
      .set(SimulateMRRExtendedSetup.NUM_SPILLS, mr3Conf.get(SimulateMRRExtendedSetup.NUM_SPILLS))
      .build
    val reduceLogicalInputPayload = Some(UtilsForBuilder.createUserPayloadFromConf(reduceLogicalInputConf))

    // setup EntityDescriptorMap for final Vertex
    val distanceFromRoot = height - 1
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        vertexManagerClassName = destVertexVertexManagerClassName,
        vertexManagerPayload = mr3ConfPayload,
        vPos = distanceFromRoot)
    SetupEntityDescriptorMap.baseLeafOutput(entityDescriptorMapLeafOutput, mr3Conf)

    // setup EntityDescriptorMap for src Vertices
    setupEntityDescriptorMapForSimulateMRRExtendedDag(
        entityDescriptorMapVertex, entityDescriptorMapRootInput,
        entityDescriptorMapEdge, entityDescriptorMapDaemonVertex,
        mr3Conf, inputDegree, containerGroupType, numSleepDaemonVertices, maxDaemonSleepTimeMS,
        destVertexVertexManagerClassName, mr3ConfPayload, logicalOutputPayload, reduceLogicalInputPayload,
        numVerticesInDepth = 1, distanceFromRoot = distanceFromRoot)

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.rootInput -> entityDescriptorMapRootInput.toMap,
        EntityDescriptorMapType.leafOutput -> entityDescriptorMapLeafOutput.toMap,
        EntityDescriptorMapType.edge -> entityDescriptorMapEdge.toMap,
        EntityDescriptorMapType.daemonVertex -> entityDescriptorMapDaemonVertex.toMap)
    DAGBuilder.simulateMRRExtendedDag(
        entityDescriptorMap, mr3Conf, height, inputDegree, shuffleScheme,
        containerGroupType, numSleepDaemonVertices, name)
  }

  @tailrec
  private def setupEntityDescriptorMapForSimulateMRRExtendedDag(
      entityDescriptorMapVertex: mutable.Map[String, (String, Option[UserPayloadProto])],
      entityDescriptorMapRootInput: mutable.Map[String, (String, Option[UserPayloadProto])],
      entityDescriptorMapEdge: mutable.Map[String, (String, Option[UserPayloadProto])],
      entityDescriptorMapDaemonVertex: mutable.Map[String, (String, Option[UserPayloadProto])],
      mr3Conf: MR3Conf, inputDegree: Int, containerGroupType: String,
      numSleepDaemonVertices: Int, maxDaemonSleepTimeMS: Int,
      destVertexVertexManagerClassName: String,
      mr3ConfPayload: Option[UserPayloadProto],
      logicalOutputPayload: Option[UserPayloadProto], reduceLogicalInputPayload: Option[UserPayloadProto],
      numVerticesInDepth: Int, distanceFromRoot: Int): Unit = {
    if (distanceFromRoot == 0) {
      // setup EntityDescriptorMap for daemonVertices
      setupEntityDescriptorMapDaemonVertexForSimulateMRRExtendedDag(
          entityDescriptorMapDaemonVertex,
          mr3Conf, inputDegree, containerGroupType, numSleepDaemonVertices, maxDaemonSleepTimeMS,
          numVerticesInDepth, distanceFromRoot)
    } else {
      val numVerticesInNextDepth = numVerticesInDepth * inputDegree
      // setup EntityDescriptorMap for edges and srcVertices (ignore rootVertices)
      (0 until numVerticesInNextDepth).foreach{ vertexIdx =>
        if (distanceFromRoot == 1) {
          // setup EntityDescriptorMap for map vertices
          SetupEntityDescriptorMap.baseVertex(
              entityDescriptorMapVertex, mr3Conf,
              vertexManagerClassName = classOf[RootInputVertexManager].getName,
              hPos = vertexIdx)
          SetupEntityDescriptorMap.baseRootInput(
              entityDescriptorMapRootInput, mr3Conf,
              logicalInputRootClassName = classOf[SimulateMapLogicalInput].getName,
              inputInitializerClassName = classOf[SimulateInputInitializer].getName,
              inputInitializerPayload = mr3ConfPayload,
              hPos = vertexIdx)
        } else {
          // setup EntityDescriptorMap for src reduce vertices
          SetupEntityDescriptorMap.baseVertex(
              entityDescriptorMapVertex, mr3Conf,
              vertexManagerClassName = destVertexVertexManagerClassName,
              vertexManagerPayload = mr3ConfPayload,
              vPos = distanceFromRoot - 1, hPos = vertexIdx)
        }
        SetupEntityDescriptorMap.baseEdge(
            entityDescriptorMapEdge, mr3Conf,
            srcVertexLogicalOutputClassName = classOf[SimulateLogicalOutput].getName,
            destVertexLogicalInputClassName = classOf[SimulateReduceLogicalInput].getName,
            srcVertexLogicalOutputPayload = logicalOutputPayload,
            destVertexLogicalInputPayload = reduceLogicalInputPayload,
            vPos = distanceFromRoot - 1, hPos = vertexIdx)
      }

      // setup EntityDescriptorMap for daemonVertices
      setupEntityDescriptorMapDaemonVertexForSimulateMRRExtendedDag(
          entityDescriptorMapDaemonVertex,
          mr3Conf, inputDegree, containerGroupType, numSleepDaemonVertices, maxDaemonSleepTimeMS,
          numVerticesInDepth, distanceFromRoot)

      setupEntityDescriptorMapForSimulateMRRExtendedDag(
          entityDescriptorMapVertex, entityDescriptorMapRootInput,
          entityDescriptorMapEdge, entityDescriptorMapDaemonVertex,
          mr3Conf, inputDegree, containerGroupType, numSleepDaemonVertices, maxDaemonSleepTimeMS,
          destVertexVertexManagerClassName, mr3ConfPayload, logicalOutputPayload, reduceLogicalInputPayload,
          numVerticesInNextDepth, distanceFromRoot - 1)
    }
  }

  private def setupEntityDescriptorMapDaemonVertexForSimulateMRRExtendedDag(
      entityDescriptorMapDaemonVertex: mutable.Map[String, (String, Option[UserPayloadProto])],
      mr3Conf: MR3Conf, inputDegree: Int, containerGroupType: String,
      numSleepDaemonVertices: Int, maxDaemonSleepTimeMS: Int,
      numVerticesInDepth: Int, distanceFromRoot: Int): Unit = {
    val daemonSleepTimeMS = scala.util.Random.nextInt(maxDaemonSleepTimeMS.toInt)
    val sleepTimePayload = getSleepTimePayload(daemonSleepTimeMS.millis)

    containerGroupType match {
      case SimulateMRRExtendedSetup.ISOLATION_CONTAINERGROUP_TYPE =>
        (0 until numVerticesInDepth).foreach{ vertexIdx =>
          (0 until numSleepDaemonVertices).foreach{ daemonVertexIdx =>
            SetupEntityDescriptorMap.baseDaemonVertex(
                entityDescriptorMapDaemonVertex, mr3Conf,
                daemonProcessorClassName = classOf[SleepProcessor].getName,
                daemonProcessorPayload = sleepTimePayload,
                vPos = distanceFromRoot, hPos = vertexIdx * numSleepDaemonVertices + daemonVertexIdx)
          }
        }

      case SimulateMRRExtendedSetup.PARENT_CONTAINERGROUP_TYPE =>
        val numGroupsInDepth = math.max(numVerticesInDepth / inputDegree, 1)

        (0 until numGroupsInDepth).foreach{ groupIdx =>
          (0 until numSleepDaemonVertices).foreach{ daemonVertexIdx =>
            SetupEntityDescriptorMap.baseDaemonVertex(
                entityDescriptorMapDaemonVertex, mr3Conf,
                daemonProcessorClassName = classOf[SleepProcessor].getName,
                daemonProcessorPayload = sleepTimePayload,
                vPos = distanceFromRoot, hPos = groupIdx * numSleepDaemonVertices + daemonVertexIdx)
          }
        }

      case SimulateMRRExtendedSetup.DEPTH_CONTAINERGROUP_TYPE =>
        (0 until numSleepDaemonVertices).foreach{ daemonVertexIdx =>
          SetupEntityDescriptorMap.baseDaemonVertex(
              entityDescriptorMapDaemonVertex, mr3Conf,
              daemonProcessorClassName = classOf[SleepProcessor].getName,
              daemonProcessorPayload = sleepTimePayload,
              vPos = distanceFromRoot, hPos = daemonVertexIdx)
        }

      case SimulateMRRExtendedSetup.ALL_CONTAINERGROUP_TYPE =>
        if (distanceFromRoot == 0) {
          (0 until numSleepDaemonVertices).foreach{ daemonVertexIdx =>
            SetupEntityDescriptorMap.baseDaemonVertex(
                entityDescriptorMapDaemonVertex, mr3Conf,
                daemonProcessorClassName = classOf[SleepProcessor].getName,
                daemonProcessorPayload = sleepTimePayload,
                hPos = daemonVertexIdx)
          }
        }
    }
  }

  def dagForTestingCrossDagContainerReuse(
      mr3Conf: MR3Conf, numDaemonTasksPerContainer: Int, daemonSleepTime: Duration,
      numWorkerTasksInVertices: Seq[Int], name: String): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapDaemonVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    // baseVertices
    numWorkerTasksInVertices.indices.foreach{ vertexIdx =>
      SetupEntityDescriptorMap.baseVertex(entityDescriptorMapVertex, mr3Conf, hPos = vertexIdx)
    }
    // sleepDaemonVertices
    (0 until numDaemonTasksPerContainer).foreach{ daemonVertexIdx =>
      SetupEntityDescriptorMap.baseDaemonVertex(
          entityDescriptorMapDaemonVertex, mr3Conf,
          daemonProcessorClassName = classOf[SleepProcessor].getName,
          daemonProcessorPayload = getSleepTimePayload(daemonSleepTime),
          hPos = daemonVertexIdx)
    }

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.daemonVertex -> entityDescriptorMapDaemonVertex.toMap)
    DAGBuilder.dagForTestingCrossDagContainerReuse(
        entityDescriptorMap, mr3Conf,
        numDaemonTasksPerContainer, daemonSleepTime, numWorkerTasksInVertices, name)
  }

  def dagForTestingLocalResources(
      mr3Conf: MR3Conf, dagLR: (String, LocalResource), containerGroupLR: (String, LocalResource),
      containerGroupName: String, name: String): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[CopyFileProcessor].getName,
        processorPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)))

    val entityDescriptorMap = Map(EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap)
    DAGBuilder.dagForTestingLocalResources(
        entityDescriptorMap, mr3Conf, dagLR, containerGroupLR, containerGroupName, name)
  }

  def dagForMultiUsersSharingAM(
      mr3Conf: MR3Conf, input: String, output: String, numReducers: Int,
      containerGroupLRs: Seq[(String, LocalResource)], dagLRs: Seq[(String, LocalResource)],
      dagCredentials: Credentials, name: String): DAGProto = {
    val entityDescriptorMapVertex = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapRootInput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapLeafOutput = mutable.Map.empty[String, (String, Option[UserPayloadProto])]
    val entityDescriptorMapEdge = mutable.Map.empty[String, (String, Option[UserPayloadProto])]

    val inputPayload = getRootInputPayload(mr3Conf, input)
    val outputPayload = getLeafOutputPayload(mr3Conf, output)

    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[ListFileTokenizerProcessor].getName,
        vertexManagerClassName = classOf[RootInputVertexManager].getName,
        processorPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)))
    SetupEntityDescriptorMap.baseVertex(
        entityDescriptorMapVertex, mr3Conf,
        processorClassName = classOf[ListFileSummationProcessor].getName,
        vertexManagerClassName = classOf[ShuffleVertexManager].getName,
        processorPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)),
        vertexManagerPayload = Some(UtilsForBuilder.createUserPayloadFromConf(mr3Conf)),
        vPos = 1)

    SetupEntityDescriptorMap.baseRootInput(
        entityDescriptorMapRootInput, mr3Conf,
        logicalInputRootClassName = classOf[MRInput].getName,
        logicalInputRootPayload = Some(inputPayload),
        inputInitializerClassName = classOf[MRInputAMSplitGenerator].getName)
    SetupEntityDescriptorMap.baseLeafOutput(
        entityDescriptorMapLeafOutput, mr3Conf,
        logicalOutputLeafClassName = classOf[SleepMROutput].getName,
        logicalOutputRootPayload = Some(outputPayload),
        outputCommitterClassName = classOf[MROutputCommitter].getName)

    val tokenToSumEdgePayload = getEdgePayload(
        mr3Conf, classOf[Text].getName, classOf[IntWritable].getName)
    SetupEntityDescriptorMap.baseEdge(
        entityDescriptorMapEdge, mr3Conf,
        srcVertexLogicalOutputClassName = classOf[OrderedPartitionedKVOutput].getName,
        srcVertexLogicalOutputPayload = Some(tokenToSumEdgePayload),
        destVertexLogicalInputClassName = classOf[OrderedGroupedKVInput].getName,
        destVertexLogicalInputPayload = Some(tokenToSumEdgePayload))

    val entityDescriptorMap = Map(
        EntityDescriptorMapType.vertex -> entityDescriptorMapVertex.toMap,
        EntityDescriptorMapType.rootInput -> entityDescriptorMapRootInput.toMap,
        EntityDescriptorMapType.leafOutput -> entityDescriptorMapLeafOutput.toMap,
        EntityDescriptorMapType.edge -> entityDescriptorMapEdge.toMap)
    DAGBuilder.dagForMultiUsersSharingAM(
        entityDescriptorMap, mr3Conf, numReducers, dagLRs, containerGroupLRs, dagCredentials, name)
  }
}

object SetupEntityDescriptorMap {

  def baseVertex(
      entityDescriptorMap: mutable.Map[String, (String, Option[UserPayloadProto])],
      mr3Conf: MR3Conf,
      processorClassName: String = classOf[BaseProcessor].getName,
      vertexManagerClassName: String = classOf[ImmediateStartVertexManager].getName,
      processorPayload: Option[UserPayloadProto] = None,
      vertexManagerPayload: Option[UserPayloadProto] = None,
      vPos: Int = 0,
      hPos: Int = 0): Unit = {
    entityDescriptorMap.put(UtilsForBuilder.getName(BaseDagSetup.processorName, vPos, hPos),
        (processorClassName, processorPayload))
    entityDescriptorMap.put(UtilsForBuilder.getName(BaseDagSetup.vertexManagerName, vPos, hPos),
        (vertexManagerClassName, vertexManagerPayload))
  }

  def baseDaemonVertex(
      entityDescriptorMap: mutable.Map[String, (String, Option[UserPayloadProto])],
      mr3Conf: MR3Conf,
      daemonProcessorClassName: String = classOf[BaseProcessor].getName,
      daemonVertexManagerClassName: String = classOf[BaseDaemonVertexManager].getName,
      daemonProcessorPayload: Option[UserPayloadProto] = None,
      daemonVertexManagerPayload: Option[UserPayloadProto] = None,
      vPos: Int = 0,
      hPos: Int = 0): Unit = {
    entityDescriptorMap.put(
        UtilsForBuilder.getName(BaseDagSetup.processorName + UtilsForBuilder.daemonSuffix, vPos, hPos),
        (daemonProcessorClassName, daemonProcessorPayload))
    entityDescriptorMap.put(
        UtilsForBuilder.getName(BaseDagSetup.vertexManagerName + UtilsForBuilder.daemonSuffix, vPos, hPos),
        (daemonVertexManagerClassName, daemonVertexManagerPayload))
  }

  def baseRootInput(
      entityDescriptorMap: mutable.Map[String, (String, Option[UserPayloadProto])],
      mr3Conf: MR3Conf,
      logicalInputRootClassName: String = classOf[BaseLogicalInput].getName,
      inputInitializerClassName: String = classOf[BaseInputInitializer].getName,
      logicalInputRootPayload: Option[UserPayloadProto] = None,
      inputInitializerPayload: Option[UserPayloadProto] = None,
      vPos: Int = 0,
      hPos: Int = 0): Unit = {
    entityDescriptorMap.put(UtilsForBuilder.getName(BaseDagSetup.logicalInputRootName, vPos, hPos),
        (logicalInputRootClassName, logicalInputRootPayload))
    entityDescriptorMap.put(UtilsForBuilder.getName(BaseDagSetup.inputInitializerName, vPos, hPos),
        (inputInitializerClassName, inputInitializerPayload))
  }

  def baseLeafOutput(
      entityDescriptorMap: mutable.Map[String, (String, Option[UserPayloadProto])],
      mr3Conf: MR3Conf,
      logicalOutputLeafClassName: String = classOf[BaseLogicalOutput].getName,
      outputCommitterClassName: String = classOf[BaseOutputCommitter].getName,
      logicalOutputRootPayload: Option[UserPayloadProto] = None,
      outputCommitterPayload: Option[UserPayloadProto] = None,
      vPos: Int = 0,
      hPos: Int = 0): Unit = {
    entityDescriptorMap.put(UtilsForBuilder.getName(BaseDagSetup.logicalOutputLeafName, vPos, hPos),
        (logicalOutputLeafClassName, logicalOutputRootPayload))
    entityDescriptorMap.put(UtilsForBuilder.getName(BaseDagSetup.outputCommitterName, vPos, hPos),
        (outputCommitterClassName, outputCommitterPayload))
  }

  def baseEdge(
      entityDescriptorMap: mutable.Map[String, (String, Option[UserPayloadProto])],
      mr3Conf: MR3Conf,
      srcVertexLogicalOutputClassName: String = classOf[BaseLogicalOutput].getName,
      destVertexLogicalInputClassName: String = classOf[BaseLogicalInput].getName,
      srcVertexLogicalOutputPayload: Option[UserPayloadProto] = None,
      destVertexLogicalInputPayload: Option[UserPayloadProto] = None,
      vPos: Int = 0,
      hPos: Int = 0): Unit = {
    entityDescriptorMap.put(UtilsForBuilder.getName(BaseMRDagSetup.srcVertexLogicalOutputName, vPos, hPos),
        (srcVertexLogicalOutputClassName, srcVertexLogicalOutputPayload))
    entityDescriptorMap.put(UtilsForBuilder.getName(BaseMRDagSetup.destVertexLogicalInputName, vPos, hPos),
        (destVertexLogicalInputClassName, destVertexLogicalInputPayload))
    entityDescriptorMap.put(UtilsForBuilder.getName(BaseMRDagSetup.edgeManager, vPos, hPos),
        (classOf[ScatterGatherEdgeManager].getName, None))
  }

  def baseMergedInputEdge(
      entityDescriptorMap: mutable.Map[String, (String, Option[UserPayloadProto])],
      mr3Conf: MR3Conf,
      mergedLogicalInputClassName: String = classOf[OrderedGroupedMergedKVInput].getName,
      mergedInputPayload: Option[UserPayloadProto] = None,
      vPos: Int = 0,
      hPos: Int = 0): Unit = {
    entityDescriptorMap.put(UtilsForBuilder.getName(BaseMRDagSetup.mergedLogicalInputName, vPos, hPos),
        (mergedLogicalInputClassName, mergedInputPayload))
  }
}
