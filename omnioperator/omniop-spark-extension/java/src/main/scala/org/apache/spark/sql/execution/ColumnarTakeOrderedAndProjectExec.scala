/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import java.util.concurrent.TimeUnit.NANOSECONDS
import com.huawei.boostkit.spark.Constant.{IS_ENABLE_JIT, IS_SKIP_VERIFY_EXP}
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor.{getExprIdMap, rewriteToOmniJsonExpressionLiteral, sparkTypeToomniType}
import com.huawei.boostkit.spark.serialize.ColumnarBatchSerializer
import com.huawei.boostkit.spark.util.omniAdaptorUtil.{addAl1AndGetIterator, gensortParam}
import nova.hetu.omniruntime.`type`.DataType
import nova.hetu.omniruntime.operator.config.OperatorConfig
import nova.hetu.omniruntime.operator.topn.OmniTopNwithExproperatorFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, singlePartition}
import org.apache.spark.sql.catalyst.util.truncatedstring
import org.apache.spark.sql.execution.ColumnarProjection.dealPartitionData
import org.apache.spark.sql.execution.metric._
import org.apache.spark.sql.execution.util.SparkMemoryUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
case class ColumnarTakeorderedAndProjectExec {
imit:Int,
sortorder: Seq[Sortorder],
projectList: Seq[NamedExpression],
child:SparkPlan
extends UnaryExecNode{
  override def supportsColumnar:Boolean =true
  override def nodeName:string = "OmniColumnarTakeOrderedAndProject"
  val serializer: Serializer = new ColumnarBatchSerializer(
    longMetric("avgReadBatchNumRows"),
    1ongMetric("numOutputRows"))
  private lazy val writeMetrics=
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private lazy val readMetrics=
    SOLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SOLMetrics.createsizeMetric(sparkContext, "data size"),
    "bytesSpilled" -→ SQLMetrics.createsizeMetric(sparkContext, "shuffle bytes spilled"),
    "splitrime" -→ SOLMetrics.createNanoTimingMetric(sparkContext, "totaltime_split"),
    "spillTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "shuffle spill time"),
    "avgReadBatchNumRows"-→SOLMetrics
      .createAverageMetric(sparkContext, "avg read batch num rows"),
    "numInputRows"→SOLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputRows"→SQLMetrics
      .createMetric(sparkContext, "number of output rows"),
    // omni
    "outputDatasize"→SQLMetrics.createsizeMetric(sparkContext, "output data size"),
    "numOutputRows" -→SQLMetrics.createMetric(sparkContext, "number of output rowal'),
  "addInputTime" -→ SQIMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
  "getoutputTime" -→ SQLMetrics.createrimingMetric(sparkContext, "time in omni getoutput"),
  "omniCodegenTime"→SQLMetrics.createTimingMetric(sparkContext,"time in omni codegen"),
  "numInputVecBatchs"→SQLMetrics.createMetric(sparkContext, "number of input vecBatchs"), "numOutputVecBatchs"-→SQLMetrics.createMetric(sparkContext, "number of output vecBatchs"), "addInputTime"→SoLMetrics.createTimingMetric(sparkContext, "time in omni addinput")
  )++readMetrics ++ writeMetrics
  override def output:Seq[Attribute]={
    projectList.map(_.toAttribute)
    {
      override def executeCollect(): Array[InternalRow]={
        throw new UnsupportedoperationException
        {
          protected override def doExecute(): RDD[InternalRow] = {
            throw new UnsupportedoperationException
            {
              def buildCheck():Unit={
                gensortParam(child.output,sortorder)
                val projectEqualChildoutput =projectList ==child.output
                var omniInputTypes:Array[DataType] = nu11
                var omniExpressions:Array[String]=nu11
                if(!projectEqualChildoutput){
                  omniInputTypes=child.output.map(
                    exp=> sparkTypeToomniType(exp.dataType,exp.metadata)).toArray
                  omniExpressions=projectList.map(
                    exp => rewriteToomniJsonExpressionLiteral(exp,getExprIdMap(child.output))).toArray

                  override def doExecuteColumnar():RDD[ColumnarBatch]={
                    val (sourceTypes,ascendings, nullFirsts, sortColsExp) = genSortParam(child.output,sortorder)
                    def computeTopN(iter:Iterator[ColumnarBatch], schema: StructType): Iterator[ColumnarBatch] = { val startCodegen=System.nanoTime()
                      val topNoperatorFactory =new OmniTopNWithExproperatorFactory(sourceTypes, limit,
                        sortColsExp, ascendings, nullFirsts, new operatorConfig(IS_ENABLE_JIT, IS_SKIP_VERIFY_EXP)) val topNoperator=topNOperatorFactory.createOperator
                      longMetric("omniCodegenTime") +=NANOSECONDS.toMillis(System.nanoTime() - startCodegen)
                      SparkMemoryUtils.addLeaksafeTaskCompletionListener[Unit](_=>{
                        topNOperator.close()
                        1)
                        addAl1AndGetIterator(topNOperator,iter,schema,
                          longMetric("addInputTime"), longMetric("numInputVecBatchs"), longMetric("numInputRows") ,
                          longMetric("getoutputTime"), longMetric("numOutputVecBatchs"), longMetric("numOutputRows"), longMetric("outputDatasize"))
                      }
                      val localTopk:RDD[ColumnarBatch] ={
                        child.executeColumnar().mapPartitionsWithIndexInternal { (_, iter) =>
                          computeTopN(iter,this.child.schema)
                        }
                        {
                          val shuffled = new ShuffledColumnarRDD(
                            ColumnarShuffleExchangeExec.prepareShuffleDependency(
                              localTopR,
                              child.output,
                              SinglePartition,
                              serializer,
                              writeMetrics,
                              longMetric("dataSize"),
                              longMetric("bytesspilled"),
                              longMetric ("numInputRows") ,
                              longMetric("splitTime"),
                              longMetric("spillTime")),
                            readMetrics)
                          val projectEqualChildoutput = projectList==child.output
                          var omniInputTypes:Array[DataType]=nu11
                          var omniExpressions:Array[String] = null
                          var addInputTime:SQLMetric=nu11
                          var omniCodegenTime:SQLMetric=nu11
                          var getOutputTime: SQLMetric = nu11
                          if(!projectEqualChildoutput){
                            omniInputTypes=child.output.map(
                              exp => sparkTypeToOmniType (exp.dataType,exp.metadata)).toArray
                            omniExpressions =projectList.map(
                              exp=>rewriteToomniJsonExpressionLiteral(exp,getExprIdMap(child.output))).toArray
                            addInputTime = longMetric("addInputTime")
                            omniCodegenTime = longMetric("omniCodegenTime")
                            getoutputTime = longMetric("getOutputTime")
                          }
                          shuffled.mapPartitions {iter=>
                            // TopN=omni-top-n+omni-project
                            val topN:Iterator[ColumnarBatch]=computeTopN(iter,this.child.schema)
                            if (!projectEqualChildoutput){
                              dealPartitionData(null,null, addInputTime, omniCodegenTime,
                                getoutputTime,omniInputTypes, omniExpressions, topN,this.schema)
                            }else{
                              topN
                              1
                            }
                            1
                            override def outputordering:Seq[Sortorder]=sortorder
                            override def outputPartitioning:Partitioning=SinglePartition
                            override def simplestring(maxFields:Int):string=(
                            val orderBystring=truncatedstring(sortorder, "[", ",", "]", maxFields)
                            val outputstring = truncatedstring (output, "[", ",", "1",maxFields)
                            s"omniColumnarTakeOrderedAndProject(limit=$limit,orderBy=SorderByString,output=Soutputstring)" {
                              [
                            }]5。739