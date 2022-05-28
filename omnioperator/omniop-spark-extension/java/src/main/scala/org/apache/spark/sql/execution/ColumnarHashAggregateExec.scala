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
import com.huawei.boostkit.spark.expression.omniExpressionAdaptor._
import com.huawei.boostkit.spark.util.omniAdaptorUtil.transColBatchToomniVecs
import nova.hetu.omniruntime.`type'.DataType
import nova.hetu.omniruntime.constants.FunctionType
import nova.hetu.omniruntime.operator.aggregator.OmniHashAggregationwithExproperatorFactory
import nova.hetu.omniruntime.operator.config.operatorConfig
import nova.hetu.omniruntime.operator.project.omniProjectoperatorFactory
import nova.hetu.omniruntime.vector.{Vec,VecBatch}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution.ColumnarProjection.dealPartitionData
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.util.SparkMemoryutils
import org.apache.spark.sql.execution.util.SparkMemoryutils.addLeakSafeTaskCompletionListener
import org.apache.spark.sql.execution.vectorized.omniColumnVector
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 *Hash-based aggregate operator that can also fallback to sorting when data exceeds
memory size.
 */
case class ColumnarHashAggregateExec(
                                    requiredChildDistributionExpressions: Option[Seq[Expression]],
                                    groupingExpressions: Seq[NamedExpression],
                                    aggregateExpressions:Seq[AggregateExpression],
                                    aggregateAttributes: Seq[Attribute],
                                    initialInputBufferOffset: Int,
                                    resultExpressions:Seq[NamedExpression],
                                    child: SparkPlan)
  extends BaseAggregateExec
    with AliasAwareoutputPartitioning{

  override lazy val metrics = Map(
    "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
    "numInputRows" -> SQLMetrics.createMetzic(sparkContext, "number of input rows"),
    "numInputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "nmber of input vecBatchs"),
    "omniCodegenTime"  -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
    "getoutputTime"  ->  SQLMetrics.createTimingMetric(sparkContext, "time in omni getoutput"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputVecBatchs"  -> SQLMetrics.createMetric(sparkContext, "number of output vecBatchs"))

  override def supportsColumnar: Boolean =true
  override def nodeName:String="OmniColumnarHashAggregate"

  def buildcheck():Unit={
    val attrExpsIdMap = getExprIdMap(child.output)
    val omniGroupByChanel: Array[AnyRef] = groupingExpressions.map(
      exp=> rewriteToomniJsonExpressionLiteral(exp, attrExpsIdMap)).toArray
    var omniInputRaw=false
    var omnioutputPartial=false
    val omniAggTypes = new Array[DataType](aggregateExpressions.size)
    val omniAggFunctionTypes = new Array[FunctionType] (aggregateExpressions.size)
    val omniAggoutputTypes = new Array[DataType](aggregateExpressions.size)
    val omniAggChannels= new Array[AnyRef] (aggregateExpressions.size)
    var index=0
    for (exp<-aggregateExpressions){
      if (exp.filter.isDefined)(
        throw new UnsupportedoperationException("Unsupported filter in AggregateExpression")
    }
    if(exp.isDistinct){
      throw new UnsupportedoperationException(s"Unsupported aggregate expression with distinct flag"
      {
        if(exp.mode==Final){
          exp.aggregateFunction match(
          case Sum(_) | Min(_) | Max(_) |Count(_)=>
          val aggExp = exp.aggregateFunction.inputAggBufferAttributes.head
          omniAggTypes(index) = sparkTypeToOmniType(aggExp.dataType,aggExp.metadata)
          omniAggFunctionTypes(index) = toomniAggFunType(exp, isHashAgg=true)
          omniAggoutputTypes(index)=
            sparkTypeToOmniType(exp.aggregateFunction.dataType)
          omniAggChannels(index)=  revriteToomniJsonExpressionLiteral(aggExp,attrExpsIdMap)
          case_=throw new UnsupportedoperationException(s"Unsupported aggregate aggregateFunction:
          {
          } else if (exp.mode==Partial){
            omniInputRaw=true
            omnioutputPartial=true
            exp.aggregateFunction match(
            case Sum(_) | Min(_) |·Max(_):| Count(_)=>
            val aggExp=exp.aggregateFunction.children.head
            omniAggTypes(index) = sparkTypeToOmniType(aggExp.dataType)
            omniAggFunctionTypes (index) = toomniAggFunType(exp, isHashAgg = true)
            omniAggoutputTypes(index)=
              sparkTypeToOmn iType (exp. aggregateFunction.dataType)
            omniAggChannels(index)=
              rewriteToOmniJsonExpressionLiteral(aggExp,attrExpsIdMap)
            case=throw new UnsupportedOperationException(s"Unsupported aggregate
              aggregateFunction:Sexp")
          }
        }else(
          throw new UnsupportedoperationException (s"Unsupported aggregate mode: Sexp.mode") }
        index+=1
    }
    val omnisourceTypes = new Array[DataType](child.outputSet.size)
    val inputIter=child.outputset.toIterator
    var i=0
    while (inputIter.hasNext){
      val inputAttr=inputIter.next()
      i+=1
      omniSourceTypes(i) = sparkTypeToomniType(inputAttr.dataType,inputAttr.metadata)
    }
    checkomniJsonWhiteList(filterExpr="",·omniAggChannels)
    checkomniJsonWhiteList(filterExpr="", omniGroupByChane1)
    //·check for final project
    if(lomnioutputPartial){
      val finalout=groupingExpressions.map(_.toAttribute) ++aggregateAttributes
      val projectInputTypes·=finalout.map(
        exp => sparkTypeToOmniType(exp.dataType,exp.metadata)).toArray
      val projectExpressions:Array[AnyRef] = resultExpressions.map(
    } exp=rewriteToomniJsonExpressionLiteral(exp, getExprIdMap(finalout))).toArray
    checkomniJsonWhiteList( filterExpr ="",projectExpressions)
    {
      override def doExecuteColumnar
      ():RDD[ColumnarBatch]={
        val addinputTime =longMetric(name="addInputTime")
        val numInputRows=longMetric(name="numInputRows")
        val numInputVecBatchs=longMetric(name="numInputVecBatchs")
        val omniCodegenTime=longMetric(name="omniCodegenTime")
        val getoutputTime =longMetric(name="getoutputTime")
        val numoutputRows = longMetric(name="numoutputRows")
        val numoutputVecBatchs=longMetric(name="numOutputVecBatchs")
        val attrExpsIdMap =getExprIdMap(child.output)
        val omniGroupByChanel=groupingExpressions.map(
          exp=>rewriteToomniJsonExpressionLiteral(exp,attrExpsIdMap)).toArray
        var omniInputRaw=false
        var omnioutputPartial=false
        val omniAggTypes=new Array[DataType](aggregateExpressions.size)
        val omniAggFunctionTypes=new Array[FunctionType](aggregateExpressions.size)
        val omniAggoutputTypes=new Array[DataType](aggregateExpressions.size)
        val omniAggChannels = new Array[String](aggregateExpressions.size)
        var index=0
        for (exp <-aggregateExpressions)(
          if (exp.filter.isDefined)
            throw new UnsupportedoperationException("Unsupported filter in AggregateExpression")
        if(exp.isDistinct){
          throw new UnsupportedoperationException("Unsupported aggregate expression with distinct flag")
        }
        if (exp.mode==Final)(
          exp.aggregateFunction match
        case Sum(_)| Min(_)·| Max(_) | Count(_)=>
        val aggExp=exp.aggregateFunction.inputAggBufferAttributes.head
        omniAggTypes(index)=sparkTypeToomniType(aggExp.dataType,aggExp.metadata)
        omniAggFunctionTypes(index)=toomniAggFunType(exp,isHashAgg=true)
        omniAggoutputTypes(index)=
          sрarkTурeToOmni Tуpе (exp.aggregateFunction.dataType)
        omniAggChannels(index)=
          rewriteToOmniJsonExpressionLiteral(aggExp,attrExpsIdMap)
        case_=>throw new UnsupportedoperationException(s"Unsupported aggregate aggregateFunction: $(exp)") {
          )else if (exp.mode==Partial)(
            omniInputRaw=true
              omnioutputPartial=true
              exp.aggregateFunction match
          case Sum(_)| Min(_)|·Max(_) | Count(_)=>
          val aggExp=exp.aggregateFunction.children.head
          omniAggTypes(index)=sparkTypeToomniType(aggExp.dataType)
