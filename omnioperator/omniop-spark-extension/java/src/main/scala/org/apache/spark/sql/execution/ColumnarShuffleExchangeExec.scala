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

import com.huawei.boostkit.spark.ColumnarPluginConfig

import java.util.Random
import com.huawei.boostkit.spark.Constant.{IS_ENABLE_JIT, IS_SKIP_VERIFY_EXP}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor._
import com.huawei.boostkit.spark.serialize.ColumnarBatchSerializer
import com.huawei.boostkit.spark.util.OmniAdaptorutil.transColBatchToOmniVecs
import com.huawei.boostkit.spark.vectorized.PartitionInfo
import nova.hetu.omniruntime.`type`.{DataType, DataTypeSerializer}
import nova.hetu.omniruntime.operator.config.OperatorConfig
import nova.hetu.omniruntime.operator.project.OmniProjectOperatorFactory
import nova.hetu.omniruntime.vector.{IntVec,VecBatch}
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ColumnarShuffleDependency
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS,ShuffleExchangeExec, ShuffleOrigin}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec.createShuffleWriteProcessor
import org.apache.spark.sql.execution.metric._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.execution.util.MergeIterator
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.MutablePair

class ColumnarShuffleExchangeExec(
                                   override val outputPartitioning: Partitioning,
                                   child:SparkPlan,
                                   shuffleOrigin:ShuffleOrigin = ENSURE_REQUIREMENTS)
  extends ShuffleExchangeExec(outputPartitioning,child) {

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  override lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "datasize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "bytesspilled" -> SQLMetrics.createSizeMetric(sparkContext, "shuffle bytes spilled"),
    "splitTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_split"),
    "spi11Time" -> SQLMetrics.createNanoTimingMetric(sparkContext, "shuffle spill time"),
    "compressTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_ compress"),
    "avgReadBatchNumRows" -> SQLMetrics
      .createAverageMetric(sparkContext, "avg read batch num rows"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numMergedVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of merged vecBatchs"),
    "numOutputRows" -> SQLMetrics
      .createMetric(sparkContext, "number of output rows")) ++ readMetrics ++ writeMetrics

  override def nodeName: String = "OmniColumnarShuffleExchange"

  override def supportsColumnar: Boolean = true

  val serializer: Serializer = new ColumnarBatchSerializer(
    longMetric("avgReadBatchNumRows"),
    longMetric("numOutputRows"))

  @transient lazy val inputColumnarRDD: RDD[ColumnarBatch] = child.executeColumnar()

  //'mapOutputStatisticsFuture' is only needed when enable AQE.
  @transient override lazy val mapOutputstatisticsFuture: Future[MapOutputStatistics] = {
    if (inputColumnarRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(columnarShuffleDependency)
    }
  }

  @transient
  lazy val columnarShuffleDependency: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    ColumnarShuffleExchangeExec.prepareShuffleDependency(
      inputColumnarRDD,
      child.output,
      outputPartitioning,
      serializer,
      writeMetrics,
      longMetric("dataSize"),
      longMetric("bytesSpilled"),
      longMetric("numInputRows"),
      longMetric("splitTime"),
      longMetric("spillTime"))
  }
  var cachedshuffleRDD: ShuffledColumnarRDD = _

  def buildCheck(): Unit = {
    val inputTypes = new Array[DataType](child.output.size)
    child.output.zipWithIndex.foreach {
      case (attr, i) =>
        inputTypes(i) = sparkTypeToOmniType(attr.dataType, attr.metadata)
    }

    outputPartitioning match {
      case HashPartitioning(expressions, numPartitions) =>
        val genHashExpression = ColumnarShuffleExchangeExec.genHashExpr()
        genHashExpression(expressions, numPartitions, ColumnarShuffleExchangeExec.defaultMm3HashSeed, child.output)
      case _ =>
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = new ShuffledColumnarRDD(columnarShuffleDependency, readMetrics)
    }
    val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf
    val enableShuffleBatchMerge: Boolean = columnarConf.enableShuffleBatchMerge
    if (enableShuffleBatchMerge) {
      cachedShuffleRDD.mapPartitionsWithIndexInternal { (index, iter) =>
        new MergeIterator(iter,
          StructType.fromAttributes(child.output),
          longMetric("numMergedVecBatchs"))
      }
    } else {
      cachedShuffleRDD
    }
  }
}

object ColumnarShuffleExchangeExec extends Logging {
  val defaultMm3HashSeed: Int = 42;

  def prepareShuffleDependency(
                                rdd:RDD[ColumnarBatch],
                                outputAttributes:Seq[Attribute],
                                newPartitioning: Partitioning,
                                serializer:Serializer,
                                writeMetrics: Map[String, SQLMetric],
                                datasize:SQLMetric,
                                bytesspilled:SQLMetric,
                                numInputRows:SQLMetric,
                                splitTime:SQLMetric,
                                spillTime:SQLMetric):
  ShuffleDependency[Int,ColumnarBatch,ColumnarBatch] = {


            val rangePartitioner:Option[Partitioner] = newPartitioning match {
              case RangePartitioning(sortingExpressions, numPartitions) =>
                // Extract only fields used for sorting to avoid collecting large fields that does not
                // affect sorting result when deciding partition bounds in RangePartitioner
                val rddForSampling = rdd.mapPartitionsInternal { iter =>
                  // Internally,RangePartitioner runs a job on the RDD that samples keys to compute
                  // partition bounds.To get accurate samples, we need to copy the mutable keys.
                  iter.flatMap(batch=>{
                  val rows = batch.rowIterator.asScala
                  val projection =
                    UnsafeProjection.create(sortingExpressions.map(_.child),outputAttributes)
                  val mutablePair = new MutablePair[InternalRow, Null]()
                    rows.map(row => mutablePair.update(projection(row).copy(), null))
                  1)
                }
                // Construct ordering on extracted sort key.
                val orderingAttributes:Seq[SortOrder]=sortingExpressions.zipWithIndex.map {
              case (ord,i)=>
                ord.copy(child = BoundReference(i, ord.dataType, ord.nullable))
            }
            implicit val ordering = new LazilyGeneratedOrdering (orderingAttributes)
            val part = new RangePartitioner(
              numPartitions,
              rddForSampling,
              ascending=true,
              samplePointsPerPartitionHint =SQLConf.get.rangeExchangeSampleSizePerPartition)
            Some(part)
            case _ => None
           }

            val inputTypes=new Array[DataType](outputAttributes.size)
            outputAttributes.zipWithIndex.foreach {
              case (attr,i) =>
                inputTypes(i) = sparkTypeToOmniType(attr.dataType, attr.metadata)
            }
            // gen RoundRobin pid
            def getRoundRobinPartitionkey:(ColumnarBatch,Int) => IntVec = {
              ／／随机数
              (columnarBatch: ColumnarBatch, numPartitions: Int) => {
                val pidArr = new Array[Int](columnarBatch.numRows())
                for (i <- 0 until columnarBatch.numRows()) {
                  val position = new Random(TaskContext.get().partitionId()).nextInt(numPartitions)
                  pidArr(i) = position + 1
                }
                val vec = new IntVec(columnarBatch.numRows())
                vec.put(pidArr, 0, 0, pidArr.length)
                vec
              }
            }

                        def addpidToColumnBatch(): (IntVec, ColumnarBatch) => (Int, ColumnarBatch) = (pidvec,cb) => {
                          val pidVecTmp=new OmniColumnVector(cb.numRows(), IntegerType, false)
                          pidVecTmp.setVec(pidVec)
                          val newColumns = (pidVecTmp +: (0 until cb.numCols).map(cb.column)).toArray
                          (0,new ColumnarBatch(newColumns,cb.numRows))
                        }
                        // only used for fallback range partitioning
                        def computeAndAddRangePartitionId(
                                                           cbIter:Iterator[ColumnarBatch],
                                                           partitionkeyExtractor:InternalRow=>Any):Iterator[(Int,  ColumnarBatch)] = {
                          val addPid2ColumnBatch = addPidToColumnBatch()
                          cbIter.filter(cb => cb.numRows != 0 && cb.numCols != 0).map {
                            cb =>
                              val pidArr = new Array[Int](cb.numRows)
                              (0 until cb.numRows).foreach { i =>
                                val row = cb.getRow(i)
                                val pid = rangePartitioner.get.getPartition(partitionkeyExtractor(row))
                                pidArr(i) = pid
                              }
                              val pidvec = new IntVec(cb.numRows)
                              pidVec.put(pidArr, 0, 0, cb.numRows)

                              addPid2ColumnBatch(pidVec, cb)
                          }
                        }

                        val isRoundRobin = newPartitioning.isInstanceOf[RoundRobinPartitioning] &&
                          newPartitioning.numPartitions > 1
                        val isOrderSensitive = isRoundRobin && !SQLConf.get.sortBeforeRepartition

                        val rddWithPartitionId: RDD[Product2[Int, ColumnarBatch]] = newPartitioning match {
                          case RoundRobinPartitioning(numPartitions) =>
                            // 按随机数分区
                            rdd.mapPartitionsWithIndexInternal((_, cbIter) => {
                              val getRoundRobinPid = getRoundRobinPartitionKey
                              val addPid2ColumnBatch = addPidToColumnBatch()
                              cbIter.map { cb =>
                                val pidVec = getRoundRobinPid(cb, numPartitions)
                                addPid2ColumnBatch(pidVec, cb)
                              }
                            }, isOrderSensitive = isOrderSensitive)
                          case RangePartitioning(sortingExpressions, _) =>
                            // 排序，按采样数据进行分区
                            rdd.mapPartitionsWithIndexInternal((_, cbIter) => {
                              val partitionKeyExtractor: InternalRow => Any = {
                                val projection =
                                  UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
                                row => projection(row)
                              }
                              val newIter = computeAndAddRangePartitionId(cbIter, partitionKeyExtractor)
                              newIter
                            }, isOrderSensitive = isOrderSensitive)
                          case HashPartitioning(expressions, numPartitions) =>
                            rdd.mapPartitionsWithIndexInternal((_, cbIter) => {
                              val addPid2ColumnBatch = addPidToColumnBatch()
                              // omni project
                              val genHashExpression = genHashExpr()
                              val omniExpr: String = genHashExpression(expressions, numPartitions, defaultMm3HashSeed, outputAttributes)
                              val factory = new omniProjectOperatorFactory(Array(omniExpr), inputTypes, 1, new OperatorConfig(IS_ENABLE_JIT, IS_SKIP_VERIFY_EXP))
                              val op = factory.createOperator()
                              cbIter.map { cb =>
                                val vecs = transColBatchToOmniVecs(cb, true)
                                op.addInput(new VecBatch(vecs, cb.numRows()))
                                val res = op.getOutput
                                if (res.hasNext) {
                                  // TODO call next()once while get all result?
                                  val retBatch = res.next()
                                  val pidVec = retBatch.getVectors()(0)
                                  // close return VecBatch
                                  retBatch.close()
                                  addPid2ColumnBatch(pidVec.asInstanceOf[IntVec], cb)
                                } else {
                                  throw new Exception("Empty Project Operator Result...")
                                }
                              }
                            }, isOrderSensitive = isOrderSensitive)
                        }
                      case SinglePartition=>
                        rdd.mapPartitionsWithIndexInternal((_,cbIter) =>{
                          cbIter.map {cb=>(0, cb)}
                        }, isOrderSensitive =isOrderSensitive)
            }
                                        val numCols =outputAttributes.size
                                        val intputTypeArr:Seq[DataType]=outputAttributes.map {attr=>
                                          sparkTypeToOmniType(attr.dataType,attr.metadata)
                                        }
                                        val intputTypes = DataTypeSerializer.serialize(intputTypeArr.toArray)

                                        val partitionInfo:PartitionInfo =newPartitioning match {
                                          case SinglePartition =>
                                            new PartitionInfo("single", 1, numCols, intputTypes)
                                          case RoundRobinPartitioning(numPartitions) =>
                                            new PartitionInfo("rr", numPartitions, numCols, intputTypes)
                                          case HashPartitioning(expressions, numPartitions) =>
                                            new PartitionInfo("hash", numPartitions, numCols, intputTypes)
                                          case RangePartitioning(ordering, numPartitions) =>
                                            new PartitionInfo("range", numPartitions, numCols, intputTypes)
                                            }
                                              new ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
                                                rddWithPartitionId,
                                                new PartitionIdPassthrough(newPartitioning.numPartitions),
                                                serializer,
                                                shuffleWriterProcessor = createShuffleWriteProcessor(writeMetrics),
                                                partitionInfo = partitionInfo,
                                                dataSize = dataSize,
                                                bytesspilled = bytesSpilled,
                                                numTnputRows = mumTnputRows,
                                                splitTime = splitTime,
                                                spillTime = spillTime)
                                            }

                                            // gen hash partition expression
                                            def genHashExpr(): (Seq[Expression], Int, Int, Seq[Attribute]) => String = {
                                              (expressions: Seq[Expression], numPartitions: Int, seed: Int, outputAttributes: Seq[Attribute]) => {
                                                val exprIdMap = getExprIdMap(outputAttributes)
                                                val EXP_JSON_FORMATER1 =
                                                  ("{\"exprType\": \"FUNCTION\", \"returnTypet":1,\"function_name\":\"%s\",\"arguments\":[" +
                                                  "%s,{\"exprType\": \"LITERAL\", \"dataType\":1,\"isNull\":false,\"value\":%d}]}"
                                                )
                                                val EXP_JSON_FORMATER2 = ("{\"exprType\": \"FUNCTION\", \"returnType\":1,\"function_name\":\"%s\", \"arguments\": [%s,%s] }")
                                                var omniExpr: String = ""
                                                expressions.foreach { expr =>
                                                  val colExpr = rewriteToOmniJsonExpressionLiteral(expr, exprIdMap)
                                                  if (omniExpr.isEmpty) {
                                                    omniExpr = EXP_JSON_FORMATER1.format("mm3hash", colExpr, seed)
                                                  } else {
                                                    omniExpr = EXP_JSON_FORMATER2.format("mm3hash", colExpr, omniExpr)
                                                  }
                                                }
                                                omniExpr = EXP_JSON_FORMATER1.format("pmod", omniExpr, numPartitions)
                                                logDebug(s"hash omni expression: $omniExpr")
                                                omniExpr
                                              }
                                            }
                                        }


