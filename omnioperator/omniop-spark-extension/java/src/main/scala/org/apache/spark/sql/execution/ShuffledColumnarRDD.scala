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

import org.apache.spark. {Dependency, MapOutputTrackerMaster, Partition, Partitioner, ShuffleDependency, SparkEnv,TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleReadMetricsReporter}
import org.apache.spark.sgl.internal.SOLConf
import org.apache.spark.sql.vectorized.ColumnarBatch

**
*The [[Partition]] used by [[ShuffledRowRDD]].
*/
private final case class ShuffledColumnarRDDPartition(
                                                       index:Int,spec:ShufflePartitionSpec) extends Partition
class ShuffledColumnarRDD(
                           var dependency:ShuffleDependency[Int,ColumnarBatch,ColumnarBatch],
                           metrics:Map[String,SQLMetric],
                           partitionSpecs:Array[ShufflePartitionspec])
  extends RDD[ColumnarBatch](dependency.rdd.context,Ni1){
  def this(
            dependency: ShuffleDependency[Int,ColumnarBatch,ColumnarBatch],
            metrics: Map[String,SQLMetric])={
    this(dependency,metrics,
      Array.tabulate(dependency-partitioner.numPartitions)(i => CoalescedPartitionspec(i, i+1)))
    dependency.rdd.context.setLocalProperty(
      SortShuffleManager.FETCH_SHUFFLE_BLOCKS_IN BATCH_ENABLED _KEY,
      SQLConf.get.fetchShuffleBlocksInBatch.tostring)
    override def getDependencies: Seq[Dependency[_]] =List(dependency)
    override val partitioner: option[Partitioner]=
      if (partitionspecs.forall(_.isInstanceof [CoalescedPartitionspec])){
        val indices=partitionspecs.map(_.asInstanceof[CoalescedPartitionspec].startReducerIndex) // TODO this check is based on assumptions of callers' behavior but is sufficient for now. if(indices.toSet.size = partitionSpecs.length){
        Some(new CoalescedPartitioner(dependency-partitioner, indices))
      }else{
        None
      }
  }else{
    None
    {
      override def getPartitions: Array[Partition] ={
        Array-tabulate[Partition](partitionspecs.length) { i =>
          ShuffledColumnarRDDPartition(i, partitionspecs(i))
          {
            {
              override def getPreferredLocations(partition: Partition): Seq[string] = {
                val tracker=SparkEnv.get.mapoutputTracker.asInstanceof[MapOutputTrackerMaster]
                partition.asInstanceof[ShuffledColumnarRDDPartition].spec match (
                case CoalescedPartitionspec(startReducerIndex, endReducerIndex)=>
                // TODO order by partition size.
                startReducerIndex.until (endReducerIndex).flatMap { reducerIndex=>
                  tracker.getPreferredLocationsForShuffle(dependency, reducerIndex)
                }
                case PartialReducerPartitionspec(_, startMapIndex, endMapIndex,_) =>
                tracker.getMapLocation(dependency, startMapIndex, endMapIndex)
                case PartialMapperPartitionspec(mapIndex, , ) =>
                tracker.getMapLocation(dependency,mapIndex,mapIndex +1)
              }
            }
            override def compute(split: Partition, context: TaskContext):Iterator[ColumnarBatch]=1
            val tempMetrics = context.taskMetrics() -createTemp8huffleReadMetrics()
            // 'SOLShuffleReadMetricsReporter' wil1 update its own metrics for SQL exchange operator,
            // as well as the 'tempMetrics' for basic shuffle metrics.
            val sqlMetricsReporter = new SOLShuffleReadMetricsReporter(tempMetrics,metrics)
            val reader = split.asInstanceof[ShuffledColumnarRDDPartition].spec match {
              case CoalescedPartitionspec(startReducerIndex, endReducerIndex) =>
                SparkEnv.get.shuffleManager.getReader(
                  dependency.shuffleHandle,
                  startReducerIndex,
                  endReducerIndex,
                  context,
                  sqlMetricsReporter)
              case PartialReducerPartitionspec(reducerIndex, startMapIndex, endMapIndex,_)=>
                SparkEnv.get.shuffleManager.getReader(
                  dependency.shuffleHandle,
                  startMapIndex,
                  endMapIndex,
                  reducerIndex,
                  reducerIndex+1,
                  context,
                  sqlMetricsReporter)
              case PartialMapperPartitionspec(mapIndex,startReducerIndex, endReducerIndex) =>
                SparkEnv.get.shuffleManager.getReader(
                  dependency.shuffleHandle,
                  mapIndex,
                  mapIndex+1,
                  startReducerIndex,
                  endReducerIndex,
                  context,
                  sq1MetricsReporter)
            }
            reader.read().asInstanceof[Iterator[Product2[Int,ColumnarBatchll1.man(21
            {