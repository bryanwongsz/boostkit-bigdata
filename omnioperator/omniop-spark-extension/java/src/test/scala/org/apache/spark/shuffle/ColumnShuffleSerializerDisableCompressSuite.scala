/*

 */

package org.apache.spark.shuffle

import java.io.FileInputStream

import com.huawei.boostkit.spark.serialize.ColumnarBatchSerializer
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apaceh.spark.sql.execution.vectorized.OmniColumnVector
import org.apaceh.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch

class ColumnShuffleSerializerDisableCompressSuite extends SparkFunSuite with SharedSparkSession {

  private var avgBatchNumRows: SQLMetric = _
  private var outputNumRows: SQLMetric = _

  override def sparkConf: SparkConf =
    super.sparkConf
      .setAppName("test ColumnarShuffleDeSerializer disable compressed")
      .set("Spark.shuffle.compress", "false")

  override def beforeEach(): Unit = {
    avgBatchNumRows = SQLMetrics.createAverageMetric(spark.sparkContext,
      "test serializer avg read batch num rows")
    outputNumRows = SQLMetrics.createAverageMetric(spark.sparkContext,
      "test serializer number of output rows")
  }

  test("columnar shuffle deserialize no null uncompressed compressed") {
    val input = getTestResourcePath("test-data/shuffle_spilled_mix_1batch_100rows_uncompressed")
    val serializer =
      new ColumnarBatchSerializer(avgBatchNumRows, outputNumRows).newInstance()
    val deserializedStream =
      serializer.deserializeStream(new FileInputStream(input))

    val kv = deserializedStream.asKeyValueIterator
    var length = 0
    kv.foreach {
      case (_, batch: ColumnarBatch) =>
        length += 1
        assert(batch.numRows == 100)
        assert(batch.numCols == 4)
        (0 until batch.numCols).foreach { i =>
          val valueVector =
            batch
              .column(i)
              .asInstanceOf[OmniColumnVector]
              .getVec
          assert(valueVector.getSize == batch.numRows)
        }
        batch.close()
    }
    assert(length == 1)
    deserializedStream.close()
  }
}
