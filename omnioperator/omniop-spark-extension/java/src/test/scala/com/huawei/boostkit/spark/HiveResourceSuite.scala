/*
*
 */

package com.huawei.boostkit.spark.hive

import java.util.Properties

import com.huawei.boostkit.spark.hive.util.HiveResourceRunner
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.SparkSession

/**
 * @since 2021/12/15
 */
class HiveResourceSuite extends SparkFunSuite {
  private val QUERY_SQLS = "query-sqls"
  private var spark: SparkSession = _
  private var runner: HiveResourceRunner = _

  override def beforeAll(): Unit = {
    val properties = new Properties()
    properties.load(this.getClass.getClassLoader.getResourceAsStream("HiveResource.properties"))

    spark = SparkSession.builder()
      .appName("test-sql-context")
      .master("local[2]")
      .config(readConf(properties))
      .enableHiveSupport()
      .getOrCreate()
    LogManager.getRootLogger.setLevel(Level.WARN)
    runner = new HiveResourceRunner(spark, QUERY_SQLS)

    val hiveDb = properties.getProperty("hive.db")
    spark.sql(if (hiveDb == null) "use default" else s"use $hiveDb")
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("queryBySparkSql-HiveDataSource") {
    runner.runQuery("q1", 1)
    runner.runQuery("q2", 1)
    runner.runQuery("q3", 1)
    runner.runQuery("q4", 1)
    runner.runQuery("q5", 1)
    runner.runQuery("q6", 1)
    runner.runQuery("q7", 1)
    runner.runQuery("q8", 1)
    runner.runQuery("q9", 1)
    runner.runQuery("q10", 1)
  }

  def readConf(properties: Properties): SparkConf = {
    val conf = new SparkConf()
    val wholeStage = properties.getProperty("spark.sql.codegen.wholeStage")
    val offHeapSize = properties.getProperty("spark.memory.offHeap.size")
    conf.set("hive.metastore.uris", properties.getProperty("hive.metastore.uris"))
      .set("spark.sql.warehouse.dir", properties.getProperty("spark.sql.warehouse.dir"))
      .set("spark.memory.offHeap.size", if (offHeapSize == null) "8G" else offHeapSize)
      .set("spark.sql.codegen.wholeStage", if (wholeStage == null) "false" else wholeStage)
      .set("spark.sql.extensions", properties.getProperty("spark.sql.extensions"))
      .set("spark.shuffle.manager", properties.getProperty("spark.shuffle.manager"))
      .set("spark.sql.orc.impl", properties.getProperty("spark.sql.orc.impl"))
  }
}
