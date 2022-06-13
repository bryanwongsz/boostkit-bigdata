/*

 */

package com.huawei.boostkit.spark.hive.util

import java.io.{File, FilenameFilter}
import java.nio.charset.StandardCharsets

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Row, SparkSession}

class HiveResourceRunner(val spark: SparkSession, val resource: String) {
  val caseIds = HiveResourceRunner.parseCaseIds(HiveResourceRunner.locateResourcePath(resource),
    ".sql")

  def runQuery(caseId: String, roundId: Int, explain: Boolean = false): Unit = {
    val path = "%s%s.sql".format(resource, caseId)
    val absolute = HiveResourceRunner.locateResourcePath(path)
    val sql = FileUtils.readFileToString(new File(absolute), StandardCharsets.UTF_8)
    println("Running query %s (round %d)... ".format(caseId, roundId))
    val df = spark.sql(sql)
    if (explain) {
      df.explain(extended = true)
    }
    val result: Array[Row] = df.head(100)
    result.foreach(row => println(row))
  }
}

object HiveResourceRunner {
  private def parseCaseIds(dir: String, suffix: String): List[String] = {
    val folder = new File(dir)
    if (!folder.exists()) {
      throw new IllegalArgumentException("dir does not exist" + dir)
    }
    folder
      .listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = name.endsWith(suffix)
      })
      .map(f => f.getName)
      .map(n => n.substring(0, n.lastIndexOf(suffix)))
      .sortBy(s => {
        //fill with leading zeros
        "%s%s".format(new String((0 until 16 - s.length).map(_ => '0').toArray), s)
      })
      .toList
  }

  private def locateResourcePath(resource: String): String = {
    classOf[HiveResourceRunner].getClassLoader.getResource("")
      .getPath.concat(File.separator).concat(resource)
  }
}