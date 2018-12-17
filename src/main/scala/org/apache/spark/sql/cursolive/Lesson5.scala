package org.apache.spark.sql.cursolive

import scala.language.postfixOps

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

// WebUI + Caching
object Lesson5 extends App {
  val spark = SparkSession
      .builder()
      .appName("test")
      .master("local[8]")
      .getOrCreate()

  val rdd = spark.sparkContext.parallelize(1 to 1000)
  val transformedRdd = rdd
    .map(_ * 4)
    .flatMap(i => 0 to i) // 5 => 0, 1, 2, 3, 4, 5
    .cache()
  val count = transformedRdd.count()
  val sum = transformedRdd.map(_.toLong).reduce(_ + _)
  println(sum.toDouble / count)

  Thread.sleep(600000)
}
