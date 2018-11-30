package org.apache.spark.sql.curso

import scala.language.postfixOps

import org.apache.spark.sql.SparkSession

// WebUI + Caching
object Lesson5 extends App {
  val spark = SparkSession
      .builder()
      .appName("test")
      .master("local[8]")
      .getOrCreate()


  val rdd = spark.sparkContext.parallelize(1 to 1000)
  val transformedRdd = rdd.map(4 * ).flatMap { i => 0 to i } //.persist()
  val count = transformedRdd.count()
  val sum = transformedRdd.map(_.toLong).reduce(_ + _)
  println(sum.toDouble / count)

  Thread.sleep(60000)
}
