package org.apache.spark.sql.curso

import scala.language.postfixOps

import org.apache.spark.sql.SparkSession

// Processing with RDDs
object Lesson2 extends App {
  val spark = SparkSession
      .builder()
      .appName("test")
      .master("local[8]")
      .getOrCreate()

  val data = spark.sparkContext.parallelize(1 to 1000)
  println(data
    .map(1 +)
    .filter(_ % 2 == 0)
    .count())
}
