package org.apache.spark.sql.curso

import scala.util.Try

import org.apache.spark.sql.SparkSession

// Execution order
object Lesson4 extends App {
  def add1WithFail(v: Int) = {
    if (v == 256) ???
    v + 1
  }
  def add1WithPrint(v: Int) = {
    if (v == 128) println("GOT IT")
    v + 1
  }
  val spark = SparkSession
      .builder()
      .appName("test")
      .master("local[8]")
      .getOrCreate()

  val range = 1 to 1000
  println("RANGE")
  Try { range.map(add1WithFail).map(add1WithPrint) }

  val rdd = spark.sparkContext.parallelize(range)
  println("RDD")
  rdd.map(add1WithFail).map(add1WithPrint).count()
}
