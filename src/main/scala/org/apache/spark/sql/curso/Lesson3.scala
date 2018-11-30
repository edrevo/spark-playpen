package org.apache.spark.sql.curso

import scala.language.postfixOps

import org.apache.spark.sql.SparkSession

// Actions vs transformations
object Lesson3 extends App {
  def add1WithPrint(v: Int) = {
    if (v == 128) println("Got it!")
    v + 1
  }
  val spark = SparkSession
      .builder()
      .appName("test")
      .master("local[8]")
      .getOrCreate()

  val range = 1 to 1000
  println("RANGE")
  range.map(add1WithPrint)
  val rdd = spark.sparkContext.parallelize(range)
  println("RDD")
  val rdd1 = rdd.map(add1WithPrint)

  println("Here comes the action!")
  rdd1.count()
}
