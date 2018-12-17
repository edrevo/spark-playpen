package org.apache.spark.sql.curso

import scala.language.postfixOps

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// Shuffling / determinism
object Lesson6 extends App {
  val spark = SparkSession
      .builder()
      .appName("test")
      .master("local[8]")
      .getOrCreate()

  val data = spark.sparkContext.parallelize(1 to 1000)
  println(data
      .repartition(100)
      .mapPartitions(iter => Iterator(iter.next()))
      .reduce(_ + _)
  )

  Thread.sleep(60000)
}
