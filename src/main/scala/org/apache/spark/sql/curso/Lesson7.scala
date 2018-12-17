package org.apache.spark.sql.curso

import scala.language.postfixOps

import org.apache.spark.sql.SparkSession

// Serialization
object Lesson7 extends App {
  val spark = SparkSession
      .builder()
      .appName("test")
      .master("local[8]")
      .getOrCreate()

  val data = spark.sparkContext.parallelize(1 to 1000)
  val myHelper = new {
    def add(a: Int, b: Int) = a + b
  }
  println(data
      .repartition(100)
      .mapPartitions(iter => Iterator(iter.next()))
      .reduce(myHelper.add)
  )

  Thread.sleep(60000)
}
