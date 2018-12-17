package org.apache.spark.sql.curso

import scala.language.postfixOps

import org.apache.spark.sql.SparkSession

// Joins and broadcasts
object Lesson8 extends App {
  val spark = SparkSession
      .builder()
      .appName("test")
      .master("local[8]")
      .getOrCreate()

  val commonWords = Seq("a", "about", "all", "also", "and", "as", "at", "be", "because", "but", "by", "can", "come", "could", "day", "do", "even", "find", "first", "for", "from", "get", "give", "go", "have", "he", "her", "here", "him", "his", "how", "I", "if", "in", "into", "it", "its", "just", "know", "like", "look", "make", "man", "many", "me", "more", "my", "new", "no", "not", "now", "of", "on", "one", "only", "or", "other", "our", "out", "people", "say", "see", "she", "so", "some", "take", "tell", "than", "that", "the", "their", "them", "then", "there", "these", "they", "thing", "think", "this", "those", "time", "to", "two", "up", "use", "very", "want", "way", "we", "well", "what", "when", "which", "who", "will", "with", "would", "year", "you", "your")
  val commonWordsRDD = spark.sparkContext.parallelize(commonWords).map(_.toLowerCase)
  val shakespeare = spark.sparkContext.textFile("shakespeare")
    .flatMap(_.split("""\s+"""))
    .filter(_.nonEmpty)
    .map(_.toLowerCase)
  /*val resultJoin = shakespeare
    .map(_ -> 1)
    .reduceByKey(_ + _)
    .leftOuterJoin(commonWordsRDD.map(_.toLowerCase).keyBy(identity))
    .collect { case (word, (count, None)) => word -> count }
    .sortBy({ case (word, count) => count }, ascending = false)
    .take(10)
    .toList
  println(resultJoin)

  val resultNoBroad = shakespeare
    .filter(w => !commonWords.contains(w))
    .map(_ -> 1)
    .reduceByKey(_ + _)
    .sortBy({ case (word, count) => count }, ascending = false)
    .take(10)
    .toList
  println(resultNoBroad)*/

  val commonWordsBroadcast = spark.sparkContext.broadcast(commonWords.toSet)
  val resultBroad = shakespeare
    .filter(w => !commonWordsBroadcast.value.contains(w))
    .map(_ -> 1)
    .reduceByKey(_ + _)
    .sortBy({ case (word, count) => count }, ascending = false)
    .take(10)
    .toList
  println(resultBroad)

  Thread.sleep(60000)
}

