package org.apache.spark.sql.curso

import scala.language.postfixOps

import org.apache.spark.sql.SparkSession

// Dataframe + Datasets
object Lesson9 extends App {
  val spark = SparkSession
      .builder()
      .appName("test")
      .master("local[8]")
      .getOrCreate()

  import spark.sqlContext.implicits._
  val commonWords = Seq("a", "about", "all", "also", "and", "as", "at", "be", "because", "but", "by", "can", "come", "could", "day", "do", "even", "find", "first", "for", "from", "get", "give", "go", "have", "he", "her", "here", "him", "his", "how", "I", "if", "in", "into", "it", "its", "just", "know", "like", "look", "make", "man", "many", "me", "more", "my", "new", "no", "not", "now", "of", "on", "one", "only", "or", "other", "our", "out", "people", "say", "see", "she", "so", "some", "take", "tell", "than", "that", "the", "their", "them", "then", "there", "these", "they", "thing", "think", "this", "those", "time", "to", "two", "up", "use", "very", "want", "way", "we", "well", "what", "when", "which", "who", "will", "with", "would", "year", "you", "your")
  val commonWordsDataset = commonWords.toDS().map(_.toLowerCase)
  val shakespeare = spark.sqlContext.read.text("shakespeare")
    .as[String]
    .flatMap(_.split("""\s+"""))
    .filter(_.nonEmpty)
    .map(_.toLowerCase)
  shakespeare
    .joinWith(commonWordsDataset, shakespeare("value") === commonWordsDataset("value"), "left")
    .filter(_._2 == null)
    .groupByKey(_._1)
    .mapValues(_ => 1)
    .reduceGroups(_ + _)
    .sort($"ReduceAggregator(int)".desc)
    .show(10)

  Thread.sleep(60000)
}
