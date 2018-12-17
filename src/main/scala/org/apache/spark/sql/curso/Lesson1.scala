package org.apache.spark.sql.curso

import scala.language.postfixOps

// Processing with sequences
object Lesson1 extends App {
  val data = 1 to 1000
  println(data
    .map(_ + 1)
    .filter(_ % 2 == 0)
    .length)
}
