/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.socket

import java.io.{BufferedInputStream, DataInputStream}
import java.net.Socket

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

private[socket] class SocketRelation(
    override val sqlContext: SQLContext)
    extends BaseRelation with TableScan with Logging {
  override def schema: StructType = StructType(Seq(
    StructField("value", DoubleType)
  ))

  override def buildScan(): RDD[Row] = {
    sqlContext.sparkContext.parallelize(Seq.empty[Int], 60).mapPartitionsWithIndexInternal { (idx, _) =>
      new Iterator[Row] {
        private val sock = new Socket("localhost", 5449 + idx)
        sock.setKeepAlive(true)
        sock.setTcpNoDelay(true)
        sock.setPerformancePreferences(0,1,2)
        private val in = new DataInputStream(new BufferedInputStream(sock.getInputStream, 33554432))
        private var nextElem: Row = _
        private var hasNextElem: Boolean = true

        private def read(): Unit = {
          if (!hasNextElem) return
          val v = in.readDouble()
          if (v.isNaN) {
            hasNextElem = false
            sock.close()
          }
          else nextElem = Row(v)
        }

        override def hasNext: Boolean = {
          if (nextElem != null)
            true
          else {
            read()
            hasNextElem
          }
        }

        override def next(): Row = {
          if (nextElem == null) read()
          val local = nextElem
          nextElem = null
          local
        }
      }
    }
  }
}
