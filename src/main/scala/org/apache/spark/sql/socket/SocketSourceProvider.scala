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

import java.io.{BufferedOutputStream, DataOutputStream}
import java.net.ServerSocket

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SaveMode}

private[socket] class SocketSourceProvider extends DataSourceRegister
    with RelationProvider
    with CreatableRelationProvider
    with Logging {

  override def shortName(): String = "sock"

  /**
   * Returns a new base relation with the given parameters.
   *
   * @note The parameters' keywords are case insensitive and this insensitivity is enforced
   *       by the Map that is passed to the function.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    new SocketRelation(sqlContext)
  }

  override def createRelation(
      outerSQLContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    mode match {
      case SaveMode.Overwrite | SaveMode.Ignore =>
        throw new AnalysisException(s"Save mode $mode not allowed for Kafka. " +
          s"Allowed save modes are ${SaveMode.Append} and " +
          s"${SaveMode.ErrorIfExists} (default).")
      case _ => // good
    }
    import outerSQLContext.implicits._
    data.map(_.getDouble(0)).foreachPartition { iter: Iterator[Double] =>
      val partitionId = org.apache.spark.TaskContext.getPartitionId()
      val sockServer = new ServerSocket(5449 + partitionId)
      sockServer.setPerformancePreferences(0, 1, 2)
      val sock = sockServer.accept()
      val out = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream, 33554432))
      iter.foreach(out.writeDouble(_))
      out.writeDouble(Double.NaN)
      out.flush()
      sock.close()
    }

    /* This method is suppose to return a relation that reads the data that was written.
     * We cannot support this for Kafka. Therefore, in order to make things consistent,
     * we return an empty base relation.
     */
    new BaseRelation {
      override def sqlContext: SQLContext = unsupportedException
      override def schema: StructType = unsupportedException
      override def needConversion: Boolean = unsupportedException
      override def sizeInBytes: Long = unsupportedException
      override def unhandledFilters(filters: Array[Filter]): Array[Filter] = unsupportedException
      private def unsupportedException =
        throw new UnsupportedOperationException("BaseRelation from Kafka write " +
          "operation is not usable.")
    }
  }
}
