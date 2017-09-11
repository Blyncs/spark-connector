/**
  *
  * Copyright 2017 Blyncsy
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.blyncsy.spark

import com.blyncsy.connector.Connector
import org.apache.spark.{Partition, SparkContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


package object connector {

  implicit class SparkContextWrapper(sc: SparkContext) {
    def connectorRdd[C: Connector, T: ClassTag, P <: Partition](partitioner: ConnectorPartitioner[C, T, P]): RDD[T] = {
      ConnectorRDD(
        implicitly[Connector[C]],
        partitioner,
        sc
      )
    }
  }

  implicit class RDDWrapper[K](rdd: RDD[K]) {
    def connectorGet[C: Connector, T: ClassTag](joiner: RDDJoiner[C, K, T]): RDD[T] = {
      GetRDD(
        rdd,
        implicitly[Connector[C]],
        joiner
      )
    }

    def connectorWrite[C: Connector, R: ClassTag](writer: RDDWriter[C, K, R]): R = {
      rdd.mapPartitions { items =>
        if (items.hasNext) {
          Iterator(writer.doWrite(implicitly[Connector[C]].connection, items))
        } else {
          Iterator.empty
        }
      }.fold(writer.zero)(writer.reduceResults)
    }
  }

  implicit class PairRDDWrapper[K, V](rdd: RDD[(K, V)]) {
    def connectorLeftOuterJoin[C: Connector, T: ClassTag](joiner: RDDJoiner[C, K, T]): RDD[(K, (V, Option[T]))] = {
      LeftOuterJoinRDD(
        rdd,
        implicitly[Connector[C]],
        joiner
      )
    }
  }

}
