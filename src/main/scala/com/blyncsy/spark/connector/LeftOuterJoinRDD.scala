/**
  *
  * Copyright 2017 Blyncsy
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.blyncsy.spark.connector

import com.blyncsy.connector.Connector
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

/**
  * Created by David Lewis on 7/28/17.
  *
  */
case class LeftOuterJoinRDD[K, V, T: ClassTag, C](var baseRDD: RDD[(K, V)],
                                                  connector: Connector[C],
                                                  joiner: RDDJoiner[C, K, T]
                                                 ) extends RDD[(K, (V, Option[T]))](baseRDD) {

  override def compute(split: Partition, context: TaskContext): Iterator[(K, (V, Option[T]))] = {
    val connection = connector.connection
    val baseIterator = baseRDD.iterator(split, context)
    joiner.doJoin(connection, baseIterator)
  }

  override protected def getPartitions: Array[Partition] = baseRDD.partitions

  override protected def clearDependencies(): Unit = {
    super.clearDependencies()
    baseRDD = null
  }
}





