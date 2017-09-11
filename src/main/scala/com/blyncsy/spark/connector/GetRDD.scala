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
package com.blyncsy.spark.connector

import com.blyncsy.connector.Connector
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

/**
  * Created by David Lewis on 9/10/17.
  *
  */
case class GetRDD[K, T: ClassTag, C](baseRDD: RDD[K],
                                     connector: Connector[C],
                                     joiner: RDDJoiner[C, K, T]
                                    ) extends RDD[T](baseRDD) {
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    joiner.doGet(connector.connection, baseRDD.iterator(split, context))
  }

  override protected def getPartitions: Array[Partition] = baseRDD.partitions
}
