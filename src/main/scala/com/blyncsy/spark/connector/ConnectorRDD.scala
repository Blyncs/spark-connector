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
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
  * Created by David Lewis on 7/28/17.
  * Base class for a ConnectorRDD
  */
case class ConnectorRDD[C, T: ClassTag, P <: Partition](connector: Connector[C],
                                                        connectorPartitioner: ConnectorPartitioner[C, T, P],
                                                        @transient sc: SparkContext
                                                       ) extends RDD[T](sc, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    connectorPartitioner.compute(connector.connection, split.asInstanceOf[P], context)
  }

  override protected def getPartitions: Array[Partition] = {
    connectorPartitioner.getPartitions.asInstanceOf[Array[Partition]]
  }
}

trait ConnectorPartitioner[C, T, P <: Partition] {
  this: Serializable =>
  def getPartitions: Array[P]

  def compute(connection: C, split: P, context: TaskContext): Iterator[T]
}
