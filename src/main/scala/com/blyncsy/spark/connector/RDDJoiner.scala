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

/**
  * Class that can pull data from a connector
  * @tparam C Type of connector
  * @tparam K Type of keys
  * @tparam T Type being pulled from connector
  */
trait RDDJoiner[C, K, T] extends Serializable {
  def doJoin[V](connection: C, keys: Iterator[(K, V)]): Iterator[(K, (V, Option[T]))]

  def doGet(connection: C, keys: Iterator[K]): Iterator[T] = {
    doJoin(connection, keys.map((_, null))).flatMap(_._2._2)
  }
}
