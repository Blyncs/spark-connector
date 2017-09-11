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
  * Object that can write data to a connector
  * @tparam C Type of connector
  * @tparam T Type being written to connector
  * @tparam R Object being returned from the write (to track stats, timing, etc)
  */
trait RDDWriter[C, T, R] extends Serializable {
  def doWrite(connection:C, items: Iterator[T]): R

  def reduceResults: (R, R) => R

  def zero: R
}
