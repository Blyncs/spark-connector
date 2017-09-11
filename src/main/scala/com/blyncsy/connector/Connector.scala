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
package com.blyncsy.connector

import java.util.concurrent.ConcurrentHashMap
import java.util.function

/**
  * Created by David Lewis on 7/27/17.
  * This trait represents all of the information required to connect to an outside source of data
  */
trait Connector[T] extends Serializable {
  @transient lazy val connection: T = Connector.getConnection(this)

  protected def connect: T

  private[connector] def getConnection: T = connect
}

object Connector {
  private val cachedConnectors = new ConcurrentHashMap[Connector[_], Any]
  private val connectionCreator = new function.Function[Connector[_], Any] {
    override def apply(t: Connector[_]): Any = {
      t.getConnection
    }
  }

  private[connector] def getConnection[T, CT <: Connector[T]](connector: Connector[T]): T = {
    cachedConnectors.computeIfAbsent(connector, connectionCreator).asInstanceOf[T]
  }
}
