package com.puroguramingu.rdd

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

import com.puroguramingu.{ClientCache, ESClientKey, ESWriter}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.{IndexDefinition, UpdateDefinition}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class RDDESWriter[T: ClassTag](@transient rdd: RDD[T]) extends ESWriter[T] {

  override def esIndex(esKey: ESClientKey, fun: T => IndexDefinition): Unit = {
    rdd.foreachPartition { partition =>
      val client = ClientCache.client(esKey)
      partition.foreach { el => client.execute(fun(el)) }
    }
  }

  override def esUpdate(esKey: ESClientKey, fun: T => UpdateDefinition): Unit = {
    rdd.foreachPartition { partition =>
      val client = ClientCache.client(esKey)
      partition.foreach { el => client.execute(fun(el)) }
    }
  }
}