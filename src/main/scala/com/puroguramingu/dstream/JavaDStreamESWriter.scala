package com.puroguramingu.dstream

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

import com.puroguramingu.ESClientKey
import com.sksamuel.elastic4s.{IndexDefinition, UpdateDefinition}
import org.apache.spark.api.java.function.Function
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

class JavaDStreamESWriter[T](dstream: DStream[T])(implicit val classTag: ClassTag[T]) {
  private val esWriter = new DStreamESWriter[T](dstream)

  def esIndex(esKey: ESClientKey, fun: Function[T, IndexDefinition]): Unit = {
    esWriter.esIndex(esKey, t => fun.call(t))
  }

  def esUpdate(esKey: ESClientKey, fun: Function[T, UpdateDefinition]): Unit = {
    esWriter.esUpdate(esKey, t => fun.call(t))
  }

}

object JavaDStreamESWriterFactory {
  def fromJavaDStream[T](dstream: DStream[T]): JavaDStreamESWriter[T] = {
    implicit val cmt: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    new JavaDStreamESWriter[T](dstream)

  }
}