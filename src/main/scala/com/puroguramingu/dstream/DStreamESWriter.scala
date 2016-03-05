package com.puroguramingu.dstream

import com.puroguramingu.{ESClientKey, ESWriter}
import com.sksamuel.elastic4s.{UpdateDefinition, IndexDefinition}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

class DStreamESWriter[T: ClassTag](dstream: DStream[T]) extends ESWriter[T] {
  override def esIndex(esKey: ESClientKey, fun: (T) => IndexDefinition): Unit = ???
  override def esUpdate(esKey: ESClientKey, fun: (T) => UpdateDefinition): Unit = ???
}
