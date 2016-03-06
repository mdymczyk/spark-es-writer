package com.puroguramingu.dstream

import com.puroguramingu.rdd.RDDESWriter
import com.puroguramingu.{ESClientKey, ESWriter}
import com.sksamuel.elastic4s.{UpdateDefinition, IndexDefinition}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

class DStreamESWriter[T: ClassTag](dstream: DStream[T]) extends ESWriter[T] {

  override def esIndex(esKey: ESClientKey, fun: (T) => IndexDefinition): Unit = {
    dstream.foreachRDD ( rdd => new RDDESWriter[T](rdd).esIndex(esKey, fun) )
  }

  override def esUpdate(esKey: ESClientKey, fun: (T) => UpdateDefinition): Unit = {
    dstream.foreachRDD ( rdd => new RDDESWriter[T](rdd).esUpdate(esKey, fun) )
  }
}
