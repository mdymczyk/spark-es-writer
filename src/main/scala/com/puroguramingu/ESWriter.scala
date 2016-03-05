package com.puroguramingu

import com.puroguramingu.dstream.DStreamESWriter
import com.puroguramingu.rdd.RDDESWriter
import com.sksamuel.elastic4s.{UpdateDefinition, IndexDefinition}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.language.implicitConversions
import scala.reflect.ClassTag

object ESWriter {
  implicit def createESWriter[T: ClassTag](dstream: DStream[T]): ESWriter[T] =
    new DStreamESWriter[T](dstream)

  implicit def createESWriter[T: ClassTag](rdd: RDD[T]): ESWriter[T] =
    new RDDESWriter[T](rdd)
}

abstract class ESWriter[T: ClassTag] {

  def esIndex(esKey: ESClientKey, fun: T => IndexDefinition): Unit

  def esUpdate(esKey: ESClientKey, fun: T => UpdateDefinition): Unit
}