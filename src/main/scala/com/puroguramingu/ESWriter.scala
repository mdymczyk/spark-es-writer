package com.puroguramingu

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Created by mateusz on 3/4/16.
  */

object ESWriter {
  implicit def createESWriter[T: ClassTag](dstream: DStream[T]): ESWriter[T] =
    new DStreamESWriter[T](dstream)

  implicit def createESWriter[T: ClassTag](rdd: RDD[T]): ESWriter[T] =
    new RDDESWriter[T](rdd)
}

abstract class ESWriter[T: ClassTag] {

  def filter[E](esKey: ESClientKey, fun: T => E)

  def foreach[E](esKey: ESClientKey, fun: T => E)

  def map[E](esKey: ESClientKey, fun: T => E)

}
