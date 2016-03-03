package com.puroguramingu

import org.apache.spark.rdd.RDD

/**
  * Created by mateusz on 3/4/16.
  */
case class RDDESWriter[T](rdd: RDD[T]) extends ESWriter[T] {
  override def filter[E](esKey: ESClientKey, fun: (T) => E): Unit = ???
  override def foreach[E](esKey: ESClientKey, fun: (T) => E): Unit = ???
  override def map[E](esKey: ESClientKey, fun: (T) => E): Unit = ???
}
