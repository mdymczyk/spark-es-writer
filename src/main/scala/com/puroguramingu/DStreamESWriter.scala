package com.puroguramingu

import org.apache.spark.streaming.dstream.DStream

/**
  * Created by mateusz on 3/4/16.
  */
class DStreamESWriter[T](dstream: DStream[T]) extends ESWriter[T] {
  override def filter[E](esKey: ESClientKey, fun: (T) => E): Unit = ???
  override def foreach[E](esKey: ESClientKey, fun: (T) => E): Unit = ???
  override def map[E](esKey: ESClientKey, fun: (T) => E): Unit = ???
}
