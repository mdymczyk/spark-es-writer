package com.puroguramingu.dstream

import com.puroguramingu.{ESClientKey, ESWriter}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * Created by mateusz on 3/4/16.
  */
class DStreamESWriter[T: ClassTag](dstream: DStream[T]) extends ESWriter[T] {
  override def esForeach[E,R,Q](esKey: ESClientKey, fun: T => E) = ???
}
