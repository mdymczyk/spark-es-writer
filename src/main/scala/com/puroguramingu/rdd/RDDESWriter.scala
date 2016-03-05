package com.puroguramingu.rdd

import com.puroguramingu.{ClientCache, ESClientKey, ESWriter}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by mateusz on 3/4/16.
  */
class RDDESWriter[T: ClassTag](@transient rdd: RDD[T]) extends ESWriter[T] {
  override def esForeach[E, R, Q](esKey: ESClientKey, fun: T => E): Unit = {
    /**
      * TODO:
      * This does not work because execute() requires an implicit which cannot be found when called like this.
      * I could change the method signature to pass (implicit executable: Executable[E, R, Q])
      * but Executable from elastic4s aren't Serializable... Looking for a solution
      */
    rdd.foreach((el: T) => {
      ClientCache.client(esKey).execute(fun(el))
    })
  }
}