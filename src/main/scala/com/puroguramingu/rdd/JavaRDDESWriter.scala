package com.puroguramingu.rdd

import org.apache.spark.api.java.function.Function
import com.puroguramingu.ESClientKey
import com.sksamuel.elastic4s.{UpdateDefinition, IndexDefinition}
import org.apache.spark.api.java.JavaRDD

import scala.reflect.ClassTag

class JavaRDDESWriter[T](rdd: JavaRDD[T])(implicit val classTag: ClassTag[T]) {
  private val esWriter = new RDDESWriter[T](rdd)

  def esIndex(esKey: ESClientKey, fun: Function[T, IndexDefinition]): Unit = {
    esWriter.esIndex(esKey, t => fun.call(t))
  }

  def esUpdate(esKey: ESClientKey, fun: Function[T, UpdateDefinition]): Unit = {
    esWriter.esUpdate(esKey, t => fun.call(t))
  }
}

object JavaRDDESWriterFactory {
  def fromJavaRDD[T](rdd: JavaRDD[T]): JavaRDDESWriter[T] = {
    implicit val cmt: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    new JavaRDDESWriter[T](rdd)
  }
}