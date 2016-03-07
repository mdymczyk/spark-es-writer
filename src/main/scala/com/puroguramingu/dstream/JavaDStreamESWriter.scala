package com.puroguramingu.dstream

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