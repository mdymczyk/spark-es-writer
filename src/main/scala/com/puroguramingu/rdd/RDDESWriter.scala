package com.puroguramingu.rdd

import com.puroguramingu.{ClientCache, ESClientKey, ESWriter}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.{IndexDefinition, UpdateDefinition}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class RDDESWriter[T: ClassTag](@transient rdd: RDD[T]) extends ESWriter[T] {

  override def esIndex(esKey: ESClientKey, fun: T => IndexDefinition): Unit = {
    rdd.foreachPartition { partition =>
      val client = ClientCache.client(esKey)
      partition.foreach { el => client.execute(fun(el)) }
    }
  }

  override def esUpdate(esKey: ESClientKey, fun: T => UpdateDefinition): Unit = {
    rdd.foreachPartition { partition =>
      val client = ClientCache.client(esKey)
      partition.foreach { el => client.execute(fun(el)) }
    }
  }
}