package com.puroguramingu

import com.puroguramingu.util.WithElasticSearch
import com.sksamuel.elastic4s.ElasticsearchClientUri
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}

/**
  * Created by mateusz on 3/4/16.
  */
class RDDESWriterTest extends FunSuite with Matchers with WithElasticSearch {

  val sc = new SparkContext(new SparkConf().setAppName("Test").setMaster("local[*]"))

  test("Save new documents") {
    import com.puroguramingu.ESWriter._
    import com.sksamuel.elastic4s.ElasticDsl._

    sc.parallelize(Array(1, 2, 3))
      .esForeach(
        ESClientKey(Map(), ElasticsearchClientUri("localhost", 9400)),
        (x: Int) => index into "bands" / "artists" fields "val" -> x
      )
  }

}
