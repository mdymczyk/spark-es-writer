package com.puroguramingu

import java.util.concurrent.TimeUnit

import com.puroguramingu.util.WithElasticSearch
import com.sksamuel.elastic4s.ElasticsearchClientUri
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class RDDESWriterTest extends FunSuite with Matchers with WithElasticSearch {

  val sc = new SparkContext(new SparkConf().setAppName("Test").setMaster("local[*]"))

  test("Save new documents") {
    import com.sksamuel.elastic4s.ElasticDsl._
    import com.puroguramingu.ESWriter._

    client.execute(
      createIndex("bands")
    )

    while (
      !Await.result(
        client.execute(
          indexExists("bands")
        ),
        Duration(1, TimeUnit.MINUTES)
      ).isExists
    ) {
      Thread.sleep(100)
    }

    sc.parallelize(Array(1, 2, 3)).esIndex(
      ESClientKey(Map(), ElasticsearchClientUri("localhost", 9400)),
      (x: Int) => index into "bands" / "artists" fields "val" -> x
    )

    Thread.sleep(1000)

    val res = Await.result(
      client.execute(
        search("bands" / "artists").query(matchAllQuery)
      ),
      Duration(1, TimeUnit.MINUTES)
    ).getHits.hits().size

    assertResult(3)(res)
  }

}
