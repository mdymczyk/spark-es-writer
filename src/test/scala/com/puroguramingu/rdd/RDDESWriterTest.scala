package com.puroguramingu.rdd

import java.util.concurrent.TimeUnit

import com.puroguramingu.ESClientKey
import com.puroguramingu.ESWriter._
import com.puroguramingu.util.ElasticSearch
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.ElasticsearchClientUri
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class RDDESWriterTest extends FunSuite with Matchers with ElasticSearch {

  val sc = new SparkContext(new SparkConf().setAppName("Test").setMaster("local[*]"))
  val esKey: ESClientKey = ESClientKey(Map(), ElasticsearchClientUri("localhost", 9400))

  override def beforeAll() = {
    super.beforeAll()
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
  }

  override def afterAll() = {
    super.afterAll()
    sc.stop()
  }

  test("Save new documents") {
    import com.sksamuel.elastic4s.ElasticDsl._

    sc.parallelize(Array(1, 2, 3)).esIndex(
      esKey,
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

  test("Update a documents") {
    import com.sksamuel.elastic4s.ElasticDsl._

    val id = Await.result(
      client.execute(
        index into "bands" / "artists" fields "text_val" -> "test"
      ),
      Duration.Inf
    ).getId

    sc.parallelize(Seq((id, "post_test"))).esUpdate(
      esKey,
      (t: (String,String)) => update(t._1).in("bands" / "artists").doc(Map("text_val" -> t._2))
    )

    Thread.sleep(1000)

    val res = Await.result(
      client.execute(
        search("bands" / "artists").query(idsQuery(Seq(id)))
      ),
      Duration(1, TimeUnit.MINUTES)
    ).getHits.hits()(0)

    assertResult("post_test")(res.sourceAsMap().get("text_val"))
  }

}
