package com.puroguramingu.dstream

import java.util.concurrent.TimeUnit

import com.puroguramingu.ESClientKey
import com.puroguramingu.ESWriter._
import com.puroguramingu.util.ElasticSearch
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.ElasticsearchClientUri
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{ClockWrapper, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, streaming}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class DStreamESWriterTest extends FunSuite with ElasticSearch with Matchers {

  /**
    * TODO:
    * Make tests nicer - create Spark and SparkStreaming specs, do something about ES waits?
    */

  val sc = new SparkContext(
    new SparkConf()
      .setAppName("Test")
      .setMaster("local[*]")
      .set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
  )
  val esKey: ESClientKey = ESClientKey(Map(), ElasticsearchClientUri("localhost", 9400))

  val batchDuration: streaming.Duration = Seconds(1)

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

    // GIVEN
    val ssc = new StreamingContext(sc, batchDuration)
    val clock = new ClockWrapper(ssc)

    val testQueue = mutable.Queue[RDD[Int]]()
    val dstream = ssc.queueStream(testQueue)

    dstream.esIndex(
      esKey,
      (x: Int) => index into "bands" / "artists" fields "val" -> x
    )

    ssc.start()

    // WHEN
    testQueue += sc.parallelize(Array(1, 2, 3))
    clock.advance(batchDuration.milliseconds)
    Thread.sleep(1000)

    // THEN
    val res = Await.result(
      client.execute(
        search("bands" / "artists").query(matchAllQuery)
      ),
      Duration(1, TimeUnit.MINUTES)
    ).getHits.hits().size

    assertResult(3)(res)
    ssc.stop(false)
  }

  test("Update a documents") {
    import com.sksamuel.elastic4s.ElasticDsl._

    // GIVEN
    val ssc = new StreamingContext(sc, batchDuration)
    val clock = new ClockWrapper(ssc)
    val testQueue = mutable.Queue[RDD[(String, String)]]()
    val dstream = ssc.queueStream(testQueue)
    dstream.esUpdate(
      esKey,
      (t: (String,String)) => update(t._1).in("bands" / "artists").doc(Map("text_val" -> t._2))
    )

    val id = Await.result(
      client.execute(
        index into "bands" / "artists" fields "text_val" -> "test"
      ),
      Duration.Inf
    ).getId

    ssc.start()

    // WHEN
    testQueue += sc.parallelize(Seq((id, "post_test")))
    clock.advance(batchDuration.milliseconds)
    Thread.sleep(1000)

    // THEN
    val res = Await.result(
      client.execute(
        search("bands" / "artists").query(idsQuery(Seq(id)))
      ),
      Duration(1, TimeUnit.MINUTES)
    ).getHits.hits()(0)

    assertResult("post_test")(res.sourceAsMap().get("text_val"))
    ssc.stop(false)
  }


}
