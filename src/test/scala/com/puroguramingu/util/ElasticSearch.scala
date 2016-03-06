package com.puroguramingu.util

import java.io.File
import java.util.UUID

import com.sksamuel.elastic4s.ElasticClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.NodeBuilder
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait ElasticSearch extends BeforeAndAfterEach with BeforeAndAfterAll {
  this: Suite =>

  private val tempFile = File.createTempFile("elasticsearchtests", "tmp")
  private val homeDir = new File(tempFile.getParent + "/" + UUID.randomUUID().toString)

  homeDir.mkdir()
  homeDir.deleteOnExit()
  tempFile.deleteOnExit()

  protected val settings = Settings.settingsBuilder()
    .put("node.http.enabled", true)
    .put("http.enabled", true)
    .put("index.store.type", "niofs")
    .put("path.home", homeDir.getAbsolutePath)
    .put("index.number_of_shards", 1)
    .put("index.number_of_replicas", 0)
    .put("es.logger.level", "DEBUG")
    .put("http.port", 9500)
    .put("transport.tcp.port", 9400)
    .build()

  val node = NodeBuilder.nodeBuilder().settings(settings).local(false).build().start()

  val client = ElasticClient.fromNode(node)

}


