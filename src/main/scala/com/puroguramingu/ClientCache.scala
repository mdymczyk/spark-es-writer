package com.puroguramingu

import com.sksamuel.elastic4s.ElasticClient
import org.elasticsearch.common.settings.Settings

import scala.collection.mutable

/**
  * Created by mateusz on 3/2/16.
  */
object ClientCache {

  // TODO not sure if this is thread safe, might need to modify it
  private val clients = mutable.Map[ESClientKey, ElasticClient]()

  def client(key: ESClientKey): ElasticClient = {
    import scala.collection.JavaConverters._
    clients.getOrElse(key, {
      val client = ElasticClient.transport(Settings.settingsBuilder().put(key.settings.asJava).build(), key.uri)
      clients(key) = client
      client
    })
  }

}
