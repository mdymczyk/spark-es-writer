package com.puroguramingu

import com.sksamuel.elastic4s.ElasticClient

import scala.collection.mutable

/**
  * Created by mateusz on 3/2/16.
  */
object ClientCache {

  // TODO not sure if this is thread safe, might need to modify it
  private val clients = mutable.Map[ESClientKey, ElasticClient]()

  def client(key: ESClientKey): ElasticClient = {
    clients.getOrElse(key, {
      val client = ElasticClient.transport(key.settings, key.uri)
      clients(key) = client
      client
    })
  }

}
