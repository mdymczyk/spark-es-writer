package com.puroguramingu

import com.sksamuel.elastic4s.ElasticsearchClientUri

/**
  * Created by mateusz on 3/2/16.
  */
case class ESClientKey(settings: Map[String, String], uri: ElasticsearchClientUri) {}
