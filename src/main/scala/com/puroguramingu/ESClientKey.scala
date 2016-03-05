package com.puroguramingu

import com.sksamuel.elastic4s.ElasticsearchClientUri

case class ESClientKey(settings: Map[String, String], uri: ElasticsearchClientUri) {}
