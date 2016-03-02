package com.puroguramingu

import com.sksamuel.elastic4s.ElasticsearchClientUri
import org.elasticsearch.common.settings.Settings

/**
  * Created by mateusz on 3/2/16.
  */
case class ESClientKey(settings: Settings, uri: ElasticsearchClientUri) {}
