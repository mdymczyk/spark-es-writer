# spark-es-writer

This repository is highly influenced by [Cloudera's spark-kafka-writer](https://github.com/cloudera/spark-kafka-writer) and serves the same purpose but for ElasticSearch clients.

It contains APIs allowing you to execute ElasticSearch requests on worker nodes, which might be otherwise problematic due to ES Clients not being Serializable and/or creating too many clients on the worker nodes.

TODO:
- add installation
- add tutorials
