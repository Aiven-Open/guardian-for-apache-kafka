# Overview

Guardian for Apache Kafka is an open source utility for backing up [Apache Kafka](https://kafka.apache.org/) clusters.
It is built using [Scala](https://www.scala-lang.org/) entirely
with [Pekko-Streams](https://pekko.apache.org/docs/pekko/current/stream/index.html)
to ensure that the tool runs as desired with large datasets in different scenarios.

## Versions

The core modules are compiled against:

* Pekko Streams $pekko.version$+ (@extref:[Reference](pekko-docs:stream/index.html), [Github](https://github.com/apache/incubator-pekko))
* Pekko Streams Circe $pekko-stream-circe.version$+ ([Github](https://github.com/mdedetrich/pekko-streams-circe))
* PureConfig $pure-config.version$+ ([Reference](https://pureconfig.github.io/docs/), [Github](https://github.com/pureconfig/pureconfig))
* ScalaLogging $scala-logging.version$+ ([Github](https://github.com/lightbend/scala-logging))

The cli modules are compiled against:

* Decline $decline.version$+ ([Reference](https://ben.kirw.in/decline/), [Github](https://github.com/bkirwi/decline))
