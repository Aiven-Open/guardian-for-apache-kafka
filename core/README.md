# Guardian for Apache Kafka - Core

This module contains core configuration for setting up the Kafka Consumer

By default core uses [Alpakka Kafka][alpakka-kafka] to interact with a Kafka Cluster however you can also provide your
own implementation by extending the `aiven.io.guardian.kafka.KafkaClientInterface`. Since Kafka consumers handle auto 
commit of cursors the `KafkaClientInterface` uses a `SourceWithContext` so that its possible for the `Source`
to automatically commit cursors when successfully reading topics.

## Configuration

Specification (including environment variable overrides) can be found [here](/src/main/resources/reference.conf).

The primary `aiven.io.guardian.kafka.KafkaClient` is configured using [Alpakka Kafka][alpakka-kafka] [Consumer
configuration](https://doc.akka.io/docs/alpakka-kafka/current/consumer.html) which also contains the default values.
The committing of Kafka cursors also requires 
[CommitterSettings configuration](https://doc.akka.io/docs/alpakka-kafka/current/consumer.html#committer-sink).

There is also a generic `aiven.io.guardian.kafka.configs.KafkaCluster` configuration at `"kafka-cluster"` for anything not specific
to the kafka consumer, i.e. which topics to backup/compact/restore.

[alpakka-kafka]: https://doc.akka.io/docs/alpakka-kafka/current/home.html
