# Configuration

## Reference

@@snip (/core-restore/src/main/resources/reference.conf)

Scala API doc @apidoc[kafka.restore.configs.Restore]

## Explanation

* `pekko.kafka.producer`: See @extref:[documentation](pekko-connectors-kafka-docs:producer.html#settings)
* `pekko.kafka.producer.kafka-clients`: See @extref:[documentation](kafka-docs:documentation.html#producerconfigs)
* `restore`:
    * `from-when`: An `ISO-8601` time that specifies from when topics need to be restored. Note that the time used is
      based on the original Kafka timestamp and **NOT** the current time.
    * `override-topics`: A mapping of currently backed up topics to a new topic in the destination Kafka cluster
