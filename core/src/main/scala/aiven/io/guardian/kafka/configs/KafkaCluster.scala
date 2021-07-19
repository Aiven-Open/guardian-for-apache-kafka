package aiven.io.guardian.kafka.configs

/** @param topics The set of topics to subscribe to (and hence backup and restore)
  */
final case class KafkaCluster(topics: Set[String])
