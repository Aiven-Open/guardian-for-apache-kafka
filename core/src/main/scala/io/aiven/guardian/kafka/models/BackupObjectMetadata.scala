package io.aiven.guardian.kafka.models

final case class BackupObjectMetadata(compression: Option[CompressionType])

object BackupObjectMetadata {
  def fromKey(key: String): BackupObjectMetadata =
    if (key.endsWith(".gz"))
      BackupObjectMetadata(Some(Gzip))
    else
      BackupObjectMetadata(None)
}
