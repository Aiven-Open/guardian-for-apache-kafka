package io.aiven.guardian.cli.arguments

import cats.data.ValidatedNel
import cats.implicits._
import com.monovore.decline.Argument
import enumeratum._

sealed trait StorageOpt extends EnumEntry with EnumEntry.Lowercase

object StorageOpt extends Enum[StorageOpt] {
  case object S3 extends StorageOpt

  val values: IndexedSeq[StorageOpt] = findValues

  implicit val storageArgument: Argument[StorageOpt] = new Argument[StorageOpt] {
    override def read(string: String): ValidatedNel[String, StorageOpt] =
      StorageOpt.withNameOption(string).toValidNel("Invalid Storage Argument")

    override def defaultMetavar: String = "storage"
  }

}
