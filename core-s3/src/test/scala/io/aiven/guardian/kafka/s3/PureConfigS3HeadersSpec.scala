package io.aiven.guardian.kafka.s3

import com.softwaremill.diffx.generic.auto._
import com.softwaremill.diffx.scalatest.DiffMustMatcher._
import io.aiven.guardian.kafka.s3.Config._
import org.apache.pekko
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalatest.matchers.must.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import pureconfig.ConfigReader.Result
import pureconfig.ConfigSource

import pekko.stream.connectors.s3.MetaHeaders
import pekko.stream.connectors.s3.S3Headers
import pekko.stream.connectors.s3.headers._

class PureConfigS3HeadersSpec extends AnyPropSpec with Matchers with ScalaCheckPropertyChecks {
  implicit val cannedAclArb: Arbitrary[CannedAcl] = Arbitrary(
    Gen.oneOf(
      List(
        CannedAcl.AuthenticatedRead,
        CannedAcl.AwsExecRead,
        CannedAcl.BucketOwnerFullControl,
        CannedAcl.BucketOwnerRead,
        CannedAcl.Private,
        CannedAcl.PublicRead,
        CannedAcl.PublicReadWrite
      )
    )
  )

  implicit val storageClassArb: Arbitrary[StorageClass] = Arbitrary(
    Gen.oneOf(
      List(
        StorageClass.Standard,
        StorageClass.InfrequentAccess,
        StorageClass.Glacier,
        StorageClass.ReducedRedundancy
      )
    )
  )

  implicit val aes256serverSideEncryptionArb: Gen[AES256] = Gen.const(ServerSideEncryption.aes256())
  implicit val kmsServerSideEncryptionArb: Gen[KMS] = for {
    keyId   <- Gen.alphaStr.filter(_.nonEmpty)
    context <- Gen.option(Gen.alphaStr.filter(_.nonEmpty))
  } yield {
    val base = ServerSideEncryption.kms(keyId)
    context.fold(base)(base.withContext)
  }
  implicit val customerKeysServerSideEncryptionArb: Gen[CustomerKeys] = for {
    key <- Gen.alphaStr.filter(_.nonEmpty)
    md5 <- Gen.option(Gen.alphaStr.filter(_.nonEmpty))
  } yield {
    val base = ServerSideEncryption.customerKeys(key)
    md5.fold(base)(base.withMd5)
  }

  implicit val serverSideEncryptionArb: Arbitrary[ServerSideEncryption] = Arbitrary(
    Gen.frequency(
      (1, aes256serverSideEncryptionArb),
      (1, kmsServerSideEncryptionArb),
      (1, customerKeysServerSideEncryptionArb)
    )
  )

  // According to https://stackoverflow.com/a/48138818 with HTTP header values
  // we should be dealing with ASCII chars however that would involve us
  // having to manually escape certain characters (at which point we are
  // just testing PureConfig is just parsing things correctly).
  val headersMapGen: Gen[Map[String, String]] = Gen
    .listOf(for {
      key   <- Gen.alphaStr.filter(_.nonEmpty)
      value <- Gen.alphaStr.filter(_.nonEmpty)
    } yield (key, value))
    .map(_.toMap)
    .filter(_.nonEmpty)

  implicit val metaHeadersArb: Arbitrary[MetaHeaders] = Arbitrary(
    headersMapGen.map(MetaHeaders.apply)
  )

  implicit val s3HeadersArb: Arbitrary[S3Headers] = Arbitrary(
    for {
      cannedAcl            <- Gen.option(cannedAclArb.arbitrary)
      metaHeaders          <- Gen.option(metaHeadersArb.arbitrary)
      storageClass         <- Gen.option(storageClassArb.arbitrary)
      customHeaders        <- Gen.option(headersMapGen)
      serverSideEncryption <- Gen.option(serverSideEncryptionArb.arbitrary)
    } yield {
      val base  = S3Headers()
      val base2 = cannedAcl.fold(base)(base.withCannedAcl)
      val base3 = metaHeaders.fold(base2)(base2.withMetaHeaders)
      val base4 = storageClass.fold(base3)(base3.withStorageClass)
      val base5 = customHeaders.fold(base4)(base4.withCustomHeaders)
      serverSideEncryption.fold(base5)(base5.withServerSideEncryption)
    }
  )

  def configCannedAcl(cannedAcl: CannedAcl): String =
    cannedAcl.value

  def configMetaHeaders(metaHeaders: MetaHeaders): String =
    metaHeaders.metaHeaders.map { case (k, v) => s"$k=$v" }.mkString("\n")

  def configStorageClass(storageClass: StorageClass): String =
    storageClass.storageClass

  def configCustomHeaders(customHeaders: Map[String, String]): String =
    customHeaders.map { case (k, v) => s"$k=$v" }.mkString("\n")

  def configServerSideEncryption(serverSideEncryption: ServerSideEncryption): String =
    serverSideEncryption match {
      case _: AES256 => "type=aes256"
      case kms: KMS =>
        s"""
           |type=kms
           |key-id=${kms.keyId}
           |${kms.context.fold("")(c => s"context=$c")}
           |""".stripMargin
      case keys: CustomerKeys =>
        s"""
           |type=customer-keys
           |key=${keys.key}
           |${keys.md5.fold("")(c => s"md5=$c")}
           |""".stripMargin
    }

  property("Valid CannedAcl configs should parse correctly") {
    forAll { (cannedAcl: CannedAcl) =>
      ConfigSource.string(s"test=${configCannedAcl(cannedAcl)}").at("test").load[CannedAcl] mustMatchTo (
        Right(cannedAcl): Result[CannedAcl]
      )
    }
  }

  property("Valid MetaHeaders configs should parse correctly") {
    forAll { (metaHeaders: MetaHeaders) =>
      ConfigSource.string(configMetaHeaders(metaHeaders)).load[MetaHeaders] mustMatchTo (
        Right(metaHeaders): Result[MetaHeaders]
      )
    }
  }

  property("Valid StorageClass configs should parse correctly") {
    forAll { (storageClass: StorageClass) =>
      ConfigSource.string(s"test=${configStorageClass(storageClass)}").at("test").load[StorageClass] mustMatchTo (
        Right(storageClass): Result[StorageClass]
      )
    }
  }

  property("Valid ServerSideEncryption configs should parse correctly") {
    forAll { (serverSideEncryption: ServerSideEncryption) =>
      ConfigSource.string(configServerSideEncryption(serverSideEncryption)).load[ServerSideEncryption] mustMatchTo (
        Right(serverSideEncryption): Result[ServerSideEncryption]
      )
    }
  }

  property("Valid S3Headers configs should parse correctly") {
    forAll { (s3Headers: S3Headers) =>
      val string = s"""
          |${s3Headers.cannedAcl.fold("")(cannedAcl => s"canned-acl=${configCannedAcl(cannedAcl)}")}
          |${s3Headers.metaHeaders.fold("")(metaHeaders => s"meta-headers={\n${configMetaHeaders(metaHeaders)}\n}")}
          |${s3Headers.storageClass.fold("")(storageClass => s"storage-class=${configStorageClass(storageClass)}")}
          |${if (s3Headers.customHeaders.isEmpty) ""
                     else s"custom-headers={\n${configCustomHeaders(s3Headers.customHeaders)}\n}"}
          |${s3Headers.serverSideEncryption.fold("")(serverSideEncryption =>
                       s"server-side-encryption={\n${configServerSideEncryption(serverSideEncryption)}\n}"
                     )}
          |""".stripMargin
      ConfigSource.string(string).load[S3Headers] mustMatchTo (
        Right(s3Headers): Result[S3Headers]
      )
    }
  }
}
