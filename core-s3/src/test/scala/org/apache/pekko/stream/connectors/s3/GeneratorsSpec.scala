package org.apache.pekko.stream.connectors.s3

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import io.aiven.guardian.kafka.s3.Generators
import org.scalacheck.Gen
import org.scalatest.matchers.must.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.annotation.nowarn

class GeneratorsSpec extends AnyPropSpec with Matchers with ScalaCheckPropertyChecks {

  def createBasicConfigFactory(virtualDotHost: Boolean): Config = {
    @nowarn("msg=possible missing interpolator: detected an interpolated expression")
    val baseS3SettingsConf =
      """
        |buffer = "memory"
        |disk-buffer-path = ""
        |
        |aws {
        |  credentials {
        |    provider = default
        |  }
        |  region {
        |    provider = default
        |  }
        |}
        |access-style = virtual
        |list-bucket-api-version = 2
        |validate-object-key = true
        |retry-settings {
        |  max-retries = 3
        |  min-backoff = 200ms
        |  max-backoff = 10s
        |  random-factor = 0.0
        |}
        |multipart-upload {
        |  retry-settings = ${retry-settings}
        |}
        |sign-anonymous-requests = true
        |""".stripMargin

    val config = ConfigFactory.parseString(baseS3SettingsConf).resolve()
    if (virtualDotHost)
      config.withValue("access-style", ConfigValueFactory.fromAnyRef("virtual"))
    else
      config.withValue("access-style", ConfigValueFactory.fromAnyRef("path"))
  }

  property("Bucket name generators generates valid bucket names according to S3Settings with virtualDotHost") {
    forAll(Generators.bucketNameGen(useVirtualDotHost = true)) { bucket =>
      noException must be thrownBy BucketAndKey.validateBucketName(bucket, S3Settings(createBasicConfigFactory(true)))
    }
  }

  property("Bucket name generators generates valid bucket names according to S3Settings without virtualDotHost") {
    forAll(Generators.bucketNameGen(useVirtualDotHost = false)) { bucket =>
      noException must be thrownBy BucketAndKey.validateBucketName(bucket, S3Settings(createBasicConfigFactory(false)))
    }
  }

  def withPrefixGen(useVirtualDotHost: Boolean): Gen[String] = for {
    range      <- Gen.choose(2, Generators.MaxBucketLength - 3)
    firstChar  <- Generators.bucketLetterOrNumberCharGen
    chars      <- Gen.listOfN(range, Generators.bucketAllCharGen(useVirtualDotHost = false))
    bucketName <- Generators.bucketNameGen(useVirtualDotHost, Some((firstChar +: chars).mkString))
  } yield bucketName

  property(
    "Bucket name generators generates valid bucket names according to S3Settings with virtualDotHost and prefix"
  ) {
    forAll(withPrefixGen(useVirtualDotHost = true)) { bucket =>
      noException must be thrownBy BucketAndKey.validateBucketName(bucket, S3Settings(createBasicConfigFactory(true)))
    }
  }
}
