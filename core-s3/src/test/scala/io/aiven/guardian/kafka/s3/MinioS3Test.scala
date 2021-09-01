package io.aiven.guardian.kafka.s3

import akka.stream.alpakka.s3.AccessStyle
import akka.stream.alpakka.s3.S3Settings
import akka.testkit.TestKitBase
import com.dimafeng.testcontainers.ForAllTestContainer
import org.scalatest.Suite
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.AwsRegionProvider

trait MinioS3Test extends ForAllTestContainer with TestKitBase { this: Suite =>
  private val S3DummyAccessKey = "DUMMY_ACCESS_KEY"
  private val S3DummySecretKey = "DUMMY_SECRET_KEY"

  lazy val s3Settings: S3Settings = S3Settings()
    .withEndpointUrl(s"http://${container.getHostAddress}")
    .withCredentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(S3DummyAccessKey, S3DummySecretKey))
    )
    .withS3RegionProvider(new AwsRegionProvider {
      lazy val getRegion: Region = Region.US_EAST_1
    })
    .withAccessStyle(AccessStyle.PathAccessStyle)

  override val container: MinioContainer = new MinioContainer(S3DummyAccessKey, S3DummySecretKey)
}
