package akka.stream.alpakka.s3

import akka.actor.ActorSystem
import io.aiven.guardian.kafka.s3.Generators
import org.scalacheck.Gen
import org.scalatest.matchers.must.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class GeneratorsSpec extends AnyPropSpec with Matchers with ScalaCheckPropertyChecks {
  property("Bucket name generators generates valid bucket names according to S3Settings with virtualDotHost") {
    forAll(Generators.bucketNameGen(useVirtualDotHost = true)) { bucket =>
      noException must be thrownBy BucketAndKey.validateBucketName(bucket, S3Settings(ActorSystem()))
    }
  }

  property("Bucket name generators generates valid bucket names according to S3Settings without virtualDotHost") {
    forAll(Generators.bucketNameGen(useVirtualDotHost = false)) { bucket =>
      noException must be thrownBy BucketAndKey.validateBucketName(bucket, S3Settings(ActorSystem()))
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
      noException must be thrownBy BucketAndKey.validateBucketName(bucket, S3Settings(ActorSystem()))
    }
  }
}
