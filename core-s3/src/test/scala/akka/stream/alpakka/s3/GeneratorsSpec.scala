package akka.stream.alpakka.s3

import akka.actor.ActorSystem
import io.aiven.guardian.kafka.s3.Generators
import org.scalatest.matchers.must.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class GeneratorsSpec extends AnyPropSpec with Matchers with ScalaCheckPropertyChecks {
  property("Bucket name generators generates valid bucket names according to S3Settings") {
    forAll(Generators.bucketNameGen) { bucket =>
      noException must be thrownBy BucketAndKey.validateBucketName(bucket, S3Settings(ActorSystem()))
    }
  }
}
