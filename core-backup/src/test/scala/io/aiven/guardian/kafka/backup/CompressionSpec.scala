package io.aiven.guardian.kafka.backup

import akka.actor.ActorSystem
import akka.stream.scaladsl.Compression
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.SourceWithContext
import akka.util.ByteString
import io.aiven.guardian.akka.AnyPropTestKit
import io.aiven.guardian.kafka.backup.configs.{Compression => CompressionModel}
import io.aiven.guardian.kafka.models.Gzip
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.ExecutionContext

class CompressionSpec
    extends AnyPropTestKit(ActorSystem("CompressionSpec"))
    with Matchers
    with ScalaFutures
    with ScalaCheckPropertyChecks {

  implicit val ec: ExecutionContext = system.dispatcher

  property("Gzip compression works with a SourceWithContext/FlowWithContext") {
    forAll { data: List[String] =>
      val asByteString    = data.map(ByteString.fromString)
      val zippedWithIndex = asByteString.zipWithIndex
      val sourceWithContext = SourceWithContext.fromTuples(
        Source(zippedWithIndex)
      )

      val calculatedFuture = for {
        compressed <- sourceWithContext
                        .unsafeDataVia(BackupClientInterface.compressionFlow(CompressionModel(Gzip, None)))
                        .asSource
                        .map { case (byteString, _) => byteString }
                        .runFold(ByteString.empty)(_ ++ _)
        decompressed <- Source.single(compressed).via(Compression.gunzip()).runFold(ByteString.empty)(_ ++ _)
      } yield decompressed

      val decompressed = calculatedFuture.futureValue
      data.mkString mustEqual decompressed.utf8String
    }
  }
}
