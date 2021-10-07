package io.aiven.guardian.kafka.gcs

import io.aiven.guardian.kafka.gcs.configs.{GCS => GCSConfig}
import org.scalacheck.Gen

object Generators {
  // See https://cloud.google.com/storage/docs/naming-buckets for valid bucketnames. Note that even though bucket names
  // can theoretically contain dots this has to be verified against domain names so they have been removed

  lazy val bucketLetterOrNumberCharGen: Gen[Char] = Gen.frequency(
    (1, Gen.numChar),
    (1, Gen.alphaLowerChar)
  )

  lazy val bucketAllCharGen: Gen[Char] = Gen.frequency(
    (10, Gen.alphaLowerChar),
    (1, Gen.const('-')),
    (1, Gen.numChar)
  )

  lazy val bucketNameGen: Gen[String] = {
    for {
      range <- Gen.choose(3, 63)
      bucketName <- range match {
                      case 3 =>
                        for {
                          first  <- bucketLetterOrNumberCharGen
                          second <- bucketAllCharGen
                          third  <- bucketLetterOrNumberCharGen
                        } yield List(first, second, third).mkString
                      case _ =>
                        (for {
                          first  <- bucketLetterOrNumberCharGen
                          last   <- bucketLetterOrNumberCharGen
                          middle <- Gen.listOfN(range - 2, bucketAllCharGen)
                        } yield first.toString ++ middle.mkString ++ last.toString).filterNot { s =>
                          s.toLowerCase.contains("goog") || s.toLowerCase.contains("google") || s.toLowerCase.contains(
                            "g00g"
                          )
                        }
                    }
    } yield bucketName
  }

  val gcsConfigGen: Gen[GCSConfig] = (for {
    dataBucket       <- bucketNameGen
    compactionBucket <- bucketNameGen
  } yield GCSConfig(dataBucket, compactionBucket)).filter(config => config.dataBucket != config.compactionBucket)

}
