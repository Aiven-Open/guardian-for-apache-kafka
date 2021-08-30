package io.aiven.guardian.kafka.s3

import org.scalacheck.Gen

import scala.annotation.nowarn

object Generators {
  // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html for valid
  // bucketnames

  lazy val bucketLetterOrNumberCharGen: Gen[Char] = Gen.frequency(
    (1, Gen.numChar),
    (1, Gen.alphaLowerChar)
  )

  lazy val bucketAllCharGen: Gen[Char] = Gen.frequency(
    (10, Gen.alphaLowerChar),
    (1, Gen.const('.')),
    (1, Gen.const('-')),
    (1, Gen.numChar)
  )

  @nowarn("msg=not.*?exhaustive")
  private def checkInvalidDuplicateChars(chars: List[Char]): Boolean =
    chars.sliding(2).forall { case Seq(before, after) =>
      !(before == '.' && after == '.' || before == '-' && after == '.' || before == '.' && after == '-')
    }

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
                        for {
                          first  <- bucketLetterOrNumberCharGen
                          last   <- bucketLetterOrNumberCharGen
                          middle <- Gen.listOfN(range - 2, bucketAllCharGen).filter(checkInvalidDuplicateChars)
                        } yield first.toString ++ middle.mkString ++ last.toString
                    }
    } yield bucketName
  }
}
