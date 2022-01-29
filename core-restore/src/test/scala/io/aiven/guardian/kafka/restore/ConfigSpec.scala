package io.aiven.guardian.kafka.restore

import io.aiven.guardian.kafka.Generators.kafkaTopic
import io.aiven.guardian.kafka.restore.configs.Restore
import org.scalacheck.Gen
import org.scalacheck.ops.time.ImplicitJavaTimeGenerators._
import org.scalatest.matchers.must.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import pureconfig._
import pureconfig.configurable._
import pureconfig.generic.auto._

import scala.annotation.nowarn

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

class ConfigSpec extends AnyPropSpec with Matchers with ScalaCheckPropertyChecks {
  implicit val localDateConvert: ConfigConvert[OffsetDateTime] = offsetDateTimeConfigConvert(
    DateTimeFormatter.ISO_OFFSET_DATE_TIME
  )

  property("Valid Restore configs should parse correctly") {
    val overrideMapGen = for {
      size   <- Gen.choose(1, 10)
      keys   <- Gen.containerOfN[Set, String](size, kafkaTopic)
      values <- Gen.containerOfN[Set, String](size, kafkaTopic)
    } yield keys.zip(values).toMap

    val offsetDateTimeGen = arbZonedDateTime.arbitrary.map(_.toOffsetDateTime)

    forAll(offsetDateTimeGen, overrideMapGen) { (fromWhen: OffsetDateTime, overrideTopics: Map[String, String]) =>
      val topics = overrideTopics
        .map { case (key, value) =>
          val k = "\"" + key + "\""
          val v = "\"" + value + "\""
          s"$k=$v"
        }
        .mkString("", "\n      ", "")

      val conf = s"""
           |restore {
           |    from-when = "${fromWhen.toString}"
           |    override-topics = {
           |      $topics
           |    }
           |}
           |""".stripMargin

      @nowarn("cat=lint-byname-implicit")
      val restore = ConfigSource.string(conf).at("restore").loadOrThrow[Restore]
      restore mustEqual Restore(Some(fromWhen), Some(overrideTopics))
    }
  }

}
