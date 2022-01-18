package io.aiven.guardian.kafka.backup

import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.backup.configs.ChronoUnitSlice
import io.aiven.guardian.kafka.backup.configs.PeriodFromFirst
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalatest.matchers.must.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import scala.annotation.nowarn
import scala.concurrent.duration.FiniteDuration

import java.time.temporal.ChronoUnit

class ConfigSpec extends AnyPropSpec with Matchers with ScalaCheckPropertyChecks {
  implicit val chronoUnitArb: Arbitrary[ChronoUnit] = Arbitrary(
    Gen.oneOf(ChronoUnit.values().toList)
  )

  property("Valid Backup TimeConfiguration chrono-unit-slice configs should parse correctly") {
    forAll { (chronoUnit: ChronoUnit) =>
      val conf =
        s"""
           |backup {
           |    time-configuration = {
           |       type = chrono-unit-slice
           |       chrono-unit = ${chronoUnit.name.toLowerCase}
           |    }
           |}
           |""".stripMargin

      @nowarn("cat=lint-byname-implicit")
      val backup = ConfigSource.string(conf).at("backup").loadOrThrow[Backup]
      backup mustEqual Backup(ChronoUnitSlice(chronoUnit))
    }
  }

  property("Valid Backup TimeConfiguration period-from-first configs should parse correctly") {
    forAll { (finiteDuration: FiniteDuration) =>
      val conf =
        s"""
           |backup {
           |    time-configuration = {
           |       type = period-from-first
           |       duration = ${finiteDuration.toString()}
           |    }
           |}
           |""".stripMargin

      @nowarn("cat=lint-byname-implicit")
      val backup = ConfigSource.string(conf).at("backup").loadOrThrow[Backup]
      backup mustEqual Backup(PeriodFromFirst(finiteDuration))
    }
  }

  property("Default Backup configuration loads") {
    noException should be thrownBy Config.backupConfig
  }

}
