package io.aiven.guardian.kafka.backup

import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.backup.configs.ChronoUnitSlice
import io.aiven.guardian.kafka.backup.configs.PeriodFromFirst
import io.aiven.guardian.kafka.backup.configs.TimeConfiguration
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

  property("Valid TimeConfiguration chrono-unit-slice configs should parse correctly") {
    forAll { (chronoUnit: ChronoUnit) =>
      val conf =
        s"""
           |time-configuration = {
           |    type = chrono-unit-slice
           |    chrono-unit = ${chronoUnit.name.toLowerCase}
           |}
           |""".stripMargin

      @nowarn("cat=lint-byname-implicit")
      val backup = ConfigSource.string(conf).at("time-configuration").loadOrThrow[TimeConfiguration]
      backup mustEqual ChronoUnitSlice(chronoUnit)
    }
  }

  property("Valid TimeConfiguration period-from-first configs should parse correctly") {
    forAll { (finiteDuration: FiniteDuration) =>
      val conf =
        s"""
           |time-configuration = {
           |    type = period-from-first
           |    duration = ${finiteDuration.toString()}
           |}
           |""".stripMargin

      @nowarn("cat=lint-byname-implicit")
      val backup = ConfigSource.string(conf).at("time-configuration").loadOrThrow[TimeConfiguration]
      backup mustEqual PeriodFromFirst(finiteDuration)
    }
  }

  property("Default Backup configuration loads") {
    val config = ConfigFactory.load()

    // Inject mandatory values that have no default into the configuration
    val configWithMandatoryValues =
      config.withValue("backup.kafka-group-id", ConfigValueFactory.fromAnyRef(MockedBackupClientInterface.KafkaGroupId))

    @nowarn("cat=lint-byname-implicit")
    def readConfiguration = ConfigSource.fromConfig(configWithMandatoryValues).at("backup").loadOrThrow[Backup]

    noException should be thrownBy readConfiguration
  }

}
