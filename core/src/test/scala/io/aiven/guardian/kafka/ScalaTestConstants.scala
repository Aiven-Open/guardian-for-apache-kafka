package io.aiven.guardian.kafka

import scala.concurrent.duration._
import scala.language.postfixOps

trait ScalaTestConstants {
  val AwaitTimeout: FiniteDuration = 10 minutes
}
