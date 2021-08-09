package io.aiven.guardian.kafka

trait Errors extends Exception

object Errors {
  case object ExpectedStartOfSource extends Errors {
    override def getMessage: String = "Always expect a single element at the start of a stream"
  }
}
