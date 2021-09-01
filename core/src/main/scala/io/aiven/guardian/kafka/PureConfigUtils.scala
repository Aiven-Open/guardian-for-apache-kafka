package io.aiven.guardian.kafka

import pureconfig.ConfigCursor
import pureconfig.error.CannotConvert
import pureconfig.error.ConfigReaderFailures
import pureconfig.error.ConvertFailure

object PureConfigUtils {
  private[kafka] def failure(cur: ConfigCursor, value: String, `type`: String) = ConfigReaderFailures(
    ConvertFailure(
      CannotConvert(value, `type`, s"Invalid ${`type`}"),
      cur
    )
  )
}
