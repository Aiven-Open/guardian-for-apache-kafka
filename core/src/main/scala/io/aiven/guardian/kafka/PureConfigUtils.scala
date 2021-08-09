package io.aiven.guardian.kafka

import pureconfig.ConfigCursor
import pureconfig.error.{CannotConvert, ConfigReaderFailures, ConvertFailure}

object PureConfigUtils {
  private[kafka] def failure(cur: ConfigCursor, value: String, `type`: String) = ConfigReaderFailures(
    ConvertFailure(
      CannotConvert(value, `type`, s"Invalid ${`type`}"),
      cur
    )
  )
}
