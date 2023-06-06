# Logging

Guardian for Apache Kafka uses [logback](https://logback.qos.ch/index.html) to perform logging. This means if you are
using the modules as libraries you need to provide a `logback.xml` in your classpath (typically this is done by putting
the `logback.xml` in your `/src/main/resources` folder). Note that the Guardian modules do not provide a default
`logback.xml` for deployed artifacts since this is typically the responsibility of an application to configure and
provide.

If you want examples of `logback.xml` configuration you can have a look at the
official [logback page](https://logback.qos.ch/manual/configuration.html) but you can also use existing `logback.xml`'s
from either the @github[cli](/core-cli/src/main/resources/logback.xml) or the
@github[tests](/core/src/test/resources/logback.xml) as a reference.

@@@ warning

As documented at @extref:[pekko logback configuration](pekko-docs:logging.html#logback-configuration) it is highly recommended
to use an `AsyncAppender` in your configuration as this offsets the logging to a background thread otherwise you will
end up blocking the core pekko/pekko-streams library whenever a log is made.

@@@

## Logback adapter for pekko/pekko-streams

By default, pekko/pekko-streams uses its own asynchronous logger however they provide a
@extref:[logging adapter](pekko-docs:logging.html#slf4j) which has already been preconfigured for use in Guardian.

## CLI/Application

Note that unlike the core libraries, the CLI application does provide a default `logback.xml`. For more details read
@ref:[application logging](../application/logging.md).
