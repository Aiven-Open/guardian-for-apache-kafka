# Logging

The CLI provides its own default
logback `logback.xml` @github[logging file](/core-cli/src/main/resources/logback.xml) which has sane defaults for
typical usage. It's also possible to provide a custom `logback.xml` configuration file using the `--logback-file`
command line argument.

For more details about logback and/or the `logback.xml` configuration format read the
@ref:[general architecture section on logging](../general-architecture/logging.md).
