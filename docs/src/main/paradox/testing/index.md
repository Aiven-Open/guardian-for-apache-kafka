# Testing

As much as possible, Guardian for Apache Kafka aims to provide as little friction as possible to run tests (ideally you
should be able to run tests directly and only in SBT). As an example this means avoiding handwritten shell scripts to
set up environments since this typically doesn't play well with IDE integrations such
as [Intellij IDEA](https://www.jetbrains.com/idea/) or [Metals](https://scalameta.org/metals/) integrated SBT test.
runner.

## ScalaTest

Guardian for Apache Kafka uses [scalatest](https://www.scalatest.org/) as its testing framework. The primary reasons for
using this testing framework are

* It's the most supported testing framework in Scala, so much so that its considered a critical dependency whenever a
  new Scala release is made
* It provides very handy utilities for testing asynchronous code, for example a
  @extref:[PatienceConfig](scalatest:concurrent/AbstractPatienceConfiguration$PatienceConfig.html)
  that provides efficient polling of Scala futures with configurable scalable timeouts and intervals.
* Akka provides @extref:[Testkit](akka-docs:testing.html#asynchronous-testing-testkit) with direct integration into
  ScalaTest for easy testing of akka-streams.

### Property based tests

Guardian for Apache Kafka emphasises using property based testing over unit based tests. This is mainly due
to the fact that property based tests often reveal more problems due to covering more cases compared to unit
based tests. Here are more [details](https://www.scalatest.org/user_guide/generator_driven_property_checks)
on how property based testing works with Scala.

Like most random data generation, ScalaTest/ScalaCheck relies on an initial seed to deterministically generate
the data. When a test fails the seed for the failing test is automatically shown (search for `Init Seed: `).
If you want to specify the seed to regenerate the exact same data that caused the test to fail, you need to
specify it as a test argument in `sbt`

```sbt
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-S", "7832168009826873070")
```

where `7832168009826873070` happens to be the seed

This argument can be put into any of the projects within the @github[build](/build.sbt). For example if you
want to only specify the speed in the `core` project you can place it like so

```sbt
lazy val core = project
  .in(file("core"))
  .settings(
    librarySettings,
    name := s"$baseName-core",
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-S", "7832168009826873070"),
```

Whereas if you want it to apply globally you can just place it in the `guardian` project.

## Running test/s until failure

When diagnosing flaky tests it's very useful to be able to run a test until it fails which sbt allows you to
do with [commands](https://www.scala-sbt.org/1.x/docs/Commands.html). Doing this using a sbt command
is far quicker than other options such a shell script since you don't have to deal with startup time cost for
every test run.

This is what the base command looks like

```sbt
commands += Command.command("testUntilFailed") { state =>
  "test" :: "testUntilFailed" :: state
}
```

The command will recursively call a specific task (in this case `test`) until it fails. For it to work with
Guardin for Apache Kafka's @github[build](/build.sbt), you need to place it as a setting
within the `guardian` project.

Note that this works with any command, not just `test`. For example if you want to only run a single test
suite until failure you can do

```sbt
commands += Command.command("testUntilFailed") { state =>
  "backupS3/testOnly io.aiven.guardian.kafka.backup.s3.MockedKafkaClientBackupClientSpec" :: "testUntilFailed" :: state
}
```

Once specified in the @github[build](/build.sbt) file you can then run `testUntilFailed` within the sbt shell.

## TestContainers

[testcontainers](https://www.testcontainers.org/) along with the Scala
wrapper [testcontainers-scala](https://github.com/testcontainers/testcontainers-scala) is used to automate the spinning
up of [docker](https://www.docker.com/) whenever the relevant test is run. As long as you have docker installed on your
system you souldn't have to worry about anhything.

@@toc { depth=2 }

@@@ index

* [s3](s3.md)

@@@
