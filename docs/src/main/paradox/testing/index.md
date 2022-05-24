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
* Akka provides @extref:[Testkit](akka-docs:testing.html#asynchronous-testing-testkit) with direct integration into ScalaTest
  for easy testing of akka-streams.

## TestContainers

[testcontainers](https://www.testcontainers.org/) along with the Scala
wrapper [testcontainers-scala](https://github.com/testcontainers/testcontainers-scala) is used to automate the spinning
up of [docker](https://www.docker.com/) whenever the relevant test is run. As long as you have docker installed on your
system you souldn't have to worry about anhything.

@@toc { depth=2 }

@@@ index

* [s3](s3.md)

@@@
