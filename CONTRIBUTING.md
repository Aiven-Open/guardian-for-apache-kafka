# Welcome!

Guardian for Apache Kafka follows the [fork and pull](https://help.github.com/articles/using-pull-requests/#fork--pull)
development model. You can simply fork the repository, create and checkout a new branch and commit changes to that
branch and then create a pull request once you are done.

Feel free to submit a PR earlier rather than later, this is recommended as it can spur discussion to see if you are on
the right track. If you create a PR before its ready, we recommend using github's
[draft](https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/changing-the-stage-of-a-pull-request)
feature to clear indicate that a PR is still being worked on.

## Setting up development environment

If you haven't already done so, before you get started you need to set up your machine for development. Guardian for
Apache Kafka is written in [Scala](https://www.scala-lang.org/) so a few steps are needed

## JDK

Guardian for Apache Kafka is developed on the latest stable branch of OpenJDK. For Windows and MacOS we recommend
using [AdoptOpenJDK][adopt-openjdk-link] to download the latest installer. For Linux its recommended installing
OpenJDK through your distribution (but you can also use [AdoptOpenJDK][adopt-openjdk-link] as a last resort)

## Scala and SBT
Once you have installed JDK having Scala and SBT installed is recommended. Although some IDE's (such as Intellij)
automatically handle Scala and SBT installation for you, it's still recommended having a standalone version so you can 
compile/test/run the project without an IDE/Editor. The Scala installation also comes with its own REPL which can aid in
development.

We recommend following the official [Scala2 documentation](https://www.scala-lang.org/download/scala2.html) on how to
install Scala

## Editors/IDE's
The following editors are recommended for development with Scala. Although It's possible to use other environments since
Sclaa is a strongly typed language using a well supported editor is beneficial.

### Intellij IDEA

[Intellij IDEA](https://www.jetbrains.com/idea/) is one of the most used editors for Scala development. Upon installing
of IDEA you need to install the [scala plugin](https://plugins.jetbrains.com/plugin/1347-scala) so it can recognize SBT
projects. After installation of the plugin you can simply open the cloned `guardian-for-apache-kafka` and it should setup
everything for you.

### Metals

[Metals][metals-link] is a Scala [LSP](https://en.wikipedia.org/wiki/Language_Server_Protocol) implementation that
supports various editors. The primary supported editor for [Metals][metals-link] is 
[Visual Studio Code](https://code.visualstudio.com/) along with relevant
[marketplace plugin](https://marketplace.visualstudio.com/items?itemName=scalameta.metals).

Note that other editors can also be used with metals, documentation can be found
[here](https://scalameta.org/metals/docs/). [Spacemacs](https://www.spacemacs.org/) an 
[Emacs](https://www.gnu.org/software/emacs/) distribution also supports [Metals][metals-link] via the 
[Scala layer](https://develop.spacemacs.org/layers/+lang/scala/README.html)

## Formatting

The codebase is formatted with [scalafmt](https://scalameta.org/scalafmt/). Various runners for Scalafmt exist, such as

* A [SBT scalafmt plugin](https://github.com/scalameta/sbt-scalafmt) that lets you run scalafmt directly within sbt using
  * `scalafmt` to format base scala sources
  * `test:scalafmt` to format test scala sources
  * `scalafmtSbt` to format the `build.sbt` file
* IntelliJ IDEA and VSCode will automatically detect projects with scalafmt and prompt you whether to use Scalafmt. See
the [scalafmt installation guide][scalafmt-installation-link] for more details
* There are native builds of Scalafmt that let you run a `scalafmt` as a CLI tool, see the CLI section in
[scalafmt installation guide][scalafmt-installation-link]

Note that a github action exists which will check that your PR is formatted when you create it. The check runs
separately ad in parallel to the main build/tests

## SBT - Compiling, Building and Testing

We use [SBT][sbt-link] as the primary build tool for the project. When you run [SBT][sbt-link] by itself
it will start a REPL session where you can type in commands, i.e.

* `compile` will compile the entire project
* `test:compile` will only compile the test sources
* `test` will run the tests for the entire project
* `core/compile` will only compile the `core` project. See [build.sbt](build.sbt) to get a reference for how the projects
are named
* `publishLocal` will publish the project into the local `~/.m2` repository

[adopt-openjdk-link]: https://adoptopenjdk.net/
[metals-link]: https://scalameta.org/metals/
[scalafmt-installation-link]: https://scalameta.org/scalafmt/docs/installation.html
[sbt-link]: https://www.scala-sbt.org/
