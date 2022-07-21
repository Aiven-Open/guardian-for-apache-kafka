# Document Generation

Guardian uses [sbt-paradox][sbt-paradox-link] as the main plugin for generating documentation which is hosted
using [github pages][github-pages-link]. In addition various other plugins are used which are noted below

* [sbt-paradox-api-doc](https://github.com/lightbend/sbt-paradox-apidoc): Allows you to directly link to Scala
  documentation using the `@@apidoc` directive
* [sbt-paradox-project-info](https://github.com/lightbend/sbt-paradox-project-info): Provides an `@@projectInfo`
  directive that derives common information about the project (such as dependencies, project info etc etc)
* [sbt-site](https://github.com/sbt/sbt-site): Used in conjunction with [sbt-paradox][sbt-paradox-link] to generate the
  final site structure
* [sbt-ghpages](https://github.com/sbt/sbt-ghpages): Used for uploading the final site
  to [github-pages][github-pages-link].
* [sbt-unidoc](https://github.com/sbt/sbt-unidoc): Used to aggregate/concatenate documentation Scala API documentation
  from various sbt modules into a single documentation result

## Design

[sbt-paradox][sbt-paradox-link] generates documentation using standard [Markdown](https://www.markdownguide.org/). The
documentation can be found in the @github[docs-folder](/docs). Note that this folder also corresponds to a sbt-module
which is also named `docs` which also means that commands related to documentation are run in that sbt sub-project
(i.e. `docs/makeSite` generates the documentation site).

Guardian also uses [scaladoc][scaladoc-link] which is already included within Scala compiler/SBT to generate Scala API
documentation. [scaladoc][scaladoc-link] is analogous to Java's own [javadoc](https://en.wikipedia.org/wiki/Javadoc)
which generates API documentation that is written within the code itself.

One advantage of using [sbt-paradox][sbt-paradox-link] and its various plugins as the main driver for documentation
generation is it that checks at document generation (i.e. compile time) that the docs are well-formed. This checking
includes

* references to other links
* references to specific Scala API documentation directly using Scala classes/objects/traits
* TOC (table of contents) are well-formed (e.g. you don't have markdown files in `docs` which aren't referenced
  anywhere)
* references to versions from Guardians various Scala submodules are always up-to-date
* references to code snippets

[sbt-paradox-link]: https://github.com/lightbend/paradox
[github-pages-link]: https://pages.github.com/
[scaladoc-link]: https://docs.scala-lang.org/style/scaladoc.html
