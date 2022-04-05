# Design

Each application is contained within a corresponding sbt submodule, i.e. the application for `backup` is contained
within the `cli-backup` sbt submodule. The `core-cli` sbt submodule contains common cli arguments (i.e. `kafka-topics`).

Scala packaging has been disabled for these submodules which means that when publishing/packaging Guardian it won't push
any built `.jar` files. This is because its unnecessary since you are meant to run these applications as a binary and
not include it as a library. By the same token this also means that the cli modules are built with global inlining
using `"-opt-inline-from:**"`, see [here](https://www.lightbend.com/blog/scala-inliner-optimizer) for more info.
