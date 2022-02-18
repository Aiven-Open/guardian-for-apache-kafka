# Application

Guardian also becomes packaged as various application/s that lets you run it using a CLI interface. Currently, the
binaries provided are

* restore: A continuously running binary that performs the restore operation.
* backup: A binary which when executed allows you to restore an existing backup.

The CLI follows POSIX guidelines which means you can use `--help` as an argument to provide information on all of the
parameters.

## Package formats

Guardian is currently packaged using [sbt-native-packager](https://github.com/sbt/sbt-native-packager) to provide the
following formats by using the sbt shell.

* `rpm`
    * restore: `cliRestore/rpm:packageBin`. Created `rpm` file will be contained
      in `cli-restore/target/rpm/RPMS/noarch/`
    * backup: `cliBackup/rpm:packageBin`. Created `rpm` file will be contained in `cli-backup/target/rpm/RPMS/noarch/`
      NOTE: In order to build packages you need to have the [rpm-tools](https://rpm.org/) (specifically `rpmbuild`)
      installed and available on `PATH`. Please consult your Linux distribution for more info
* `zip`
    * restore: `cliRestore/universal:packageBin`. Created `zip` file will be contained
      in `cli-restore/target/universal/`
    * backup: `cliBackup/universal:packageBin`. Created `zip` file will be contained in `cli-backup/target/universal/`
* `tar`
    * restore: `cliRestore/universal:packageZipTarball`. Created `tar` file will be contained
      in `cli-restore/target/universal/`
    * backup: `cliBackup/universal:packageZipTarball`. Created `tar` file will be contained
      in `cli-backup/target/universal/`
* `Xz`
    * restore: `cliRestore/universal:packageXzTarball`. Created `xz` file will be contained
      in `cli-restore/target/universal/`
    * backup: `cliBackup/universal:packageXzTarball`. Created `xz` file will be contained
      in `cli-backup/target/universal/`

Note that for these packages formats you need to have JRE installed on your system to run the package. For more details
about packaging read the [docs](https://sbt-native-packager.readthedocs.io/en/latest/)

## Design

Each application is contained within a corresponding sbt submodule, i.e. the application for `backup` is contained
within the `cli-backup` sbt submodule. The `core-cli` sbt submodule contains common cli arguments (i.e. `kafka-topics`).

Scala packaging has been disabled for these submodules which means that when publishing/packaging Guardian it won't push
any built `.jar` files. This is because its unnecessary since you are meant to run these applications as a binary and
not include it as a library. By the same token this also means that the cli modules are built with global inlining
using `"-opt-inline-from:**"`, see [here](https://www.lightbend.com/blog/scala-inliner-optimizer) for more info.
