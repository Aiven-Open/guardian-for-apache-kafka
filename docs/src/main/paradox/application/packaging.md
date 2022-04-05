# Packaging

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
