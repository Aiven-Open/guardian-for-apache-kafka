addSbtPlugin("org.scalameta"         % "sbt-scalafmt"         % "2.4.6")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox"          % "0.9.2")
addSbtPlugin("com.github.sbt"        % "sbt-native-packager"  % "1.9.9")
addSbtPlugin("com.codecommit"        % "sbt-github-actions"   % "0.14.2")
addSbtPlugin("com.github.sbt"        % "sbt-pgp"              % "2.1.2")
addSbtPlugin("com.github.sbt"        % "sbt-release"          % "1.1.0")
addSbtPlugin("ch.epfl.scala"         % "sbt-scalafix"         % "0.9.34")
addSbtPlugin("org.scoverage"         % "sbt-scoverage"        % "1.9.3")
addSbtPlugin("org.scoverage"         % "sbt-coveralls"        % "1.3.1")
addSbtPlugin("net.vonbuchholtz"      % "sbt-dependency-check" % "3.4.0")

// This is here due to https://github.com/scoverage/sbt-coveralls/issues/179. When
// sbt-coveralls releases a new version higher than 1.3.1 you can remove this
val jacksonDowngradeVersion = "2.12.5"
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core"    % "jackson-core"         % jacksonDowngradeVersion,
  "com.fasterxml.jackson.core"    % "jackson-databind"     % jacksonDowngradeVersion,
  "com.fasterxml.jackson.core"    % "jackson-annotations"  % jacksonDowngradeVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonDowngradeVersion
)
