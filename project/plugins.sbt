addSbtPlugin("org.scalameta"                     % "sbt-scalafmt"             % "2.4.6")
addSbtPlugin("com.lightbend.paradox"             % "sbt-paradox"              % "0.9.2")
addSbtPlugin("com.lightbend.paradox"             % "sbt-paradox-apidoc"       % "0.10+12-1d5b87db")
addSbtPlugin("com.lightbend.paradox"             % "sbt-paradox-project-info" % "2.0.0")
addSbtPlugin("com.github.sbt"                    % "sbt-unidoc"               % "0.5.0")
addSbtPlugin("com.typesafe.sbt"                  % "sbt-ghpages"              % "0.6.3")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings"         % "3.0.2")
addSbtPlugin("com.typesafe.sbt"                  % "sbt-site"                 % "1.4.1")
addSbtPlugin("com.github.sbt"                    % "sbt-native-packager"      % "1.9.9")
addSbtPlugin("com.codecommit"                    % "sbt-github-actions"       % "0.14.2")
addSbtPlugin("com.github.sbt"                    % "sbt-pgp"                  % "2.1.2")
addSbtPlugin("com.github.sbt"                    % "sbt-release"              % "1.1.0")
addSbtPlugin("ch.epfl.scala"                     % "sbt-scalafix"             % "0.9.34")
addSbtPlugin("org.scoverage"                     % "sbt-scoverage"            % "1.9.3")
addSbtPlugin("org.scoverage"                     % "sbt-coveralls"            % "1.3.2")
addSbtPlugin("net.vonbuchholtz"                  % "sbt-dependency-check"     % "4.0.0")

// This is here to bump dependencies for sbt-paradox/sbt-site, see
// https://github.com/sirthias/parboiled/issues/175, https://github.com/sirthias/parboiled/issues/128 and
// https://github.com/sirthias/parboiled/pull/195
libraryDependencies ++= Seq(
  "org.parboiled" %% "parboiled-scala" % "1.4.1",
  "org.parboiled"  % "parboiled-java"  % "1.4.1"
)
