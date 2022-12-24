addSbtPlugin("org.scalameta"                     % "sbt-scalafmt"             % "2.5.0")
addSbtPlugin("com.lightbend.paradox"             % "sbt-paradox"              % "0.10.3")
addSbtPlugin("com.lightbend.paradox"             % "sbt-paradox-apidoc"       % "0.10+12-1d5b87db")
addSbtPlugin("com.lightbend.paradox"             % "sbt-paradox-project-info" % "3.0.0")
addSbtPlugin("com.github.sbt"                    % "sbt-unidoc"               % "0.5.0")
addSbtPlugin("com.github.sbt"                    % "sbt-ghpages"              % "0.7.0")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings"         % "3.0.2")
addSbtPlugin("com.typesafe.sbt"                  % "sbt-site"                 % "1.4.1")
addSbtPlugin("com.github.sbt"                    % "sbt-native-packager"      % "1.9.11")
addSbtPlugin("com.codecommit"                    % "sbt-github-actions"       % "0.14.2")
addSbtPlugin("com.github.sbt"                    % "sbt-pgp"                  % "2.2.1")
addSbtPlugin("com.github.sbt"                    % "sbt-release"              % "1.1.0")
addSbtPlugin("ch.epfl.scala"                     % "sbt-scalafix"             % "0.10.4")
addSbtPlugin("org.scoverage"                     % "sbt-scoverage"            % "2.0.6")
addSbtPlugin("org.scoverage"                     % "sbt-coveralls"            % "1.3.5")
addSbtPlugin("net.vonbuchholtz"                  % "sbt-dependency-check"     % "4.2.0")

// This is here to bump dependencies for sbt-paradox/sbt-site, see
// https://github.com/sirthias/parboiled/issues/175, https://github.com/sirthias/parboiled/issues/128 and
// https://github.com/sirthias/parboiled/pull/195
libraryDependencies ++= Seq(
  "org.parboiled" %% "parboiled-scala" % "1.4.1",
  "org.parboiled"  % "parboiled-java"  % "1.4.1"
)

// See https://github.com/akka/akka-http/pull/3995 and https://github.com/akka/akka-http/pull/3995#issuecomment-1026978593
libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % "always"
