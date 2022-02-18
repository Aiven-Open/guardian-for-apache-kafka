import com.jsuereth.sbtpgp.PgpKeys.publishSigned

ThisBuild / scalaVersion         := "2.13.8"
ThisBuild / organization         := "aiven.io"
ThisBuild / organizationName     := "Aiven"
ThisBuild / organizationHomepage := Some(url("https://aiven.io/"))

ThisBuild / resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

val akkaVersion                = "2.6.18"
val akkaHttpVersion            = "10.2.8"
val alpakkaKafkaVersion        = "3.0.0"
val alpakkaVersion             = "3.0.4"
val quillJdbcMonixVersion      = "3.7.2"
val postgresqlJdbcVersion      = "42.3.3"
val scalaLoggingVersion        = "3.9.4"
val logbackClassicVersion      = "1.2.10"
val declineVersion             = "2.2.0"
val pureConfigVersion          = "0.17.1"
val scalaTestVersion           = "3.2.11"
val scalaTestScalaCheckVersion = "3.2.11.0"
val akkaStreamsJson            = "0.8.0"
val diffxVersion               = "0.7.0"
val testContainersVersion      = "0.40.2"
val testContainersJavaVersion  = "1.16.3"
val scalaCheckVersion          = "1.15.5-1-SNAPSHOT"
val scalaCheckOpsVersion       = "2.8.1"
val enumeratumVersion          = "1.7.0"
val organizeImportsVersion     = "0.6.0"

val flagsFor12 = Seq(
  "-Xlint:_",
  "-Ywarn-infer-any",
  "-Ywarn-adapted-args", // Warn if an argument list is modified to match the receiver
  "-Ywarn-inaccessible",
  "-Ywarn-infer-any",
  "-opt-inline-from:<sources>",
  "-opt:l:method"
)

val flagsFor13 = Seq(
  "-Xlint:_",
  "-opt-inline-from:<sources>",
  "-opt:l:method",
  "-Xfatal-warnings",
  "-Ywarn-unused",
  "-Xlint:adapted-args",
  "-Wconf:cat=unused:info"
)

val librarySettings = Seq(
  crossScalaVersions := List("2.12.15", "2.13.8"),
  scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, n)) if n == 13 =>
        flagsFor13
      case Some((2, n)) if n == 12 =>
        flagsFor12
    }
  }
)

val cliSettings = Seq(
  publishArtifact := false,
  scalacOptions ++= Seq(
    "-opt-inline-from:**", // See https://www.lightbend.com/blog/scala-inliner-optimizer
    "-opt:l:method"
  ) ++ flagsFor13,
  publish / skip       := true,
  publishLocal / skip  := true,
  publishSigned / skip := true,
  rpmVendor            := "Aiven",
  rpmLicense           := Some("ASL 2.0")
)

val baseName = "guardian-for-apache-kafka"

lazy val core = project
  .in(file("core"))
  .settings(
    librarySettings,
    name := s"$baseName-core",
    libraryDependencies ++= Seq(
      "com.typesafe.akka"          %% "akka-actor"                     % akkaVersion                % Provided,
      "com.typesafe.akka"          %% "akka-stream"                    % akkaVersion                % Provided,
      "com.typesafe.akka"          %% "akka-stream-kafka"              % alpakkaKafkaVersion,
      "com.typesafe.scala-logging" %% "scala-logging"                  % scalaLoggingVersion,
      "com.github.pureconfig"      %% "pureconfig"                     % pureConfigVersion,
      "ch.qos.logback"              % "logback-classic"                % logbackClassicVersion,
      "org.mdedetrich"             %% "akka-stream-circe"              % akkaStreamsJson,
      "com.typesafe.akka"          %% "akka-actor"                     % akkaVersion                % Test,
      "com.typesafe.akka"          %% "akka-stream"                    % akkaVersion                % Test,
      "org.scalatest"              %% "scalatest"                      % scalaTestVersion           % Test,
      "org.scalatestplus"          %% "scalacheck-1-15"                % scalaTestScalaCheckVersion % Test,
      "org.mdedetrich"             %% "scalacheck"                     % scalaCheckVersion          % Test,
      "com.rallyhealth"            %% "scalacheck-ops_1-15"            % scalaCheckOpsVersion       % Test,
      "com.softwaremill.diffx"     %% "diffx-scalatest-must"           % diffxVersion               % Test,
      "com.typesafe.akka"          %% "akka-stream-testkit"            % akkaVersion                % Test,
      "com.typesafe.akka"          %% "akka-http-testkit"              % akkaHttpVersion            % Test,
      "com.dimafeng"               %% "testcontainers-scala-scalatest" % testContainersVersion      % Test,
      "com.dimafeng"               %% "testcontainers-scala-kafka"     % testContainersVersion      % Test,
      "org.testcontainers"          % "kafka"                          % testContainersJavaVersion  % Test
    )
  )

lazy val coreCli = project
  .in(file("core-cli"))
  .settings(
    cliSettings,
    name := s"$baseName-core-cli",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor"  % akkaVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j"  % akkaVersion,
      "com.monovore"      %% "decline"     % declineVersion,
      "com.beachape"      %% "enumeratum"  % enumeratumVersion
    )
  )
  .dependsOn(core)

lazy val coreS3 = project
  .in(file("core-s3"))
  .settings(
    librarySettings,
    name := s"$baseName-s3",
    libraryDependencies ++= Seq(
      "com.lightbend.akka" %% "akka-stream-alpakka-s3" % alpakkaVersion,
      "org.scalatest"      %% "scalatest"              % scalaTestVersion           % Test,
      "org.scalatestplus"  %% "scalacheck-1-15"        % scalaTestScalaCheckVersion % Test,
      "com.typesafe.akka"  %% "akka-http-xml"          % akkaHttpVersion            % Test
    )
  )
  .dependsOn(core % "compile->compile;test->test")

lazy val coreGCS = project
  .in(file("core-gcs"))
  .settings(
    librarySettings,
    name := s"$baseName-gcs",
    libraryDependencies ++= Seq(
      "com.lightbend.akka" %% "akka-stream-alpakka-google-cloud-storage" % alpakkaVersion,
      "org.scalatest"      %% "scalatest"                                % scalaTestVersion           % Test,
      "org.scalatestplus"  %% "scalacheck-1-15"                          % scalaTestScalaCheckVersion % Test,
      "com.typesafe.akka"  %% "akka-http-spray-json"                     % akkaHttpVersion            % Test
    )
  )
  .dependsOn(core % "compile->compile;test->test")

lazy val coreBackup = project
  .in(file("core-backup"))
  .settings(
    librarySettings,
    name := s"$baseName-core-backup"
  )
  .dependsOn(core % "compile->compile;test->test")

lazy val backupS3 = project
  .in(file("backup-s3"))
  .settings(
    librarySettings,
    Test / fork := true,
    name        := s"$baseName-backup-s3"
  )
  .dependsOn(coreS3 % "compile->compile;test->test", coreBackup % "compile->compile;test->test")

lazy val backupGCS = project
  .in(file("backup-gcs"))
  .settings(
    librarySettings,
    Test / fork := true,
    name        := s"$baseName-backup-gcs"
  )
  .dependsOn(coreGCS % "compile->compile;test->test", coreBackup % "compile->compile;test->test")

lazy val cliBackup = project
  .in(file("cli-backup"))
  .settings(
    cliSettings,
    name := s"$baseName-cli-backup"
  )
  .dependsOn(coreCli   % "compile->compile;test->test",
             backupS3  % "compile->compile;test->test",
             backupGCS % "compile->compile;test->test"
  )
  .enablePlugins(JavaAppPackaging)

lazy val coreCompaction = project
  .in(file("core-compaction"))
  .settings(
    librarySettings,
    name := s"$baseName-core-compaction",
    libraryDependencies ++= Seq(
      "org.postgresql" % "postgresql" % postgresqlJdbcVersion
    )
  )
  .dependsOn(core)

lazy val compactionS3 = project
  .in(file("compaction-s3"))
  .settings(
    librarySettings,
    name := s"$baseName-compaction-s3"
  )
  .dependsOn(coreS3, coreCompaction)

lazy val compactionGCS = project
  .in(file("compaction-gcs"))
  .settings(
    librarySettings,
    name := s"$baseName-compaction-gcs"
  )
  .dependsOn(coreGCS, coreCompaction)

lazy val cliCompaction = project
  .in(file("cli-compaction"))
  .settings(
    cliSettings,
    name := s"$baseName-cli-compaction"
  )
  .dependsOn(coreCli, compactionS3, compactionGCS)
  .enablePlugins(JavaAppPackaging)

lazy val coreRestore = project
  .in(file("core-restore"))
  .settings(
    librarySettings,
    name := s"$baseName-core-restore"
  )
  .dependsOn(core % "compile->compile;test->test")
  .dependsOn(coreBackup % "test->test")

lazy val restoreS3 = project
  .in(file("restore-s3"))
  .settings(
    librarySettings,
    name := s"$baseName-restore-s3"
  )
  .dependsOn(coreRestore % "compile->compile;test->test", coreS3 % "compile->compile;test->test")
  .dependsOn(backupS3 % "test->compile")

lazy val restoreGCS = project
  .in(file("restore-gcs"))
  .settings(
    librarySettings,
    name := s"$baseName-restore-gcs"
  )
  .dependsOn(coreRestore % "compile->compile;test->test", coreGCS % "compile->compile;test->test")

lazy val cliRestore = project
  .in(file("cli-restore"))
  .settings(
    cliSettings,
    name := s"$baseName-cli-restore"
  )
  .dependsOn(coreCli    % "compile->compile;test->test",
             restoreS3  % "compile->compile;test->test",
             restoreGCS % "compile->compile;test->test"
  )
  .enablePlugins(JavaAppPackaging)

// This is currently causing problems, see https://github.com/djspiewak/sbt-github-actions/issues/74
ThisBuild / githubWorkflowUseSbtThinClient := false

ThisBuild / githubWorkflowTargetBranches := Seq("main") // Once we have branches per version, add the pattern here

ThisBuild / githubWorkflowPublishTargetBranches := Seq()

ThisBuild / githubWorkflowBuildPreamble := Seq(
  WorkflowStep.Sbt(List("scalafixAll --check"), name = Some("Linter: Scalafix checks"))
)

// Configuration needed for Scalafix
ThisBuild / scalafixDependencies += "com.github.liancheng" %% "organize-imports" % organizeImportsVersion

ThisBuild / scalafixScalaBinaryVersion := scalaBinaryVersion.value

ThisBuild / semanticdbEnabled := true

ThisBuild / githubWorkflowEnv ++= Map(
  "ALPAKKA_S3_REGION_PROVIDER"                   -> "static",
  "ALPAKKA_S3_REGION_DEFAULT_REGION"             -> "us-west-2",
  "ALPAKKA_S3_AWS_CREDENTIALS_PROVIDER"          -> "static",
  "ALPAKKA_S3_AWS_CREDENTIALS_ACCESS_KEY_ID"     -> "${{ secrets.AWS_ACCESS_KEY }}",
  "ALPAKKA_S3_AWS_CREDENTIALS_SECRET_ACCESS_KEY" -> "${{ secrets.AWS_SECRET_KEY }}"
)

ThisBuild / githubWorkflowJavaVersions := List(JavaSpec.temurin("11"))

import ReleaseTransformations._

releaseCrossBuild := true
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommandAndRemaining("+publishSigned"),
  releaseStepCommand("sonatypeReleaseAll"),
  setNextVersion,
  commitNextVersion,
  pushChanges
)
