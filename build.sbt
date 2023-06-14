import com.jsuereth.sbtpgp.PgpKeys.publishSigned
import com.lightbend.paradox.apidoc.ApidocPlugin.autoImport.apidocRootPackage
import sbtghactions.{Permissions, PermissionScope, PermissionValue}

ThisBuild / scalaVersion         := "2.13.11"
ThisBuild / organization         := "aiven.io"
ThisBuild / organizationName     := "Aiven"
ThisBuild / organizationHomepage := Some(url("https://aiven.io/"))

ThisBuild / resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

// TODO: Remove when Pekko has a proper release
ThisBuild / resolvers += Resolver.ApacheMavenSnapshotsRepo
ThisBuild / updateOptions := updateOptions.value.withLatestSnapshots(false)

val pekkoVersion                = "0.0.0+26669-ec5b6764-SNAPSHOT"
val pekkoHttpVersion            = "0.0.0+4411-6fe04045-SNAPSHOT"
val pekkoConnectorsKafkaVersion = "0.0.0+1738-07a19b8e-SNAPSHOT"
val kafkaClientsVersion         = "3.4.1"
val pekkoConnectorsVersion      = "0.0.0+85-a82f3c3c-SNAPSHOT"
val futilesVersion              = "2.0.2"
val quillJdbcMonixVersion       = "3.7.2"
val postgresqlJdbcVersion       = "42.6.0"
val scalaLoggingVersion         = "3.9.5"
val logbackClassicVersion       = "1.4.7"
val declineVersion              = "2.4.1"
val pureConfigVersion           = "0.17.4"
val scalaTestVersion            = "3.2.16"
val scalaTestScalaCheckVersion  = s"$scalaTestVersion.0"
val pekkoStreamCirceVersion     = "0.0.0+94-dbf3173f-SNAPSHOT"
val diffxVersion                = "0.8.3"
val testContainersVersion       = "0.40.16"
val testContainersJavaVersion   = "1.18.3"
val scalaCheckVersion           = "1.17.0"
val scalaCheckOpsVersion        = "2.10.0"
val enumeratumVersion           = "1.7.2"
val organizeImportsVersion      = "0.6.0"

/** Calculates the scalatest version in a format that is used for `org.scalatestplus` scalacheck artifacts
  *
  * @see
  *   https://www.scalatest.org/user_guide/property_based_testing
  */
def scalaTestPlusScalaCheckVersion(version: String) =
  version.split('.').take(2).mkString("-")

val scalaTestScalaCheckArtifact = s"scalacheck-${scalaTestPlusScalaCheckVersion(scalaCheckVersion)}"

// See https://github.com/akka/akka-http/pull/3995 and https://github.com/akka/akka-http/pull/3995#issuecomment-1026978593
ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % "always"

val flagsFor12 = Seq(
  "-Xlint:_",
  "-Ywarn-infer-any",
  "-Ywarn-adapted-args", // Warn if an argument list is modified to match the receiver
  "-Ywarn-inaccessible",
  "-Ywarn-infer-any",
  "-language:existentials",
  "-opt-inline-from:<sources>",
  "-opt:l:method"
)

val flagsFor13 = Seq(
  "-Xlint:_",
  "-Xfatal-warnings",
  "-Ywarn-unused",
  "-Xlint:adapted-args",
  "-Wconf:cat=unused:info",
  "-language:existentials",
  "-opt-inline-from:<sources>",
  "-opt:l:method"
)

val librarySettings = Seq(
  crossScalaVersions := List("2.13.8"),
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

lazy val guardian = project
  .in(file("."))
  .enablePlugins(ScalaUnidocPlugin)
  .disablePlugins(SitePlugin)
  .aggregate(
    core,
    coreCli,
    coreS3,
    coreGCS,
    coreBackup,
    backupS3,
    backupGCS,
    cliBackup,
    coreCompaction,
    compactionS3,
    compactionGCS,
    cliCompaction,
    coreRestore,
    restoreS3,
    restoreGCS,
    cliRestore
  )
  .settings(
    name               := s"$baseName-root",
    publish / skip     := true,
    crossScalaVersions := List() // workaround for https://github.com/sbt/sbt/issues/3465
  )

lazy val core = project
  .in(file("core"))
  .settings(
    librarySettings,
    name := s"$baseName-core",
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-actor"            % pekkoVersion,
      "org.apache.pekko" %% "pekko-stream"           % pekkoVersion,
      "org.apache.pekko" %% "pekko-connectors-kafka" % pekkoConnectorsKafkaVersion,
      // Ideally we shouldn't be explicitly providing a kafka-clients version and instead getting the version
      // transitively from pekko-connectors-kafka however there isn't a nice way to extract a transitive dependency
      // for usage in linking to documentation.
      "org.apache.kafka"            % "kafka-clients"                  % kafkaClientsVersion,
      "com.typesafe.scala-logging" %% "scala-logging"                  % scalaLoggingVersion,
      "com.github.pureconfig"      %% "pureconfig"                     % pureConfigVersion,
      "ch.qos.logback"              % "logback-classic"                % logbackClassicVersion,
      "org.mdedetrich"             %% "pekko-stream-circe"             % pekkoStreamCirceVersion,
      "com.markatta"               %% "futiles"                        % futilesVersion,
      "org.apache.pekko"           %% "pekko-actor"                    % pekkoVersion               % Test,
      "org.apache.pekko"           %% "pekko-stream"                   % pekkoVersion               % Test,
      "org.scalatest"              %% "scalatest"                      % scalaTestVersion           % Test,
      "org.scalatestplus"          %% scalaTestScalaCheckArtifact      % scalaTestScalaCheckVersion % Test,
      "org.scalacheck"             %% "scalacheck"                     % scalaCheckVersion          % Test,
      "com.rallyhealth"            %% "scalacheck-ops_1-16"            % scalaCheckOpsVersion       % Test,
      "com.softwaremill.diffx"     %% "diffx-scalatest-must"           % diffxVersion               % Test,
      "org.apache.pekko"           %% "pekko-stream-testkit"           % pekkoVersion               % Test,
      "org.apache.pekko"           %% "pekko-http-testkit"             % pekkoHttpVersion           % Test,
      "com.dimafeng"               %% "testcontainers-scala-scalatest" % testContainersVersion      % Test,
      "com.dimafeng"               %% "testcontainers-scala-kafka"     % testContainersVersion      % Test,
      "org.testcontainers"          % "kafka"                          % testContainersJavaVersion  % Test
    )
  )

lazy val coreCli = project
  .in(file("core-cli"))
  .settings(
    publish / skip       := true,
    publishLocal / skip  := true,
    publishSigned / skip := true,
    scalacOptions ++= Seq(
      "-opt-inline-from:**", // See https://www.lightbend.com/blog/scala-inliner-optimizer
      "-opt:l:method"
    ) ++ flagsFor13,
    name := s"$baseName-core-cli",
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-actor"  % pekkoVersion,
      "org.apache.pekko" %% "pekko-stream" % pekkoVersion,
      "org.apache.pekko" %% "pekko-slf4j"  % pekkoVersion,
      "com.monovore"     %% "decline"      % declineVersion,
      "com.beachape"     %% "enumeratum"   % enumeratumVersion
    )
  )
  .dependsOn(core)

lazy val coreS3 = project
  .in(file("core-s3"))
  .settings(
    librarySettings,
    name := s"$baseName-s3",
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-connectors-s3" % pekkoConnectorsVersion,
      // Ordinarily this would be in Test scope however if its not then a lower version of pekko-http-xml which has a
      // security vulnerability gets resolved in Compile scope
      "org.apache.pekko"  %% "pekko-http-xml"            % pekkoHttpVersion,
      "org.scalatest"     %% "scalatest"                 % scalaTestVersion           % Test,
      "org.scalatestplus" %% scalaTestScalaCheckArtifact % scalaTestScalaCheckVersion % Test,
      "com.monovore"      %% "decline"                   % declineVersion             % Test
    )
  )
  .dependsOn(core % "compile->compile;test->test")

lazy val coreGCS = project
  .in(file("core-gcs"))
  .settings(
    librarySettings,
    name := s"$baseName-gcs",
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-connectors-google-cloud-storage" % pekkoConnectorsVersion,
      // Ordinarily this would be in Test scope however if its not then a lower version of pekko-http-spray-json which
      // has a security vulnerability gets resolved in Compile scope
      "org.apache.pekko"  %% "pekko-http-spray-json"     % pekkoHttpVersion,
      "org.scalatest"     %% "scalatest"                 % scalaTestVersion           % Test,
      "org.scalatestplus" %% scalaTestScalaCheckArtifact % scalaTestScalaCheckVersion % Test
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
    name                 := s"$baseName-cli-backup",
    executableScriptName := "guardian-backup"
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
    name                 := s"$baseName-cli-compaction",
    executableScriptName := "guardian-compaction"
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
    name                 := s"$baseName-cli-restore",
    executableScriptName := "guardian-restore"
  )
  .dependsOn(coreCli    % "compile->compile;test->test",
             restoreS3  % "compile->compile;test->test",
             restoreGCS % "compile->compile;test->test"
  )
  .enablePlugins(JavaAppPackaging)

def binaryVersion(key: String): String = key.substring(0, key.lastIndexOf('.'))

lazy val docs = project
  .enablePlugins(ParadoxPlugin, ParadoxSitePlugin, PreprocessPlugin, GhpagesPlugin)
  .settings(
    Compile / paradox / name     := "Guardian for Apache Kafka",
    publish / skip               := true,
    makeSite                     := makeSite.dependsOn(LocalRootProject / ScalaUnidoc / doc).value,
    previewPath                  := (Paradox / siteSubdirName).value,
    paradoxTheme                 := Some(builtinParadoxTheme("generic")),
    apidocRootPackage            := "io.aiven.guardian",
    Preprocess / siteSubdirName  := s"api/${projectInfoVersion.value}",
    Preprocess / sourceDirectory := (LocalRootProject / ScalaUnidoc / unidoc / target).value,
    git.remoteRepo               := scmInfo.value.get.connection.replace("scm:git:", ""),
    paradoxGroups                := Map("Language" -> Seq("Scala")),
    paradoxProperties ++= Map(
      "pekko.version"              -> pekkoVersion,
      "pekko-http.version"         -> pekkoHttpVersion,
      "pekko-stream-circe.version" -> pekkoStreamCirceVersion,
      "pure-config.version"        -> pureConfigVersion,
      "decline.version"            -> declineVersion,
      "scala-logging.version"      -> scalaLoggingVersion,
      // TODO: Replace current with binaryVersion(pekkoVersion) when pekko is released
      "extref.pekko.base_url"                 -> s"https://pekko.apache.org/api/pekko/current/%s",
      "extref.pekko-docs.base_url"            -> s"https://pekko.apache.org/docs/pekko/current/%s",
      "extref.pekko-stream-circe.base_url"    -> s"https://github.com/mdedetrich/pekko-streams-circe",
      "extref.pekko-connectors.base_url"      -> s"https://pekko.apache.org/api/pekko-connectors/current/%s",
      "extref.pekko-connectors-docs.base_url" -> s"https://pekko.apache.org/docs/pekko-connectors/current/%s",
      "extref.pekko-connectors-kafka-docs.base_url" -> s"https://pekko.apache.org/docs/pekko-connectors-kafka/current/%s",
      "extref.kafka-docs.base_url" -> s"https://kafka.apache.org/${binaryVersion(kafkaClientsVersion).replace(".", "")}/%s",
      "extref.pureconfig.base_url" -> s"https://pureconfig.github.io/docs/",
      "extref.scalatest.base_url"  -> s"https://www.scalatest.org/scaladoc/$scalaTestVersion/org/scalatest/%s",
      "github.base_url" -> s"https://github.com/aiven/guardian-for-apache-kafka/tree/${if (isSnapshot.value) "main"
        else "v" + version.value}",
      "scaladoc.io.aiven.guardian.base_url" -> s"/guardian-for-apache-kafka/${(Preprocess / siteSubdirName).value}/"
    ),
    Compile / paradoxMarkdownToHtml / sourceGenerators += Def.taskDyn {
      val targetFile = (Compile / paradox / sourceManaged).value / "license-report.md"

      (LocalRootProject / dumpLicenseReportAggregate).map { dir =>
        IO.copy(List(dir / s"$baseName-root-licenses.md" -> targetFile)).toList
      }
    }.taskValue
  )

ThisBuild / homepage := Some(url("https://github.com/aiven/guardian-for-apache-kafka"))

ThisBuild / scmInfo := Some(
  ScmInfo(url("https://github.com/aiven/guardian-for-apache-kafka"),
          "scm:git:git@github.com:aiven/guardian-for-apache-kafka.git"
  )
)

ThisBuild / startYear := Some(2021)

ThisBuild / developers := List(
  Developer("ahmedsobeh", "Ahmed Sobeh", "ahmed.sobeh@aiven.io", url("https://github.com/ahmedsobeh")),
  Developer("jlprat", "Josep Prat", "josep.prat@aiven.io", url("https://github.com/jlprat")),
  Developer("mdedetrich", "Matthew de Detrich", "matthew.dedetrich@aiven.io", url("https://github.com/mdedetrich")),
  Developer("reta", "Andriy Redko", "andriy.redko@aiven.io", url("https://github.com/reta")),
  Developer("RyanSkraba", "Ryan Skraba", "ryan.skraba@aiven.io", url("https://github.com/RyanSkraba"))
)

maintainer := "matthew.dedetrich@aiven.io"

ThisBuild / licenses += ("Apache-2.0", url("https://opensource.org/licenses/Apache-2.0"))

// This is currently causing problems, see https://github.com/djspiewak/sbt-github-actions/issues/74
ThisBuild / githubWorkflowUseSbtThinClient := false

ThisBuild / githubWorkflowTargetBranches := Seq("main")

// Once we have branches per version, add the pattern here, see
// https://github.com/djspiewak/sbt-github-actions#integration-with-sbt-ci-release
ThisBuild / githubWorkflowPublishTargetBranches := Seq(RefPredicate.Equals(Ref.Branch("main")))

ThisBuild / githubWorkflowPublish := Seq(WorkflowStep.Sbt(List("docs/ghpagesPushSite")))
ThisBuild / githubWorkflowPublishPreamble := Seq(
  // Taken from https://github.com/actions/checkout/issues/13#issue-481453396
  WorkflowStep.Run(
    commands = List(
      "git config --global user.name \"$(git --no-pager log --format=format:'%an' -n 1)\"",
      "git config --global user.email \"$(git --no-pager log --format=format:'%ae' -n 1)\""
    )
  ),
  WorkflowStep.Use(
    ref = UseRef.Public("webfactory", "ssh-agent", "v0.5.4"),
    params = Map(
      "ssh-private-key" -> "${{ secrets.GH_PAGES_SSH_PRIVATE_KEY }}"
    )
  )
)

ThisBuild / githubWorkflowBuildPreamble := Seq(
  WorkflowStep.Sbt(List("scalafixAll --check"), name = Some("Linter: Scalafix checks"))
)

// Configuration needed for Scalafix
ThisBuild / scalafixDependencies += "com.github.liancheng" %% "organize-imports" % organizeImportsVersion

ThisBuild / scalafixScalaBinaryVersion := scalaBinaryVersion.value

ThisBuild / semanticdbEnabled := true

// See https://scalacenter.github.io/scalafix/docs/users/installation.html#sbt
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

ThisBuild / githubWorkflowJavaVersions := List(JavaSpec.temurin("11"))

ThisBuild / githubWorkflowBuildPreamble ++= Seq(
  WorkflowStep.Use(
    UseRef.Public("aws-actions", "configure-aws-credentials", "v2"),
    name = Some("Configure AWS credentials"),
    params = Map(
      "role-to-assume"        -> "arn:aws:iam::310017459104:role/aiven-guardian-github-action",
      "aws-region"            -> "us-west-2",
      "role-duration-seconds" -> "7200" // 4 hours
    )
  )
)

ThisBuild / githubWorkflowPermissions := Some(
  Permissions.Specify(
    Map(
      PermissionScope.IdToken -> PermissionValue.Write
    )
  )
)

ThisBuild / githubWorkflowBuild := Seq(
  WorkflowStep.Sbt(
    List("clean", "coverage", "test"),
    name = Some("Build project"),
    env = Map(
      "PEKKO_CONNECTORS_S3_REGION_PROVIDER"          -> "default",
      "PEKKO_CONNECTORS_S3_AWS_CREDENTIALS_PROVIDER" -> "default"
    )
  ),
  WorkflowStep.Sbt(List("docs/makeSite"), name = Some("Compile docs"))
)

ThisBuild / githubWorkflowBuildPostamble ++= Seq(
  // See https://github.com/scoverage/sbt-coveralls#github-actions-integration
  WorkflowStep.Sbt(
    List("coverageReport", "coverageAggregate", "coveralls"),
    name = Some("Upload coverage data to Coveralls"),
    env = Map(
      "COVERALLS_REPO_TOKEN" -> "${{ secrets.GITHUB_TOKEN }}",
      "COVERALLS_FLAG_NAME"  -> "Scala ${{ matrix.scala }}"
    )
  )
)

dependencyCheckOutputDirectory := Some(baseDirectory.value / "dependency-check")
dependencyCheckSuppressionFile := Some(baseDirectory.value / "dependency-check" / "suppression.xml")

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
