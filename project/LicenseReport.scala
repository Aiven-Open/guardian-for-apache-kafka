import sbt._
import sbtlicensereport.SbtLicenseReport
import sbtlicensereport.SbtLicenseReport.autoImportImpl._
import sbtlicensereport.license.{DepModuleInfo, MarkDown}

object LicenseReport extends AutoPlugin {

  override lazy val projectSettings = Seq(
    licenseReportTypes      := Seq(MarkDown),
    licenseReportMakeHeader := (language => language.header1("License Report")),
    licenseConfigurations   := Set("compile", "test", "provided"),
    licenseDepExclusions := {
      case dep: DepModuleInfo if dep.organization == "io.aiven" && dep.name.contains("guardian") =>
        true // Inter guardian project dependencies are pointless
      case DepModuleInfo(_, "scala-library", _) => true // Scala library is part of Scala language
      case DepModuleInfo(_, "scala-reflect", _) => true // Scala reflect is part of Scala language
    },
    licenseReportColumns := Seq(Column.Category, Column.License, Column.Dependency, Column.Configuration)
  )

  override def requires = plugins.JvmPlugin && SbtLicenseReport

  override def trigger = allRequirements

}
