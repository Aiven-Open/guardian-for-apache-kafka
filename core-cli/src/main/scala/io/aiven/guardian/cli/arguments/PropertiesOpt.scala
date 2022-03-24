package io.aiven.guardian.cli.arguments

import cats.data.ValidatedNel
import cats.implicits._
import com.monovore.decline.Argument

import scala.util.Failure
import scala.util.Success
import scala.util.Using

import java.io.BufferedReader
import java.io.FileNotFoundException
import java.io.FileReader
import java.util.Properties

object PropertiesOpt {
  implicit val propertiesArgument: Argument[Properties] = new Argument[Properties] {
    override def read(string: String): ValidatedNel[String, Properties] = {
      val prop = new Properties()
      Using(new BufferedReader(new FileReader(string))) { reader =>
        prop.load(reader)
      } match {
        case Failure(_: FileNotFoundException) =>
          s"Properties file at path $string does not exist".invalidNel
        case Failure(_) =>
          s"Unable to read file at path $string".invalidNel
        case Success(_) => prop.validNel
      }
    }

    override def defaultMetavar: String = "path"
  }

}
