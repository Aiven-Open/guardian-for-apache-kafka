package io.aiven.guardian.cli

import ch.qos.logback.classic.joran.JoranConfigurator
import ch.qos.logback.core.Context
import org.slf4j.ILoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.blocking
import scala.io.StdIn
import scala.util.Failure
import scala.util.Success
import scala.util.Using

import java.nio.file.Files
import java.nio.file.Path

object MainUtils {

  /** Hook that lets the user specify the future that will signal the shutdown of the server whenever completed. Adapted
    * from
    * https://github.com/apache/incubator-pekko-http/blob/94d1b1c153cc39216dae4217fd0e927f04d53cd2/http/src/main/scala/org/apache/pekko/http/scaladsl/server/HttpApp.scala#L164-L176
    */
  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.null"
    )
  )
  def waitForShutdownSignal(promise: Promise[Unit] = Promise[Unit]())(implicit ec: ExecutionContext): Future[Unit] = {
    sys.addShutdownHook {
      promise.trySuccess(())
    }
    Future {
      blocking {
        if (StdIn.readLine("Press RETURN to stop...\n") != null)
          promise.trySuccess(())
      }
    }
    promise.future
  }

  /** Allows you to override the default logback.xml file with a custom one
    * @see
    *   https://stackoverflow.com/a/21886322/1519631
    */
  def setLogbackFile(path: Path, loggerContext: ILoggerFactory): Unit =
    Using(Files.newInputStream(path)) { inputStream =>
      val configurator = new JoranConfigurator
      configurator.setContext(loggerContext.asInstanceOf[Context])
      configurator.doConfigure(inputStream)
    } match {
      case Failure(exception) => throw exception
      case Success(value)     => value
    }

}
