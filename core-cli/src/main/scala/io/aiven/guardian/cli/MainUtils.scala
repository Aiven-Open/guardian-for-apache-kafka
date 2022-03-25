package io.aiven.guardian.cli

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.blocking
import scala.io.StdIn

object MainUtils {

  /** Hook that lets the user specify the future that will signal the shutdown of the server whenever completed. Adapted
    * from
    * https://github.com/akka/akka-http/blob/main/akka-http/src/main/scala/akka/http/scaladsl/server/HttpApp.scala#L151-L163
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

}
