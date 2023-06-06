package io.aiven.guardian.pekko

import org.apache.pekko
import org.scalatest.Suite

import pekko.actor.ActorSystem
import pekko.http.scaladsl.Http

trait PekkoHttpTestKit extends PekkoStreamTestKit { this: Suite =>
  implicit val system: ActorSystem

  override protected def afterAll(): Unit =
    Http(system)
      .shutdownAllConnectionPools()
      .foreach { _ =>
        super.afterAll()
      }(system.dispatcher)
}
