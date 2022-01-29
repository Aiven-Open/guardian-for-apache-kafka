package io.aiven.guardian.kafka

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import java.time.OffsetDateTime

object Utils {
  def keyToOffsetDateTime(key: String): OffsetDateTime = {
    val withoutExtension = key.substring(0, key.lastIndexOf('.'))
    OffsetDateTime.parse(withoutExtension)
  }

  def runSequentially[A](
      lazyFutures: List[() => Future[A]]
  )(implicit ec: ExecutionContext): Future[List[A]] =
    lazyFutures.foldLeft(Future.successful(List.empty[A])) { (acc, curr) =>
      for {
        a <- acc
        c <- curr()
      } yield c :: a
    }

}
