package io.aiven.guardian.kafka

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.KafkaFuture
import org.apache.pekko

import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.DurationConverters._
import scala.util.Failure
import scala.util.Success

import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.concurrent.CompletableFuture

import pekko.actor.ActorSystem

object TestUtils {

  // Taken from https://stackoverflow.com/a/56763206/1519631
  implicit final class KafkaFutureToCompletableFuture[T](kafkaFuture: KafkaFuture[T]) {
    @SuppressWarnings(Array("DisableSyntax.null"))
    def toCompletableFuture: CompletableFuture[T] = {
      val wrappingFuture = new CompletableFuture[T]
      kafkaFuture.whenComplete { (value, throwable) =>
        if (throwable != null)
          wrappingFuture.completeExceptionally(throwable)
        else
          wrappingFuture.complete(value)
      }
      wrappingFuture
    }
  }

  implicit final class ScalaFutureExtensionMethods[T](future: Future[T]) extends LazyLogging {
    def onCompleteLogError(f: () => Unit)(implicit executor: ExecutionContext): Unit =
      future.onComplete { result =>
        result match {
          case Failure(exception) => logger.error("Future resulted in error", exception)
          case Success(_)         => ()
        }
        f()
      }
  }

  /** The standard Scala groupBy returns an `immutable.Map` which is unordered, this version returns an ordered
    * `ListMap` for when preserving insertion order is important
    */
  implicit class GroupBy[A](val t: IterableOnce[A]) {
    def orderedGroupBy[K](f: A => K): immutable.ListMap[K, List[A]] = {
      var m = immutable.ListMap.empty[K, ListBuffer[A]]
      for (elem <- t.iterator) {
        val key = f(elem)
        m = m.updatedWith(key) {
          case Some(value) => Some(value.addOne(elem))
          case None        => Some(mutable.ListBuffer[A](elem))
        }
      }
      m.map { case (k, v) => (k, v.toList) }
    }
  }

  final case class UnsupportedTimeUnit(chronoUnit: ChronoUnit) extends Exception(s"$chronoUnit not supported")

  private def recurseUntilHitTimeUnit(previousChronoUnit: ChronoUnit, buffer: BigDecimal)(implicit
      system: ActorSystem
  ): Future[Unit] = {
    val now = OffsetDateTime.now()
    val (current, max) = previousChronoUnit match {
      case ChronoUnit.SECONDS =>
        (now.getSecond, 59)
      case ChronoUnit.MINUTES =>
        (now.getMinute, 59)
      case ChronoUnit.HOURS =>
        (now.getHour, 23)
      case ChronoUnit.DAYS =>
        (now.getDayOfWeek.getValue - 1, 6)
      case ChronoUnit.MONTHS =>
        (now.getMonth.getValue - 1, 11)
      case _ => throw UnsupportedTimeUnit(previousChronoUnit)
    }

    if (BigDecimal(current) / BigDecimal(max) * BigDecimal(100) <= buffer)
      Future.successful(())
    else
      pekko.pattern.after(previousChronoUnit.getDuration.toScala)(recurseUntilHitTimeUnit(previousChronoUnit, buffer))
  }

  def waitForStartOfTimeUnit(chronoUnit: ChronoUnit, buffer: BigDecimal = BigDecimal(5))(implicit
      system: ActorSystem
  ): Future[Unit] = {
    val allEnums     = ChronoUnit.values()
    val previousEnum = allEnums(chronoUnit.ordinal - 1)
    recurseUntilHitTimeUnit(previousEnum, buffer)
  }

  def checkEnvVarAvailable(env: String): Boolean =
    try
      sys.env.get(env) match {
        case Some(value) if value.isBlank => false
        case Some(_)                      => true
        case None                         => false
      }
    catch {
      case _: NoSuchElementException => false
    }

}
