package io.aiven.guardian.kafka

import org.apache.kafka.common.KafkaFuture

import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import java.util.concurrent.CompletableFuture

object Utils {

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

}
