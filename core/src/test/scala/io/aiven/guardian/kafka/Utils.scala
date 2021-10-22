package io.aiven.guardian.kafka

import org.apache.kafka.common.KafkaFuture

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

}
