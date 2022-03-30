package io.aiven.guardian.kafka

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Try

import java.util.concurrent.FutureTask

class Cancellable[T](executionContext: ExecutionContext, block: => T) {
  private val promise = Promise[T]()

  def future: Future[T] = promise.future

  private val jf: FutureTask[T] = new FutureTask[T](() => block) {
    override def done(): Unit = promise.complete(Try(get()))
  }

  def cancel(): Unit = jf.cancel(true)

  executionContext.execute(jf)
}

object Cancellable {

  /** Allows you to put a computation inside of a [[scala.concurrent.Future]] which can later be cancelled
    * @param block
    *   The computation to run inside of the [[scala.concurrent.Future]]
    * @param executionContext
    *   The [[scala.concurrent.ExecutionContext]] to run the [[scala.concurrent.Future]] on
    * @return
    *   A [[io.aiven.guardian.kafka.Cancellable]] providing both the [[scala.concurrent.Future]] and a `cancel` method
    *   allowing you to terminate the [[scala.concurrent.Future]] at any time
    * @see
    *   Adapted from https://stackoverflow.com/a/39986418/1519631
    */
  def apply[T](block: => T)(implicit executionContext: ExecutionContext): Cancellable[T] =
    new Cancellable[T](executionContext, block)
}
