package org.scalatest

import org.scalatest.tools.ConcurrentDistributor

trait WorkStealingParallelExecution extends SuiteMixin with ParallelTestExecution { this: Suite =>

  lazy val parallelism: Int = 1 max testNames.size

  abstract override def run(testName: Option[String], args: Args): Status =
    super.run(testName,
              args.copy(
                distributor = Some(
                  new ConcurrentDistributor(
                    args,
                    java.util.concurrent.Executors.newWorkStealingPool(parallelism)
                  )
                )
              )
    )
}
