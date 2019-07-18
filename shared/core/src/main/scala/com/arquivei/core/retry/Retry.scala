package com.arquivei.core.retry

import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object Retry {
  val logger = LoggerFactory.getLogger(this.getClass)

  @annotation.tailrec
  def apply[T](n: Int, sleepTimeMilliseconds: Long,
               exponential: Boolean = false, exponentialFactor: Double = 1.05, verbose: Boolean = true)(fn: => T): Try[T] = {
    Try {
      fn
    } match {
      case x: Success[T] => x
      case other if n > 1 =>

        if (verbose) {
          other match {
            case Success(value) =>
              logger.warn(s"Unexpected success type. Value returned: ${value.toString}")
            case Failure(err) =>
              logger.warn(s"Failured returned: ${err.toString}")
          }

          logger.info(s"Retrying: $n retries to go. Sleeping $sleepTimeMilliseconds milliseconds before next retry.")
        }

        Thread.sleep(sleepTimeMilliseconds)

        if (exponential) {
          Retry(n - 1, Math.pow(sleepTimeMilliseconds, exponentialFactor).ceil.toInt)(fn)
        } else {
          Retry(n - 1, sleepTimeMilliseconds)(fn)
        }
      case f => f
    }
  }
}
