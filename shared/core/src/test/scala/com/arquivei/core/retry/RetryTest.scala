package com.arquivei.core.retry

import java.time.Instant

import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Failure, Success}

class RetryTest extends FlatSpec with Matchers {
  "Retry" should "return success when no error happens" in {
    Retry(1,0,verbose = false) {
      1 + 1
    } shouldBe Success(2)
  }

  "Retry" should "return failure when an error happens" in {
    Retry(1,0,verbose = false) {
      1 / 0
    } shouldBe a[Failure[_]]
  }

  "Retry" should "return right exception type when an error happens" in {
    an[ArithmeticException] should be thrownBy Retry(1,0,verbose = false) {
      1 / 0
    }.get
  }

  "Retry" should "retry the specified number of retries" in {
    var counter = 0
    Retry(3,0,verbose = false) {
      counter = counter + 1
      1 / 0
    }
    counter shouldBe 3
  }

  "Retry" should "wait at least the specified amount of time" in {
    val retryTime = 100
    val totalWaitTime = 100
    val startTime = Instant.now()
    Retry(2,retryTime,verbose = false) {
      1 / 0
    }
    (Instant.now().minusMillis(totalWaitTime).compareTo(startTime) >= 0) shouldBe true
  }

  "Retry" should "wait at least the specified amount of time for exponential retry" in {
    val retryTime = 100
    val totalWaitTime = 226
    val startTime = Instant.now()
    Retry(3,retryTime,exponential = true,verbose = false) {
      1 / 0
    }
    (Instant.now().minusMillis(totalWaitTime).compareTo(startTime) >= 0) shouldBe true
  }
}
