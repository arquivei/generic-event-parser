package com.arquivei.core.workflow

trait Pipeline {
  def migrate(): Unit
  def build(): Unit
}

trait StreamPipeline[T] {
  def migrate(): Unit

  def build(start: T): Unit
}
