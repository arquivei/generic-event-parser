package com.arquivei.core.workflow

import com.arquivei.core.RunEnvironment
import com.arquivei.core.pipelines._
import com.arquivei.core.workflow.Workflow._
import org.scalatest.FlatSpec

class WorkflowTest extends FlatSpec {
  "Workflow" should "filter enabled pipes" in new RunEnvironment {
    val pipelines = List(
      new TestPipe(),
      new TestPipe2()
    ).filterPipelines
    assertResult(1)(pipelines.size)
  }
}
