package com.arquivei.core.workflow

import com.arquivei.core.configs.Options
import org.json4s._

import scala.language.implicitConversions

class Workflow[T](pipelines: Iterable[T])(implicit options: Options) {
  def filterPipelines: Iterable[T] = {
    val pipelineOptions = options.get \ "pipelines"
    pipelines.filter(pipeline => {
      val className = pipeline.getClass.getSimpleName
      pipelineOptions \ className \ "enable" match {
        case JBool(true) => true
        case _ => false
      }
    })
  }
}

object Workflow {
  implicit def toWorkflow[T](pipelines: Iterable[T])
                            (implicit options: Options = Options.getDefaultOptions): Workflow[T] =
    new Workflow(pipelines)
}
