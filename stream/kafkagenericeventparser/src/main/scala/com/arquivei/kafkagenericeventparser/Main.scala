package com.arquivei.kafkagenericeventparser

import com.arquivei.core.io.streaming.KafkaConfig
import com.arquivei.core.workflow.ConfigParser
import com.arquivei.core.workflow.Workflow._
import com.spotify.scio.ContextAndArgs

object Main {
  def main(args: Array[String]): Unit = {
    val mode = args(0)
    val yaml = args(1)
    val extraArgs = args.drop(2)

    val scioArgs = ConfigParser(mode, yaml, extraArgs)
    val (sc, _) = ContextAndArgs(scioArgs)

    val kafkaConfig = KafkaConfig("kafka")

    val pipelines = List(
      new GenericEventParser(sc)
    ).filterPipelines

    val kafkaRead = new KafkaRead(sc, kafkaConfig)

    mode match {
      case "migrate" =>
        for (pipeline <- pipelines) pipeline.migrate()
      case "run" | "update" =>
        val read = kafkaRead.read()
        for (pipeline <- pipelines) pipeline.build(read)
        sc.close()
      case _ =>
        println(s"Unsuported mode $mode")
    }
  }
}
