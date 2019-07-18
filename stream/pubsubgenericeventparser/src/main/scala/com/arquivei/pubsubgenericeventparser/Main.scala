package com.arquivei.pubsubgenericeventparser

import com.arquivei.core.configs.Options
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

    val fullSubscription = Options().getOptionString("fullSubscription")
    val pubSubMessages = sc
      .withName("Read From Pubsub").pubsubSubscription[String](fullSubscription)

    val pipelines = List(
      new PubsubGenericEventParser(sc)
    ).filterPipelines

    mode match {
      case "migrate" =>
        for (pipeline <- pipelines) pipeline.migrate()
      case "run" | "update" =>
        for (pipeline <- pipelines) pipeline.build(pubSubMessages)
        sc.close()
      case _ =>
        println(s"Unsuported mode $mode")
    }
  }
}
