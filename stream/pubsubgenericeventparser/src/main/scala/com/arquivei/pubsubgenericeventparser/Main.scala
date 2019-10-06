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

    val pipeline = new PubsubGenericEventParser(sc)

    mode match {
      case "migrate" =>
        pipeline.migrate()
      case "run" | "update" =>
        pipeline.build(pubSubMessages)
        sc.run()
      case _ =>
        println(s"Unsuported mode $mode")
    }
  }
}
