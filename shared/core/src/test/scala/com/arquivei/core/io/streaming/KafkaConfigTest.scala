package com.arquivei.core.io.streaming

import com.arquivei.core.RunEnvironment
import com.arquivei.core.configs.Options
import com.arquivei.core.exceptions.ArquiveiException
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.scalatest.FlatSpec

import scala.collection.JavaConverters._

class KafkaConfigTest extends FlatSpec {
  "KafkaConfig" should "get the right configs" in new RunEnvironment {
    val kafkaConfig = KafkaConfig("kafka")
    assertResult(List("mytopic", "mytopic2"))(kafkaConfig.topics.asScala)
    //TODO
  }
  it should "handle one topic" in new RunEnvironment {
    val change: JObject = "connections" -> ("kafka" -> ("topic" -> "mystringtopic"))
    implicit val newOptions: Options = new Options(values merge change)
    val kafkaConfig = KafkaConfig("kafka")(newOptions)
    assertResult(List("mystringtopic"))(kafkaConfig.topics.asScala)
  }

  it should "handle invalid topic" in new RunEnvironment {
    val change: JObject = "connections" -> ("kafka" -> ("topic" -> ("invalid" -> "object")))
    implicit val newOptions: Options = new Options(values merge change)
    assertThrows[ArquiveiException](KafkaConfig("kafka")(newOptions))
  }
}
