package com.arquivei.core.io.streaming

import com.arquivei.core.configs.Options
import com.arquivei.core.exceptions.ArquiveiException
import com.arquivei.core.formats.Json
import org.json4s._

import scala.collection.JavaConverters._

case class KafkaConfig
(
  topics: java.util.List[String],
  properties: java.util.Map[String, AnyRef]
)

object KafkaConfig {
  def apply(connection: String)(implicit options: Options = Options.getDefaultOptions): KafkaConfig = {
    val kafka = options.get \ "connections" \ connection
    val jsonTopics = kafka \ "topic"
    val jsonProperties = kafka \ "properties"

    implicit val defaultFormats = org.json4s.DefaultFormats

    val topics = jsonTopics match {
      case JString(s) => List(s)
      case JArray(arr) => arr.map(_.extract[String])
      case _ => throw new ArquiveiException("Unsuported type " + jsonTopics.getClass.toString)
    }

    val properties = {
      val fields: List[(String, AnyRef)] = for {
        JObject(obj) <- jsonProperties
        (k, v) <- obj
      } yield (k, new java.lang.String(Json.jValueToString(v)))

      Map(fields: _*)

    }
    KafkaConfig(topics.asJava, properties.asJava)
  }
}
