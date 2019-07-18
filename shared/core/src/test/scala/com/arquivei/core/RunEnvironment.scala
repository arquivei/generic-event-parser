package com.arquivei.core

import com.arquivei.core.configs.Options
import org.json4s.JsonDSL._
import org.json4s._

trait RunEnvironment {
  val values: JValue = JObject() ~
    ("environment" -> "dev") ~
    ("runner" -> (JObject() ~
      ("project" -> "project") ~
      ("numWorkers" -> 1) ~
      ("workerMachineType" -> "n1-standard-2") ~
      ("labels" -> (JObject() ~
        ("costcenter" -> "bi-development") ~
        ("infratype" -> "pipeline")
        )
        )
      )) ~
    ("pipelines" ->
      ("TestPipe" ->
        ("enable" -> true)
        )
      ) ~
    ("options" -> (JObject() ~
      ("dataset" -> "test") ~
      ("strOption" -> "test") ~
      ("intOption" -> 2) ~
      ("floatOption" -> -4.5) ~
      ("boolOption" -> true)
      )
      ) ~
    ("connections" -> (JObject() ~
      ("replica" -> (JObject() ~
        ("host" -> "1.2.3.4") ~
        ("port" -> 1234) ~
        ("dbname" -> "db") ~
        ("username" -> "user") ~
        ("password" -> "pass")
        )
        ) ~
      ("kafka" -> (JObject() ~
        ("topic" -> List("mytopic", "mytopic2")) ~
        ("properties" -> (JObject() ~
          ("bootstrap.servers" -> "localhost:9092")
          )
          )
        )
        )
      )
      )
  implicit val options: Options = new Options(values)
}