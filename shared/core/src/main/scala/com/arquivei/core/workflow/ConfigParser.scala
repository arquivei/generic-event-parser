package com.arquivei.core.workflow

import org.json4s.jackson.JsonMethods._
import org.yaml.snakeyaml.Yaml
import com.arquivei.core.formats.Json._
import com.arquivei.core.configs.Options
import org.json4s.JsonAST.JValue
import org.json4s._
import org.slf4j.LoggerFactory

import scala.util.Try

object ConfigParser {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Sets internal options based on the .yaml file
    *
    * @param mode            Execution mode. If "update", also returns the --transformNameMapping flag.
    * @param file            Yaml config file.
    * @param extraArgs       Extra args for Scio / Beam. Mainly used for Scio Test flags.
    * @param optionOverrides Which of the extraArgs should override Options
    * @return List of arguments for Spotify Scio
    */
  def apply(
             mode: String,
             file: String,
             extraArgs: Array[String] = Array(),
             optionOverrides: Seq[String] = Nil
           ): Array[String] = {
    val config = scala.io.Source.fromFile(file).mkString
    val hashmap = new Yaml().load[java.util.Map[String, AnyRef]](config)
    val json = render(hashmap)
    val valuesToOverride = configOverrides(extraArgs, optionOverrides)

    implicit val options: Options = Options.setValue(json merge valuesToOverride)

    // UpdateArgs sets transformNameMapping when in `update` mode
    val updateArgs = updateFlags(mode)
    (getArgs(json) ++ updateArgs ++ extraArgs).toArray
  }

  def getArgs(configs: JValue): Seq[String] = {
    implicit val defaultFormats = DefaultFormats
    val fields = (configs \ "runner").asInstanceOf[JObject].obj
    fields map { case (key, value) =>
      val strValue = value match {
        case JObject(_) => compact(value)
        case _ => value.extract[String]
      }
      s"--$key=$strValue"
    }
  }

  def updateFlags(mode: String)(implicit options: Options): Seq[String] = {
    if (mode == "update") {
      val mapping = options.get \ "transformNameMapping" match {
        case value: JObject => compact(value)
        case _ => "{}"
      }
      List("--update", s"--transformNameMapping=$mapping")
    } else {
      Nil
    }
  }

  def configOverrides(extraArgs: Array[String], optionOverrides: Seq[String] = Nil): JValue = {
    val regex = "(?s)--([^=]+)=(.+)".r
    val fields = extraArgs
      .flatMap(arg => regex.findFirstMatchIn(arg))
      .map(m => {
        val key = m.group(1)
        val value = m.group(2)
        key -> value
      })
      .filter(config => optionOverrides.contains(config._1))
      .map({
        case (key, value) =>
          val parsedValue = Try(parse(value)).getOrElse(JString(value))
          key -> parsedValue
      })
      .toList

    JObject(
      "options" -> JObject(fields)
    )
  }
}
