package com.arquivei.core.configs

import com.arquivei.core.exceptions.ArquiveiException
import org.json4s.JsonAST.JValue
import org.json4s._
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

class Options(value: JValue) {

  def get: JValue = value

  private implicit val defaultFormats = DefaultFormats

  def getOption[T](key: String)(implicit mf: scala.reflect.Manifest[T]): T = {
    val extractedOption = value \ "options" \ key

    Try(extractedOption.extract[T]) match {
      case Success(value) =>
        value
      case Failure(err) =>
        throw new ArquiveiException(s"Could not get Option key $key. Value = $extractedOption. Error = ${err.toString}")
    }
  }

  def getOptionString(key: String): String = getOption[String](key)
  def getOptionInt(key: String): Int = getOption[Int](key)
  def getOptionFloat(key: String): Float = getOption[Float](key)
  def getOptionBoolean(key: String): Boolean = getOption[Boolean](key)

  def gcpProject: String = (value \ "runner" \ "project").extract[String]
}

object Options {
  val logger = LoggerFactory.getLogger(this.getClass)

  private var defaultOptions: Options = _

  def setValue(value: JValue): Options = {
    defaultOptions = new Options(value)
    defaultOptions
  }

  def apply()(implicit options: Options = defaultOptions): Options = options

  def getDefaultOptions: Options = defaultOptions
}
