package com.arquivei.core.dateparser

import java.time._
import java.time.format.DateTimeFormatter

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object DateParser {

  val spZoneId: ZoneId = ZoneId.of("America/Sao_Paulo")
  val arquiveiDateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(spZoneId)

  val dateFormats: List[DateTimeFormatter] = List(
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.nXXX"),
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX"),
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(spZoneId),
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSS").withZone(spZoneId),
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS").withZone(spZoneId),
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(spZoneId),
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(spZoneId)
  )

  def isEventDatetime(inputString: String): Boolean = dateFormats.map(
    { format => Try(LocalDateTime.parse(inputString, format)) }
  ).exists(_.isSuccess)

  def isEventDate(inputString: String): Boolean = {
    Try(LocalDate.parse(inputString, DateTimeFormatter.ofPattern("yyyy-MM-dd"))).isSuccess
  }

  @tailrec
  private def tryDateFormats(inputString: String, formats: List[DateTimeFormatter]): Option[String] = {
    formats match {
      case Nil => None
      case format :: tail =>
        Try({
          arquiveiDateFormat.format(Instant.from(ZonedDateTime.parse(inputString, format)))
        }) match {
          case Success(str) => Some(str)
          case Failure(_) => tryDateFormats(inputString, tail)
        }
    }
  }

  def convertToSaoPauloOrString(inputString: String): String = {
      tryDateFormats(inputString, dateFormats).getOrElse(inputString)
  }

  def timestampToArquiveiDatetime(timestamp: Long): String = {
    val instantTimestamp = Instant.ofEpochSecond(timestamp)
    arquiveiDateFormat.format(instantTimestamp)
  }
}
