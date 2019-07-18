package com.arquivei.core.dateparser

import org.scalatest.FlatSpec

class DateParserTest extends FlatSpec {

  "isEventDatetime" should "check if string is a valid datetime of the defined patterns" in {
    assert(DateParser.isEventDatetime("2018-05-03 10:00:00"))
    assert(DateParser.isEventDatetime("2018-12-03 10:00:00.333"))
    assertResult(false)(DateParser.isEventDatetime("batata"))
    assertResult(false)(DateParser.isEventDatetime("2000-1-1 11:11"))
    assertResult(false)(DateParser.isEventDatetime("2000-01-01"))
  }
  "isEventDate" should "check if a string is a valid date in the pattern yyyy-MM-DD" in {
    assert(DateParser.isEventDate("2019-01-01"))
    assert(DateParser.isEventDate("2005-01-01"))
    assertResult(false)(DateParser.isEventDate("2019-13-01"))
    assertResult(false)(DateParser.isEventDate("joao e o pe de feijao"))
  }
  "convertToSaoPauloOrString" should "convert a datetime string to America/Sao_Paulo timezone and arquivei format or return the input string" in {
    assertResult("2017-01-18 00:00:00")(DateParser.convertToSaoPauloOrString("2017-01-18T00:00:00-02:00"))
    assertResult("formigas assassinas")(DateParser.convertToSaoPauloOrString("formigas assassinas"))
    assertResult("2017-05-07 21:00:00")(DateParser.convertToSaoPauloOrString("2017-05-08T03:00:00+03:00"))
  }

  "timestampToArquiveiDatetime" should "convert a Long to the Arquivei Datetime format in America/Sao_Paulo timezone" in {
    val timestamp: Long = 1538661878
    assertResult("2018-10-04 11:04:38")(DateParser.timestampToArquiveiDatetime(timestamp))
  }

}
