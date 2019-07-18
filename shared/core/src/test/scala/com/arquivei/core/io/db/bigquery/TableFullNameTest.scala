package com.arquivei.core.io.db.bigquery

import org.scalatest.FlatSpec

class TableFullNameTest extends FlatSpec {

  "TableFullName" should "apply" in {
    assertResult("project:dataset.table")(TableFullName("project","dataset","table"))
    assertResult("dataset.table")(TableFullName("dataset","table"))
  }

}
