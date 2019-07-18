package com.arquivei.core.io.db.bigquery

import com.google.cloud.bigquery._

object ToSchema {
  def apply(fields: Seq[(String, String)]): Schema = {

    val tableFields = fields.map({
      case (name, fieldtype) => Field.of(name, LegacySQLTypeName.valueOf(fieldtype))
    })
    Schema.of(tableFields: _*)
  }
}
