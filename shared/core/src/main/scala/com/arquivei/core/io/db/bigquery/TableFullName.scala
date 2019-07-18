package com.arquivei.core.io.db.bigquery

object TableFullName {
  def apply(project: String, dataset: String, table:String): String = s"$project:$dataset.$table"
  def apply(dataset: String, table: String): String = s"$dataset.$table"
}
