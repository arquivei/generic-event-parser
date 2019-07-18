package com.arquivei.core.io.db.bigquery

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.cloud.bigquery._

import collection.JavaConverters._

object SchemaConverter {
  def apply(tableSchema: TableSchema): Schema = {
    def convertField(oldField: TableFieldSchema): Field = {
      val subFields = scala.Option(oldField.getFields)
        .fold(Seq.empty[Field])(fields => {
          fields.asScala.toList.map(convertField)
        })


      Field
        .newBuilder(oldField.getName, LegacySQLTypeName.valueOf(oldField.getType), subFields: _*)
        .setDescription(oldField.getDescription)
        .setMode(Field.Mode.valueOf(oldField.getMode))
        .build()
    }

    val fields = tableSchema.getFields.asScala.map(convertField)
    Schema.of(fields: _*)
  }
}
