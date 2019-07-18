package com.arquivei.pubsubgenericeventparser


import com.arquivei.core.exceptions.ArquiveiException
import com.arquivei.core.io.db.bigquery.JValueToTableSchema
import com.google.api.services.bigquery.model.{TableRow, TableSchema, TimePartitioning}
import com.google.gson.Gson
import com.spotify.scio.bigquery.BigQuerySysProps
import com.spotify.scio.bigquery.client._
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.bigquery._
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, PTransform, ParDo}
import org.apache.beam.sdk.values.{PCollection, ValueInSingleWindow}

import scala.util.{Failure, Success, Try}

object StreamingInsert {

  class WriteToBigQuery(getTableSpec: TableRow => String) extends PTransform[PCollection[TableRow], PCollection[TableRow]] {

    override def expand(input: PCollection[TableRow]): PCollection[TableRow] = {
      input.apply(s"Write To BigQuery",
        BigQueryIO.writeTableRows().to(new DynamicDestinations[TableRow, TableRow] {

          override def getDestination(inputElement: ValueInSingleWindow[TableRow]): TableRow = {
            inputElement.getValue
          }

          override def getTable(inputElement: TableRow): TableDestination = {
            val tableReference = getTableSpec(inputElement)
            new TableDestination(tableReference, null, new TimePartitioning().setType("DAY"))
          }

          override def getSchema(inputElement: TableRow): TableSchema = {
            val inputJson = new Gson().toJson(inputElement)
            JValueToTableSchema(inputJson)
          }

        })
          .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
          .withWriteDisposition(WriteDisposition.WRITE_APPEND)
      ).getFailedInserts
    }
  }

  class WriteToBigQueryAfterSchemaMutation(getTableSpec: TableRow => String, isTest: Boolean)
    extends PTransform[PCollection[TableRow], PCollection[Option[TableRow]]] {

    override def expand(input: PCollection[TableRow]): PCollection[Option[TableRow]] = {
      input.apply("Try inserting to BigQuery", ParDo.of(new InsertRowsToBigQuery))
    }

    class InsertRowsToBigQuery extends DoFn[TableRow, Option[TableRow]] {

      @ProcessElement
      def processElement(c: DoFn[TableRow, Option[TableRow]]#ProcessContext): Unit = {

        val inputRow = c.element()
        val tableSpec = getTableSpec(inputRow)
        val projectId = BigQueryHelpers.parseTableSpec(tableSpec).getProjectId

        if(!isTest) {
          val bq = BigQuery(projectId)
          val sleep = 10000 // 10s
          val tries = 12
          System.setProperty(BigQuerySysProps.CacheEnabled.flag, "false")

          c.output(tryInsertRows(bq, tableSpec, inputRow, sleep, tries))

        } else {
          val tries = 2
          val sleep = 1
          retry (tries, sleep) {
            throw new ArquiveiException("testando")
          }
          c.output(Some(inputRow))
        }
      }

      def tryInsertRows(bq: BigQuery, tableSpec: String, inputRow: TableRow, sleep: Int, tries: Int): Option[TableRow] = {
        retry(tries, sleep) {
          bq.tables.writeRows(tableSpec, List(inputRow), null, WriteDisposition.WRITE_APPEND, CreateDisposition.CREATE_NEVER)
        } match {
          case Success(_) => None
          case Failure(_) => Some(inputRow)
        }
      }

      @annotation.tailrec
      private def retry[T](n: Int, sleep: Int)(fn: => T): util.Try[T] = {
        Try {
          fn
        } match {
          case x: Success[T] => x
          case _ if n > 1 =>
            Thread.sleep(sleep)
            retry(n - 1, sleep)(fn)
          case f => f
        }
      }
    }

  }
}
