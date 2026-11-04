package com.zilliz.spark.connector.backfill

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BackfillSegmentWriteTest extends AnyFunSuite with Matchers {
  test("segment rows strip tracking columns and preserve provenance counts") {
    val target = StructType(
      Seq(
        StructField("a", LongType, nullable = true),
        StructField("b", LongType, nullable = true)
      )
    )
    val rows = Iterator(
      InternalRow(10L, 0L, 1L, 2L, true, true, false, false, true),
      InternalRow(10L, 1L, null, 3L, null, false, false, true, false),
      InternalRow(10L, 2L, 4L, null, true, true, false, null, false)
    )
    val written = ArrayBuffer.empty[InternalRow]
    val counts =
      BackfillSegmentWrite.writeRows(rows, target, readsSnapshotFields = true) {
        row =>
          written += row.copy()
      }
    written.map(_.numFields).toSeq shouldBe Seq(2, 2, 2)
    written
      .map(row => (row.get(0, LongType), row.get(1, LongType)))
      .toSeq shouldBe
      Seq((1L, 2L), (null, 3L), (4L, null))
    counts shouldBe BackfillSegmentWrite.RowCounts(
      3L,
      2L,
      Map("a" -> 2L, "b" -> 1L),
      Map("a" -> 0L, "b" -> 1L)
    )
  }

  test("replace rows require no provenance columns and preserve null values") {
    val target =
      StructType(Seq(StructField("value", LongType, nullable = true)))
    val written = ArrayBuffer.empty[Any]
    val counts = BackfillSegmentWrite.writeRows(
      Iterator(
        InternalRow(10L, 0L, null, null),
        InternalRow(10L, 1L, 7L, true)
      ),
      target,
      readsSnapshotFields = false
    ) { row => written += row.get(0, LongType) }
    written.toSeq shouldBe Seq(null, 7L)
    counts shouldBe BackfillSegmentWrite.RowCounts(2L, 1L, Map.empty, Map.empty)
  }

  test("a row write failure propagates without consuming later rows") {
    val target =
      StructType(Seq(StructField("value", LongType, nullable = false)))
    var consumed = 0
    val rows = Iterator(1L, 2L, 3L).map { value =>
      consumed += 1
      InternalRow(10L, value, value, true)
    }
    val failure = new IllegalStateException("writer failed")
    val error = intercept[IllegalStateException] {
      BackfillSegmentWrite.writeRows(
        rows,
        target,
        readsSnapshotFields = false
      ) { row =>
        if (row.getLong(0) == 2L) throw failure
      }
    }
    error should be theSameInstanceAs failure
    consumed shouldBe 2
  }

}
