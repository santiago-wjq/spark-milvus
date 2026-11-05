package com.zilliz.spark.connector.backfill

import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.manifest.SnapshotSegmentEntry

class BackfillSnapshotAdapterTest extends AnyFunSuite {
  private def entry(version: Long, level: Long = 2L): SnapshotSegmentEntry =
    SnapshotSegmentEntry(100L, 200L, level, 3L, version, Seq.empty, Seq.empty)

  test("public V3 adapter rejects every non-V3 data segment before reading") {
    Seq(0L, 2L, 4L).foreach { version =>
      val error = intercept[IllegalArgumentException] {
        BackfillSnapshotAdapter.V3Only.resolve(
          Seq(entry(3L), entry(version)),
          null,
          BackfillConfig()
        )
      }
      assert(error.getMessage.contains("Storage V3"))
    }
  }

  test("public V3 adapter permits V3 data and level-zero deletion entries") {
    assert(
      BackfillSnapshotAdapter.V3Only.resolve(
        Seq(entry(3L), entry(0L, level = 1L)),
        null,
        BackfillConfig()
      ) ==
        BackfillAdditionalSegments.Empty
    )
  }

  test(
    "adapter coverage includes empty data segments and rejects omissions, extras and wrong partitions"
  ) {
    val entries = Seq(
      entry(3L),
      entry(2L).copy(segmentId = 101L),
      entry(2L).copy(segmentId = 102L, numOfRows = 0L),
      entry(0L, level = 1L).copy(segmentId = 103L)
    )
    MilvusBackfill.validateAdapterCoverage(
      entries,
      Map(101L -> 200L, 102L -> 200L)
    )
    Seq(
      Map.empty[Long, Long],
      Map(101L -> 200L),
      Map(101L -> 200L, 102L -> 999L),
      Map(101L -> 200L, 102L -> 200L, 103L -> 200L),
      Map(101L -> 200L, 102L -> 200L, 100L -> 200L)
    ).foreach { partitions =>
      intercept[IllegalArgumentException](
        MilvusBackfill.validateAdapterCoverage(entries, partitions)
      )
    }
    intercept[IllegalArgumentException](
      MilvusBackfill.validateAdapterCoverage(
        Seq(entry(3L).copy(numOfRows = -1L)),
        Map.empty
      )
    )
  }
}
