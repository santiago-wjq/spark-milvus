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
}
