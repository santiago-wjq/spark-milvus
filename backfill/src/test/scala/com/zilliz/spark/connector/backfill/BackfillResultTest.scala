package com.zilliz.spark.connector.backfill

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BackfillResultTest extends AnyFunSuite with Matchers {

  private val delta = SegmentBackfillResult(
    segmentId = 10L,
    rowCount = 5L,
    manifestPaths = Seq.empty,
    outputPath = "files/segment-10",
    executionTimeMs = 1L,
    manifestOps = Seq(
      BackfillManifestOp(
        "replace",
        Seq("101"),
        "parquet",
        Seq(
          BackfillManifestFile("files/segment-10/_data/first.parquet", 0L, 3L),
          BackfillManifestFile("files/segment-10/_data/second.parquet", 0L, 2L)
        ),
        5L
      )
    )
  )

  private def result(segments: SegmentBackfillResult*): BackfillResult =
    BackfillResult.success(
      segments.map(segment => segment.segmentId -> segment).toMap,
      1L,
      1L,
      2L,
      7,
      Seq("embedding")
    )

  test(
    "delta JSON carries exact file-local descriptors without a committed manifest"
  ) {
    val json = new ObjectMapper().readTree(result(delta).toJson)
    val segment = json.path("segments").path("10")
    Seq(
      "version",
      "manifestPaths",
      "sourceVersion",
      "storage_version",
      "column_groups"
    )
      .foreach(name => segment.has(name) shouldBe false)
    val op = segment.path("ops").get(0)
    op.path("type").asText() shouldBe "replace"
    op.path("columns").get(0).asText() shouldBe "101"
    op.path("format").asText() shouldBe "parquet"
    op.path("rowCount").asLong() shouldBe 5L
    val first = op.path("files").get(0)
    first.path("path").asText() shouldBe "files/segment-10/_data/first.parquet"
    first.path("startIndex").asLong() shouldBe 0L
    first.path("endIndex").asLong() shouldBe 3L
    first.has("properties") shouldBe false
    op.path("files").get(1).path("startIndex").asLong() shouldBe 0L
    op.path("files").get(1).path("endIndex").asLong() shouldBe 2L
    result(delta).segmentSummary should include("ops=1")
    result(delta).segmentSummary should not include "version=-1"
  }

  test("legacy, delta and V2 entries retain distinct forms in one result") {
    val legacy = delta.copy(
      segmentId = 11L,
      committedVersion = 8L,
      sourceVersion = Some(7L),
      manifestPaths = Seq("files/segment-11/_metadata/manifest-8.avro"),
      manifestOps = Seq.empty
    )
    val v2 = delta.copy(
      segmentId = 12L,
      manifestOps = Seq.empty,
      columnGroupArtifact = Some(
        BackfillSegmentArtifact(
          12L,
          2L,
          Seq(
            BackfillColumnGroupArtifact(
              Seq(101L),
              Seq("files/insert_log/1/2/12/101/42"),
              5L
            )
          )
        )
      )
    )
    val segments = new ObjectMapper()
      .readTree(result(delta, legacy, v2).toJson)
      .path("segments")
    segments.path("11").path("version").asLong() shouldBe 8L
    segments.path("11").path("sourceVersion").asLong() shouldBe 7L
    segments.path("11").path("manifestPaths").get(0).asText() should endWith(
      "manifest-8.avro"
    )
    segments.path("11").has("ops") shouldBe false
    segments.path("12").path("storage_version").asLong() shouldBe 2L
    segments.path("12").path("version").asLong() shouldBe -1L
    segments
      .path("12")
      .path("column_groups")
      .get(0)
      .path("field_ids")
      .get(0)
      .asLong() shouldBe 101L
    segments.path("12").has("ops") shouldBe false
    segments.path("12").has("sourceVersion") shouldBe false
  }

  test(
    "the result model supports add operations and optional file properties"
  ) {
    val op = delta.manifestOps.head.copy(
      opType = "add",
      files = Seq(
        BackfillManifestFile(
          "files/segment-10/_data/one.parquet",
          0L,
          5L,
          Map("file_size" -> "4096")
        )
      )
    )
    val json = new ObjectMapper().readTree(
      result(delta.copy(manifestOps = Seq(op))).toJson
    )
    val serialized = json.path("segments").path("10").path("ops").get(0)
    serialized.path("type").asText() shouldBe "add"
    serialized
      .path("files")
      .get(0)
      .path("properties")
      .path("file_size")
      .asText() shouldBe "4096"
  }

  test(
    "delta serialization rejects legacy and V2 fields rather than publishing an ambiguous result"
  ) {
    Seq(
      delta.copy(committedVersion = 7L),
      delta.copy(sourceVersion = Some(6L)),
      delta.copy(manifestPaths = Seq("manifest-7.avro")),
      delta.copy(columnGroupArtifact =
        Some(BackfillSegmentArtifact(10L, 2L, Seq.empty))
      )
    ).foreach { invalid =>
      intercept[IllegalArgumentException](result(invalid).toJson)
    }
  }

  test("a legacy source version requires a newer committed manifest") {
    val legacy = delta.copy(
      manifestOps = Seq.empty,
      sourceVersion = Some(7L),
      manifestPaths = Seq("files/segment-10/_metadata/manifest-8.avro")
    )
    Seq(-1L, 0L, 6L, 7L).foreach { version =>
      intercept[IllegalArgumentException](
        result(legacy.copy(committedVersion = version)).toJson
      ).getMessage should include("newer than sourceVersion")
    }
    intercept[IllegalArgumentException](
      result(
        legacy.copy(
          committedVersion = 8L,
          sourceVersion = Some(0L)
        )
      ).toJson
    ).getMessage should include("must be positive")
    val json = new ObjectMapper().readTree(
      result(legacy.copy(committedVersion = 8L)).toJson
    )
    json.path("segments").path("10").path("version").asLong() shouldBe 8L
  }

  test("delta serialization rejects unsafe paths and invalid descriptors") {
    val original = delta.manifestOps.head
    val invalidOps = Seq(
      original.copy(opType = "append"),
      original.copy(columns = Seq.empty),
      original.copy(columns = Seq("embedding")),
      original.copy(columns = Seq("1")),
      original.copy(columns = Seq("101", "101")),
      original.copy(format = "vortex"),
      original.copy(rowCount = 6L),
      original.copy(files = Seq.empty)
    ) ++ Seq(
      "s3://bucket/files/segment-10/_data/x.parquet",
      "files/segment-11/_data/x.parquet",
      "files/segment-10/_data/../other/x.parquet",
      "files/segment-10/_data//x.parquet",
      "files/segment-10/x.parquet"
    ).map(path =>
      original.copy(files = Seq(BackfillManifestFile(path, 0L, 5L)))
    ) ++ Seq(
      (-1L, 4L),
      (5L, 5L),
      (5L, 4L),
      (0L, 4L)
    ).map { case (start, end) =>
      original.copy(files =
        Seq(
          BackfillManifestFile("files/segment-10/_data/x.parquet", start, end)
        )
      )
    }
    invalidOps.foreach { op =>
      intercept[IllegalArgumentException](
        result(delta.copy(manifestOps = Seq(op))).toJson
      )
    }
    intercept[IllegalArgumentException](
      result(delta.copy(manifestOps = Seq(original, original))).toJson
    )
  }

  test("result JSON contains snapshot collection schema version") {
    val segment = SegmentBackfillResult(
      segmentId = 10L,
      rowCount = 3L,
      manifestPaths = Seq("s3a://bucket/manifest/1"),
      outputPath = "s3a://bucket/segment/10",
      executionTimeMs = 5L,
      committedVersion = 1L,
      sourceRowCount = 3L,
      matchedRowCount = 2L
    )
    val result = BackfillResult.success(
      segmentResults = Map(10L -> segment),
      executionTimeMs = 10L,
      collectionId = 1L,
      partitionId = 2L,
      schemaVersion = 7,
      newFieldNames = Seq("embedding")
    )

    val json = new ObjectMapper().readTree(result.toJson)

    json.get("schemaVersion").asInt() shouldBe 7
  }

  test("summary does not report a data-file rate for repeated source keys") {
    val segment = SegmentBackfillResult(
      segmentId = 10L,
      rowCount = 100L,
      manifestPaths = Seq.empty,
      outputPath = "s3a://bucket/segment/10",
      executionTimeMs = 5L,
      sourceRowCount = 100L,
      matchedRowCount = 100L
    )
    val result = BackfillResult.success(
      segmentResults = Map(10L -> segment),
      executionTimeMs = 10L,
      collectionId = 1L,
      partitionId = 2L,
      schemaVersion = 7,
      newFieldNames = Seq("value"),
      totalBackfillDataRows = 1L
    )

    result.summary should include(
      "Total Matched Source Rows: 100 (100.00% of source rows)"
    )
    result.summary should not include "10000.00%"
    result.summary should not include "of data file"
  }
  test("result JSON records only a resolved Iceberg snapshot id") {
    val base = BackfillResult.failure(0L)
    val mapper = new ObjectMapper()
    mapper.readTree(base.toJson).has("inputSnapshotId") shouldBe false
    mapper
      .readTree(base.copy(inputSnapshotId = Some(12345678901L)).toJson)
      .get("inputSnapshotId")
      .asLong() shouldBe 12345678901L
  }

}
