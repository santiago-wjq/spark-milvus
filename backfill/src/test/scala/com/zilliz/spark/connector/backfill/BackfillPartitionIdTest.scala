package com.zilliz.spark.connector.backfill

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The top-level `partitionId` is consumed by Milvus `CommitBackfillResult`,
  * which only treats `0` as "no partition restriction". Any other value is
  * compared against every segment's partition.
  */
class BackfillPartitionIdTest extends AnyFunSuite with Matchers {

  test("single partition reports that partition ID") {
    MilvusBackfill.resolveResultPartitionId(
      Set(450000000002L)
    ) shouldBe 450000000002L
  }

  test("segments spanning several partitions report 0, not -1") {
    MilvusBackfill.resolveResultPartitionId(
      Set(450000000002L, 450000000003L, 450000000004L)
    ) shouldBe 0L
  }

  test("no partitions reports 0") {
    MilvusBackfill.resolveResultPartitionId(Set.empty) shouldBe 0L
  }

  test("result JSON carries 0 for a multi-partition backfill") {
    val segment = SegmentBackfillResult(
      segmentId = 10L,
      rowCount = 3L,
      manifestPaths = Seq("s3a://bucket/manifest"),
      executionTimeMs = 1L,
      outputPath = "s3a://bucket/output"
    )
    val result = BackfillResult.success(
      segmentResults = Map(10L -> segment),
      executionTimeMs = 1L,
      collectionId = 1L,
      partitionId = MilvusBackfill.resolveResultPartitionId(Set(2L, 3L)),
      schemaVersion = 1,
      newFieldNames = Seq("f")
    )
    val json = new ObjectMapper().readTree(result.toJson)
    json.get("partitionId").asLong() shouldBe 0L
  }
  private def metadata(
      partitions: String,
      manifest: String
  ): com.zilliz.milvus.storage.snapshot.json.SnapshotJson = {
    val encoded = new ObjectMapper().writeValueAsString(manifest)
    com.zilliz.milvus.storage.snapshot.json.SnapshotJson
      .parse(s"""{
      "snapshot_info":{"collection_id":"10","partition_ids":[$partitions]},
      "collection":{"schema":{"name":"rows","fields":[]}},
      "storagev2_manifest_list":[{"segmentID":31,"manifest":$encoded}]
    }""")
      .fold(throw _, identity)
  }

  test(
    "V3 staging paths use authoritative per-segment Avro partition identity"
  ) {
    val snapshot =
      metadata("21,22", """{"ver":1,"base_path":"staging/job/task_0_2"}""")
    val (_, partitions, paths) = MilvusBackfill.extractMetadataFromSnapshot(
      snapshot,
      authoritativePartitions = Map(31L -> 22L)
    )
    partitions shouldBe Map(31L -> 22L)
    paths shouldBe Map(31L -> "staging/job/task_0_2")
  }

  test(
    "a single snapshot partition resolves arbitrary paths without guessing directory names"
  ) {
    val snapshot = metadata("21", """{"ver":1,"base_path":"segment"}""")
    MilvusBackfill.extractMetadataFromSnapshot(snapshot)._2 shouldBe Map(
      31L -> 21L
    )
  }

  test(
    "multi-partition snapshots require per-segment identity even for insert_log-looking paths"
  ) {
    val snapshot =
      metadata("21,22", """{"ver":1,"base_path":"files/insert_log/10/21/31"}""")
    intercept[IllegalArgumentException](
      MilvusBackfill.extractMetadataFromSnapshot(snapshot)
    ).getMessage should include("authoritative")
  }

  test(
    "malformed manifests and partitions outside the snapshot fail rather than omit segments"
  ) {
    intercept[Exception](
      MilvusBackfill.extractMetadataFromSnapshot(metadata("21", "not-json"))
    )
    val snapshot = metadata("21", """{"ver":1,"base_path":"segment"}""")
    intercept[IllegalArgumentException](
      MilvusBackfill.extractMetadataFromSnapshot(
        snapshot,
        authoritativePartitions = Map(31L -> 99L)
      )
    ).getMessage should include("absent")
  }

}
