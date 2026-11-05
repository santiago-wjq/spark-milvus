package com.zilliz.spark.connector.backfill

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite

class BackfillRecoveryTest extends AnyFunSuite {
  test(
    "partial recovery preserves commit identities without producing a successful result envelope"
  ) {
    val segment = SegmentBackfillResult(
      31L,
      3L,
      Seq("segment-31"),
      "segment-31",
      10L,
      committedVersion = 2L,
      sourceVersion = Some(1L),
      sourceRowCount = 3L,
      matchedRowCount = 2L
    )
    val result = BackfillResult
      .success(Map(31L -> segment), 20L, 10L, 21L, 7, Seq("n"))
      .copy(success = false, inputSnapshotId = Some(123L))
    val recovery = BackfillRecovery(result, Seq(33L, 32L, 32L))
    val json = new ObjectMapper().readTree(recovery.toJson)
    assert(json.get("kind").asText() == "backfill-recovery-v1")
    assert(!json.get("complete").asBoolean())
    assert(json.get("failedSegmentIds").toString == "[32,33]")
    assert(!json.has("success") && !json.has("segments"))
    assert(!json.get("completedResult").get("success").asBoolean())
    assert(json.get("completedResult").get("totalSourceRows").asLong() == 3L)
    assert(json.get("completedResult").get("totalRowsWritten").asLong() == 3L)
    assert(json.get("completedResult").get("inputSnapshotId").asLong() == 123L)
    intercept[IllegalArgumentException](
      BackfillRecovery(result.copy(success = true), Seq(32L))
    )
  }

  test(
    "typed recovery failure retains its receipt and cleanup status even when release fails"
  ) {
    val recovery = BackfillRecovery(
      BackfillResult
        .success(Map.empty, 1L, 10L, 21L, 1, Seq("n"))
        .copy(success = false),
      Seq(31L)
    )
    Seq(false, true).foreach { failClose =>
      val failure = new java.io.IOException("write failed")
      val close = new java.io.IOException("close failed")
      val error = BackfillExecutionError(
        "write",
        "write failed",
        Some(failure),
        Some(recovery)
      )
      assert(error.cleanupComplete.isEmpty)
      val result = MilvusBackfill.withCleanup(() => {
        if (failClose) throw close
      })(Left(error))
      val actual = result.left.toOption.get
      assert(actual.stage == "write" && actual.recovery.contains(recovery))
      assert(actual.cleanupComplete.contains(!failClose))
      assert(actual.cause.contains(failure))
      assert(
        failure.getSuppressed.toSeq == (if (failClose) Seq(close)
                                        else Seq.empty)
      )
    }
  }
}
