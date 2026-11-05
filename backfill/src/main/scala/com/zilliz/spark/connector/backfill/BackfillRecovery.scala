package com.zilliz.spark.connector.backfill

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

/** Durable evidence of an incomplete write. This is never a submission result:
  * committed segments may have advanced their manifests even though the job
  * failed. The caller must reconcile them with the pinned snapshot before
  * retry.
  */
final case class BackfillRecovery(
    result: BackfillResult,
    failedSegmentIds: Seq[Long]
) {
  require(!result.success, "Recovery cannot contain a successful job result")

  def toJson: String = {
    val mapper = new ObjectMapper()
    val root = mapper.createObjectNode()
    root.put("kind", "backfill-recovery-v1")
    root.put("complete", false)
    val failed = root.putArray("failedSegmentIds")
    failedSegmentIds.distinct.sorted.foreach(failed.add)
    root.set[JsonNode](
      "completedResult",
      mapper.readTree(result.toJson)
    )
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root)
  }
}
