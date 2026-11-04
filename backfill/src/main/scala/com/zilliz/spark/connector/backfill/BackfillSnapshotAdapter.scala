package com.zilliz.spark.connector.backfill

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.manifest.SnapshotSegmentEntry
import com.zilliz.spark.connector.write.WriteOptions

/** Resolves additional storage formats while the driver owns the snapshot
  * filesystem. The public entry supplies V3Only; another application can supply
  * its own format implementation without duplicating joins or physical
  * ordering.
  */
trait BackfillSnapshotAdapter {
  def resolve(
      entries: Seq[SnapshotSegmentEntry],
      store: ObjectStore,
      config: BackfillConfig
  ): BackfillAdditionalSegments
}

object BackfillSnapshotAdapter {
  object V3Only extends BackfillSnapshotAdapter {
    override def resolve(
        entries: Seq[SnapshotSegmentEntry],
        store: ObjectStore,
        config: BackfillConfig
    ): BackfillAdditionalSegments = {
      require(
        entries.forall(e => e.segmentLevel == 1L || e.storageVersion == 3L),
        "Public backfill requires Storage V3 data segments"
      )
      BackfillAdditionalSegments.Empty
    }
  }
}

/** Immutable format descriptions can cross Spark's task boundary; service
  * clients, filesystem handles and the driver's configuration cannot.
  */
trait BackfillAdditionalSegments extends Serializable {
  def partitions: Map[Long, Long]

  def read(
      spark: SparkSession,
      schema: StructType,
      options: Map[String, String]
  ): Option[DataFrame]

  def outputPath(
      collectionId: Long,
      partitionId: Long,
      segmentId: Long,
      storage: Map[String, String]
  ): String

  def writer(
      collectionId: Long,
      partitionId: Long,
      segmentId: Long,
      schema: StructType,
      options: WriteOptions,
      storage: Map[String, String]
  ): DataWriter[InternalRow]
}

object BackfillAdditionalSegments {
  object Empty extends BackfillAdditionalSegments {
    override val partitions: Map[Long, Long] = Map.empty
    override def read(
        spark: SparkSession,
        schema: StructType,
        options: Map[String, String]
    ): Option[DataFrame] = None
    override def outputPath(
        collectionId: Long,
        partitionId: Long,
        segmentId: Long,
        storage: Map[String, String]
    ): String = throw new IllegalArgumentException(
      s"Unknown segment $segmentId"
    )
    override def writer(
        collectionId: Long,
        partitionId: Long,
        segmentId: Long,
        schema: StructType,
        options: WriteOptions,
        storage: Map[String, String]
    ): DataWriter[InternalRow] =
      throw new IllegalArgumentException(s"Unknown segment $segmentId")
  }
}

/** Column-file descriptors returned by an additional storage writer. */
final case class BackfillAdapterCommitMessage(
    manifestPaths: Seq[String],
    artifact: BackfillSegmentArtifact
) extends WriterCommitMessage
