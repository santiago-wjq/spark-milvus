package com.zilliz.spark.connector.backfill

import java.util.{Base64, Map => JavaMap}

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.snapshot.{
  Snapshot,
  SnapshotCatalog,
  SnapshotOrigin
}
import com.zilliz.milvus.storage.snapshot.json.SegmentListJson
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.table.{MilvusTable, MilvusTables}

/** Internal physical-row source for an already validated backfill plan. A
  * backfill must retain deleted physical rows to keep new columns aligned with
  * the pinned segment files. Ordinary reads continue to require a complete
  * snapshot file, including its delete-only segment roles.
  */
private[backfill] final class BackfillSnapshotDataSource extends TableProvider {
  override def supportsExternalMetadata: Boolean = true

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    // Spark passes this schema to getTable. Metadata columns are appended by
    // MilvusTable once, after schema inference, just as for the public reader.
    MilvusTables.sparkSchemaOf(
      table(options, None).snapshot.schema,
      MilvusOption.readVectorRaw(options),
      MilvusOption.readerFieldIds(options)
    )

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: JavaMap[String, String]
  ): Table = {
    require(
      partitioning.isEmpty,
      "Backfill reads do not accept partition transforms"
    )
    table(new CaseInsensitiveStringMap(properties), Option(schema))
  }

  private def table(
      options: CaseInsensitiveStringMap,
      schema: Option[StructType]
  ): MilvusTable = {
    require(
      options.get(MilvusOption.ReadApplyDeletes) == "false",
      "Backfill physical reads must preserve deleted rows"
    )
    val partitions = Option(options.get(BackfillSnapshotDataSource.Partitions))
      .getOrElse(
        throw new IllegalArgumentException(
          "Backfill segment partitions are required"
        )
      )
      .split(",")
      .filter(_.nonEmpty)
      .map { pair =>
        val values = pair.split(":", -1)
        require(
          values.length == 2,
          s"Invalid backfill segment partition: $pair"
        )
        values(0).toLong -> values(1).toLong
      }
      .toMap
    val manifests = SegmentListJson
      .decodeManifestItems(options.get(MilvusOption.SnapshotManifests))
      .fold(throw _, identity)
    val snapshot = SnapshotCatalog
      .fromLists(
        "backfill-physical-rows",
        options.get(MilvusOption.SnapshotCollectionId).toLong,
        None,
        partitions.values.toSeq.distinct,
        Base64.getDecoder.decode(options.get(MilvusOption.SnapshotSchemaBytes)),
        manifests,
        Option(options.get(StorageProperties.BucketName)).getOrElse(""),
        SnapshotOrigin.Options,
        Option(options.get(StorageProperties.Address)).getOrElse("")
      )
      .fold(throw _, identity)
    require(
      snapshot.segments.map(_.id).toSet == partitions.keySet,
      "Backfill manifest references and segment partitions must match"
    )
    val physical: Snapshot = snapshot.copy(segments =
      snapshot.segments.map(segment =>
        segment.copy(partitionId = partitions(segment.id))
      )
    )
    MilvusTable(physical, MilvusOption(options), schema)
  }
}

private[backfill] object BackfillSnapshotDataSource {
  val Partitions = "milvus.backfill.segment.partitions"

  def partitionOption(partitions: Map[Long, Long]): String =
    partitions.toSeq
      .sortBy(_._1)
      .map { case (segment, partition) =>
        s"$segment:$partition"
      }
      .mkString(",")
}
