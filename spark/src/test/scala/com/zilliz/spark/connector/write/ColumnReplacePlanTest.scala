package com.zilliz.spark.connector.write

import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.snapshot.{
  CollectionIndex,
  Snapshot,
  SnapshotOrigin
}
import com.zilliz.milvus.storage.write.ColumnGroupFiles
import com.zilliz.spark.connector.types.{RowIdentity, SparkTypes}
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** Planning a column replacement: what the DataFrame must carry, which fields
  * can be replaced, and the records it keeps. No storage is opened.
  */
class ColumnReplacePlanTest extends AnyFunSuite with Matchers {

  private val fields = Seq(
    FieldSchema(
      fieldID = 100L,
      name = "id",
      dataType = DataType.Int64,
      isPrimaryKey = true
    ),
    FieldSchema(
      fieldID = 101L,
      name = "score",
      dataType = DataType.Int64,
      nullable = true
    ),
    FieldSchema(
      fieldID = 102L,
      name = "embedding",
      dataType = DataType.FloatVector,
      typeParams = Seq(KeyValuePair("dim", "2"))
    ),
    FieldSchema(
      fieldID = 103L,
      name = "tenant",
      dataType = DataType.Int64,
      isPartitionKey = true
    ),
    FieldSchema(
      fieldID = 104L,
      name = "label",
      dataType = DataType.Int64,
      isClusteringKey = true
    )
  )
  private val schema = CollectionSchema(name = "rows", fields = fields)
  private val embeddingIndex = CollectionIndex(
    collectionId = 10L,
    fieldId = 102L,
    indexId = 1L,
    name = "embedding_idx",
    typeParameters = Map.empty,
    indexParameters = Map.empty,
    userIndexParameters = Map.empty
  )
  private def snapshot(
      indexes: Option[Vector[CollectionIndex]] = Some(Vector(embeddingIndex))
  ) = Snapshot(
    name = "source",
    collectionId = 10L,
    createdAt = Some(1L),
    schema = schema,
    partitionIds = Seq(20L),
    segments = Seq.empty,
    origin = SnapshotOrigin.Options,
    bucket = "",
    indexes = indexes,
    id = Some(7L)
  )
  private def column(name: String): StructField = {
    val field = fields.find(_.name == name).get
    StructField(name, SparkTypes.toDataType(field), nullable = true)
  }
  private def input(columns: StructField*): StructType =
    StructType(RowIdentity.field +: columns)

  private def refusal(frame: StructType, at: Snapshot = snapshot()): String =
    intercept[IllegalArgumentException](
      ColumnReplaceTables.resolveTargets(frame, at)
    ).getMessage

  test("the replaced columns carry their field ids and nullability") {
    val resolved =
      ColumnReplaceTables.resolveTargets(input(column("score")), snapshot())
    resolved.fieldNames.toSeq shouldBe Seq("score")
    resolved.head.metadata.getLong(
      FieldMetadata.MilvusFieldIdMetadataKey
    ) shouldBe 101L
    resolved.head.nullable shouldBe true
  }

  test("the DataFrame must carry the row identity unchanged") {
    refusal(StructType(Seq(column("score")))) should include(
      "has no '_milvus_row_id' column"
    )
    refusal(
      StructType(
        Seq(StructField(RowIdentity.ColumnName, LongType), column("score"))
      )
    ) should include("carry the column from the physical read unchanged")
    refusal(input()) should include("names no column to replace")
  }

  test("fields Milvus owns or derives are refused") {
    refusal(input(column("id"))) should include("'id' (the primary key)")
    refusal(input(column("label"))) should include(
      "'label' (the clustering key)"
    )
    refusal(input(column("tenant"))) should include("partition key")
    refusal(
      input(StructField("missing", LongType))
    ) should include("are not fields of collection")
  }

  test(
    "an indexed field is refused, and so is a snapshot without index metadata"
  ) {
    refusal(input(column("embedding"))) should include(
      "'embedding' (index 'embedding_idx')"
    )
    refusal(input(column("score")), snapshot(indexes = None)) should include(
      "does not describe its indexes"
    )
    ColumnReplaceTables
      .resolveTargets(input(column("embedding")), snapshot(Some(Vector.empty)))
      .fieldNames
      .toSeq shouldBe Seq("embedding")
  }

  test("tasks find the row identity and each replaced column by name") {
    val targets =
      ColumnReplaceTables.resolveTargets(input(column("score")), snapshot())
    val plan = ColumnReplacePlan(
      snapshot = SourceSnapshot("snapshot.json", 7L, "source", 10L, 0),
      targets = Seq(TargetColumn("score", 101L)),
      targetSchema = targets,
      segments = Seq(PlannedSegment(30L, 20L, "base/30", 2L, 8L)),
      storage = Map.empty,
      bucket = "",
      result = WriteResultLocation("result", Map.empty, "result"),
      params = WriteOptions(replaceColumns = true)
    )
    val task = ColumnReplaceTask.of(
      plan,
      StructType(Seq(column("score"), RowIdentity.field))
    )
    task.rowIdOrdinal shouldBe 1
    task.targetOrdinals shouldBe Seq(0)
    task.snapshotId shouldBe 7L
  }

  test("a statistic names its field by the last dotted part of its key") {
    ColumnReplaceDataWriter.statisticFieldId("bloom_filter.100") shouldBe Some(
      100L
    )
    ColumnReplaceDataWriter.statisticFieldId("text_index.101") shouldBe Some(
      101L
    )
    ColumnReplaceDataWriter.statisticFieldId(
      "column_write.operation"
    ) shouldBe None
    ColumnReplaceDataWriter.statisticFieldId("trailing.") shouldBe None
    ColumnReplaceDataWriter.statisticFieldId("nodot") shouldBe None
  }

  test("a write result round-trips through its published JSON") {
    val result = WriteResult(
      operationId = "op",
      operation = "replace_columns",
      createdAt = "2026-11-10T00:00:00Z",
      committedAt = "2026-11-10T00:01:00Z",
      applicationId = "app",
      snapshot =
        SourceSnapshot("s3://bucket/snapshot.json", 7L, "source", 10L, 3),
      changedFields = Seq(TargetColumn("score", 101L)),
      segments = Seq(
        CommittedSegment(
          PlannedCommit(
            30L,
            20L,
            "files/insert_log/10/20/30",
            2L,
            8L,
            Seq(
              ColumnGroupFiles(
                Seq("101"),
                Seq("files/insert_log/10/20/30/_data/0_a.parquet"),
                Seq(8L),
                "parquet"
              )
            )
          ),
          version = 3L
        )
      )
    )
    WriteResultRecords.parseResult(
      WriteResultRecords.resultNode(result).toString
    ) shouldBe result
    val other = WriteResultRecords.resultNode(result).put("record", "failure")
    intercept[IllegalArgumentException](
      WriteResultRecords.parseResult(other.toString)
    ).getMessage should include("not a write result record")
  }

  test("a write result address resolves like a snapshot path") {
    val storage = Map(
      StorageProperties.StorageType -> "remote",
      StorageProperties.BucketName -> "main",
      StorageProperties.Address -> "localhost:9000"
    )
    val local = WriteResultLocation.resolve("/tmp/run/write-result/", storage)
    local.key shouldBe "tmp/run/write-result"
    local.properties(StorageProperties.StorageType) shouldBe "local"
    local.resultKey shouldBe "tmp/run/write-result/_result.json"
    WriteResultLocation
      .resolve("file:///tmp/run/write-result", storage)
      .key shouldBe "tmp/run/write-result"

    val remote =
      WriteResultLocation.resolve("s3://other/run/write-result", storage)
    remote.key shouldBe "run/write-result"
    remote.properties(StorageProperties.BucketName) shouldBe "other"
    remote.claimBasePath shouldBe "run/write-result/_claim"

    val relative = WriteResultLocation.resolve("run/write-result", storage)
    relative.key shouldBe "run/write-result"
    relative.properties(StorageProperties.BucketName) shouldBe "main"

    intercept[IllegalArgumentException](
      WriteResultLocation.resolve("../outside", storage)
    )
    intercept[IllegalArgumentException](
      WriteResultLocation.resolve(" ", storage)
    )
  }
}
