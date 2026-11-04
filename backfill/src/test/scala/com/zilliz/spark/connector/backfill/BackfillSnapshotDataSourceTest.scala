package com.zilliz.spark.connector.backfill

import java.util.Base64
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.snapshot.json.{
  ManifestItemJson,
  SegmentListJson
}
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.table.MilvusTable
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

class BackfillSnapshotDataSourceTest extends AnyFunSuite {
  private val schema = CollectionSchema(
    name = "rows",
    fields = Seq(
      FieldSchema(
        fieldID = 100L,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      )
    )
  )
  private val options = Map(
    MilvusOption.ReadApplyDeletes -> "false",
    MilvusOption.SnapshotCollectionId -> "10",
    MilvusOption.SnapshotSchemaBytes -> Base64.getEncoder.encodeToString(
      schema.toByteArray
    ),
    MilvusOption.SnapshotManifests -> SegmentListJson.encodeManifestItems(
      Seq(
        ManifestItemJson(
          30L,
          """{"ver":1,"base_path":"files/insert_log/10/20/30"}"""
        )
      )
    ),
    BackfillSnapshotDataSource.Partitions -> "30:21"
  )

  test(
    "physical source uses validated partition identity rather than inferring it from paths"
  ) {
    val table = new BackfillSnapshotDataSource()
      .getTable(
        StructType(Seq(StructField("id", LongType, false))),
        Array.empty[Transform],
        options.asJava
      )
      .asInstanceOf[MilvusTable]
    assert(
      table.snapshot.segments.map(segment =>
        segment.id -> segment.partitionId
      ) == Seq(30L -> 21L)
    )
  }

  test(
    "Spark schema inference and table creation append physical metadata only once"
  ) {
    val source = new BackfillSnapshotDataSource()
    val extra =
      options + (MilvusOption.MilvusExtraColumns -> "_segment_id,_row_offset")
    val inferred =
      source.inferSchema(new CaseInsensitiveStringMap(extra.asJava))
    assert(inferred.fieldNames.toSeq == Seq("id"))
    val table = source.getTable(inferred, Array.empty[Transform], extra.asJava)
    assert(
      table.schema().fieldNames.toSeq == Seq("id", "_segment_id", "_row_offset")
    )
  }

  test(
    "physical source refuses delete filtering and inconsistent segment identities"
  ) {
    Seq(
      options - MilvusOption.ReadApplyDeletes,
      options + (MilvusOption.ReadApplyDeletes -> "true"),
      options + (BackfillSnapshotDataSource.Partitions -> "31:21")
    ).foreach { invalid =>
      intercept[IllegalArgumentException] {
        new BackfillSnapshotDataSource().inferSchema(
          new CaseInsensitiveStringMap(invalid.asJava)
        )
      }
    }
  }
}
