package com.zilliz.milvus.storage.segcore

import java.nio.file.Path
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper

import com.zilliz.milvus.jni.segcore.SegcoreNative
import com.zilliz.milvus.storage.io.{LocalObjectStore, ReadFixtures}
import com.zilliz.milvus.storage.snapshot.{
  Segment,
  SegmentIndexes,
  Snapshot,
  SnapshotCatalog
}
import io.milvus.grpc.schema.{
  CollectionSchema,
  DataType => MilvusDataType,
  FieldSchema
}

/** A public read fixture as segcore and the JVM evaluator see it.
  *
  * The snapshot is resolved into the read model as a read resolves it, over the
  * local store, which has no bucket. segcore gets a collection with the
  * fixture's schema and the system fields RootCoord appends to every collection
  * (`createCollectionTask.appendSysFields`): a sealed segment's column groups
  * carry `RowID` and `Timestamp`, and segcore resolves each column through the
  * schema, while snapshot schemas list only the user's fields. Segments load
  * from their V3 manifests with the load info [[SegcoreLoadInfo]] writes from
  * that model, with only `loadFields`; segcore loads any other field lazily, on
  * the first request that touches it.
  *
  * The literal input rows come from `expected.json`, never from a read of the
  * stored bytes.
  */
private[segcore] final class SegcoreFixture(
    val name: String,
    root: Path,
    loadFields: Seq[Long]
) extends AutoCloseable {
  import SegcoreFixture.ExpectedSegment

  ReadFixtures.copyTo(s"collections/$name", root)

  val snapshot: Snapshot =
    new SnapshotCatalog(new LocalObjectStore(root.toString), "")
      .read("snapshot.json")

  val fields: Seq[FieldSchema] = snapshot.schema.fields

  val collection: Long = SegcoreNative.newCollection(
    CollectionSchema(
      name = snapshot.schema.name,
      fields = fields ++ Seq(
        FieldSchema(
          fieldID = 0,
          name = "RowID",
          description = "row id",
          dataType = MilvusDataType.Int64
        ),
        FieldSchema(
          fieldID = 1,
          name = "Timestamp",
          description = "time stamp",
          dataType = MilvusDataType.Int64
        )
      )
    ).toByteArray
  )
  SegcoreNative.setLoadFields(collection, loadFields.toArray)

  /** expected.json's segments: id, literal input rows and deleted ids. */
  val expected: Seq[ExpectedSegment] = {
    val tree = new ObjectMapper().readTree(root.resolve("expected.json").toFile)
    tree.get("segments").elements().asScala.toSeq.map { segment =>
      val rows = segment.get("rows").elements().asScala.toSeq.map { row =>
        val category = row.get("category")
        SegcoreFixture.Row(
          row.get("id").asLong(),
          if (category == null || category.isNull) None
          else Some(category.asLong())
        )
      }
      val deleted =
        segment.get("deletedIds").elements().asScala.map(_.asLong()).toSet
      ExpectedSegment(segment.get("id").asLong(), rows, deleted)
    }
  }

  /** The read model's segment `id`. */
  def segment(id: Long): Segment =
    snapshot.segments
      .find(_.id == id)
      .getOrElse(throw new NoSuchElementException(s"$name has no segment $id"))

  /** A sealed segment for `id`, not loaded yet. Only the indexes of the fields
    * it loads go into its load info.
    */
  def newSegment(id: Long): Long = {
    val model = segment(id)
    val indexes = model.indexes match {
      case SegmentIndexes.Available(all) =>
        all.filter(index => loadFields.contains(index.fieldId))
      case _ => Vector.empty
    }
    SegcoreNative.newSealedSegment(
      collection,
      id,
      false,
      SegcoreLoadInfo(
        snapshot.collectionId,
        model,
        indexes,
        Some(root)
      ).toByteArray
    )
  }

  override def close(): Unit = SegcoreNative.deleteCollection(collection)
}

private[segcore] object SegcoreFixture {

  /** A fixture's input row: the primary key and a nullable category. */
  final case class Row(id: Long, category: Option[Long])

  final case class ExpectedSegment(
      id: Long,
      rows: Seq[Row],
      deleted: Set[Long]
  )
}
