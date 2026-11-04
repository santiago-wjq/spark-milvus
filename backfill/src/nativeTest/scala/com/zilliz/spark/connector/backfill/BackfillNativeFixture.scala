package com.zilliz.spark.connector.backfill

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.Schema
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import com.zilliz.milvus.storage.manifest.{
  SnapshotSegmentEntry,
  SnapshotSegmentSchema
}
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.write.{
  ManifestTransaction,
  V3SegmentWriter,
  WriterResources
}
import com.zilliz.spark.connector.write.{ArrowWriter, WriteSchema}

/** Minimal test-only source snapshots; no application append or snapshot-output
  * implementation is required to test public offline backfill.
  */
private[backfill] object BackfillNativeFixture {
  def rootFor(storage: Map[String, String]): String =
    if (storage.get("fs.storage_type").contains("local")) ""
    else storage.getOrElse("fs.root_path", "files")

  def write(
      base: String,
      schema: StructType,
      storage: Map[String, String],
      rows: Seq[InternalRow]
  ): Long = {
    val fieldIds = schema.fields
      .map(field =>
        field.name -> field.metadata
          .getLong(FieldMetadata.MilvusFieldIdMetadataKey)
      )
      .toMap
    val arrow =
      WriteSchema.arrowSchemaFor(schema, fieldIds, withSystemFields = false)
    val allocator = new RootAllocator(Long.MaxValue)
    WriterResources.closing(allocator.close()) {
      val batch = VectorSchemaRoot.create(arrow, allocator)
      WriterResources.closing(batch.close()) {
        batch.allocateNew()
        rows.zipWithIndex.foreach { case (row, index) =>
          ArrowWriter.internalRowToArrow(batch, index, row, schema)
        }
        batch.setRowCount(rows.size)
        val writer = new V3SegmentWriter(base, arrow, storage, allocator)
        WriterResources.closing(writer.close()) {
          writer.write(batch)
          val groups = writer.finish()
          WriterResources.closing(groups.close()) {
            ManifestTransaction.commit(
              base,
              storage,
              groups,
              ManifestTransaction.AppendFiles
            )
          }
        }
      }
    }
  }

  def encode(entry: SnapshotSegmentEntry): Array[Byte] = {
    val schema = SnapshotSegmentSchema.schemaFor(4)
    val record = blank(schema).asInstanceOf[GenericRecord]
    record.put("segment_id", entry.segmentId)
    record.put("partition_id", entry.partitionId)
    record.put("segment_level", entry.segmentLevel)
    record.put("num_of_rows", entry.numOfRows)
    record.put("storage_version", entry.storageVersion)
    val bytes = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().directBinaryEncoder(bytes, null)
    new GenericDatumWriter[GenericRecord](schema).write(record, encoder)
    encoder.flush()
    bytes.toByteArray
  }

  private def blank(schema: Schema): AnyRef = schema.getType match {
    case Schema.Type.RECORD =>
      val record = new GenericData.Record(schema)
      schema.getFields.asScala.foreach(field =>
        record.put(field.name, blank(field.schema))
      )
      record
    case Schema.Type.ARRAY   => new java.util.ArrayList[AnyRef]()
    case Schema.Type.MAP     => new java.util.HashMap[String, AnyRef]()
    case Schema.Type.STRING  => ""
    case Schema.Type.BYTES   => ByteBuffer.allocate(0)
    case Schema.Type.LONG    => Long.box(0L)
    case Schema.Type.INT     => Int.box(0)
    case Schema.Type.BOOLEAN => Boolean.box(false)
    case Schema.Type.UNION   => blank(schema.getTypes.get(0))
    case Schema.Type.NULL    => null
    case other =>
      throw new IllegalArgumentException(s"Unsupported fixture type $other")
  }
}
