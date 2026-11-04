package com.zilliz.spark.connector.backfill

import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  BaseVariableWidthVector,
  VarCharVector,
  VectorSchemaRoot
}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.write.{
  ManifestTransaction,
  V3SegmentWriter,
  WriterResources
}
import com.zilliz.spark.connector.write.{ArrowWriter, WriteOptions, WriteSchema}

/** Writes new files inside an existing V3 segment. Delta output never opens a
  * manifest transaction; the receiver applies its operations to the current
  * revision. Legacy output uses exactly the snapshot revision as its base.
  */
private[backfill] final class BackfillV3Writer(
    schema: StructType,
    params: WriteOptions,
    storage: Map[String, String],
    basePath: String,
    sourceVersion: Long,
    receiverCapability: String
) extends DataWriter[InternalRow] {
  require(sourceVersion > 0L, "backfill requires a committed snapshot manifest")
  require(
    Set(
      BackfillConfig.LegacyReceiverCapability,
      BackfillConfig.ManifestDeltaReceiverCapability
    )
      .contains(receiverCapability),
    s"Unsupported backfill receiver capability: $receiverCapability"
  )
  private val fieldIds = schema.fields.map { field =>
    require(
      field.metadata.contains(FieldMetadata.MilvusFieldIdMetadataKey),
      s"Column '${field.name}' has no resolved Milvus field id"
    )
    field.name -> field.metadata.getLong(FieldMetadata.MilvusFieldIdMetadataKey)
  }.toMap
  private val columns =
    schema.fieldNames.toSeq.map(name => fieldIds(name).toString)
  require(
    columns.nonEmpty && columns.distinct.size == columns.size,
    "backfill columns must be non-empty and unique"
  )
  private val arrowSchema =
    WriteSchema.arrowSchemaFor(schema, fieldIds, withSystemFields = false)
  private val allocator = new RootAllocator(Long.MaxValue)
  private var root: VectorSchemaRoot = null
  private var writer: V3SegmentWriter = null
  private var batchRows = 0
  private var closed = false

  try root = newRoot()
  catch {
    case failure: Throwable =>
      WriterResources.closeAll(failure, () => cleanup())
      throw failure
  }

  override def write(row: InternalRow): Unit = {
    require(!closed, "backfill writer is closed")
    ArrowWriter.internalRowToArrow(root, batchRows, row, schema)
    batchRows += 1
    root.setRowCount(batchRows)
    if (batchRows >= params.batchRows) flush()
  }

  override def commit(): BackfillV3CommitMessage = {
    require(!closed, "backfill writer is closed")
    WriterResources.closing(cleanup()) {
      flush()
      require(
        writer != null && writer.rows > 0L,
        "backfill cannot publish an empty segment"
      )
      val rows = writer.rows
      val groups = writer.finish()
      WriterResources.closing(groups.close()) {
        if (
          receiverCapability == BackfillConfig.ManifestDeltaReceiverCapability
        ) {
          val operations = (0 until groups.size).map { index =>
            val groupColumns = groups.columns(index)
            val paths = groups.files(index)
            val counts = groups.rowCounts(index)
            require(
              groupColumns.size == 1,
              "backfill delta requires single-field column groups"
            )
            require(
              groups.format(index) == "parquet",
              "backfill delta requires fresh parquet files"
            )
            require(
              paths.size == counts.size && paths.nonEmpty,
              "backfill column-group files and row counts differ"
            )
            require(
              counts.sum == rows,
              "backfill column group does not cover every segment row"
            )
            // The pinned parquet writer starts each new file at row zero. The
            // JNI exposes its row count but not optional file_size/footer_size
            // hints; omitting these hints uses the reader's normal footer I/O.
            val files = paths.zip(counts).map { case (path, count) =>
              BackfillManifestFile(path, 0L, count)
            }
            BackfillManifestOp("replace", groupColumns, "parquet", files, rows)
          }
          require(
            operations.flatMap(_.columns).toSet == columns.toSet,
            "backfill writer returned unexpected columns"
          )
          BackfillV3CommitMessage(rows, -1L, None, operations)
        } else {
          val version = ManifestTransaction.commit(
            basePath,
            storage,
            groups,
            ManifestTransaction.ReplaceColumns(columns),
            readVersion = sourceVersion
          )
          BackfillV3CommitMessage(rows, version, Some(sourceVersion), Seq.empty)
        }
      }
    }
  }

  // Files live beside existing segment data. Neither abort nor close may
  // delete the segment or files potentially retained for uncertain recovery.
  override def abort(): Unit = cleanup()
  override def close(): Unit = cleanup()

  private def flush(): Unit = {
    if (batchRows == 0) return
    if (writer == null) {
      val properties = params.writerProperties(storage) ++ Map(
        "writer.format" -> "parquet",
        "writer.split.schema_based.formats" -> columns
          .map(_ => "parquet")
          .mkString(",")
      )
      writer = new V3SegmentWriter(
        basePath,
        arrowSchema,
        properties,
        allocator,
        columns.map(id => s"^${id}$$")
      )
    }
    root.setRowCount(batchRows)
    writer.write(root)
    val replacement = newRoot()
    val previous = root
    root = replacement
    batchRows = 0
    previous.close()
  }

  private def newRoot(): VectorSchemaRoot = {
    val value = VectorSchemaRoot.create(arrowSchema, allocator)
    try {
      value.getFieldVectors.asScala.foreach {
        case vector: VarCharVector =>
          vector.setInitialCapacity(
            params.batchRows,
            params.variableWidthBytesPerValue
          )
        case vector: BaseVariableWidthVector =>
          vector.setInitialCapacity(
            params.batchRows,
            params.variableWidthBytesPerValue
          )
        case vector => vector.setInitialCapacity(params.batchRows)
      }
      value.allocateNew()
      value.setRowCount(0)
      value
    } catch {
      case failure: Throwable =>
        WriterResources.closeAll(failure, () => value.close())
        throw failure
    }
  }

  private def cleanup(): Unit = {
    if (closed) return
    closed = true
    WriterResources.closeAll(
      null,
      () => if (writer != null) writer.close(),
      () => if (root != null) root.close(),
      () => allocator.close()
    )
  }
}

private[backfill] final case class BackfillV3CommitMessage(
    rowCount: Long,
    committedVersion: Long,
    sourceVersion: Option[Long],
    manifestOps: Seq[BackfillManifestOp]
) extends WriterCommitMessage
