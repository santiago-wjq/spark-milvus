package com.zilliz.spark.connector.read

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{LongType, IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.spark.connector.MilvusS3Option
import com.zilliz.spark.connector.binlog.LogReader
import io.milvus.grpc.schema.CollectionSchema

// Unified PartitionReaderFactory that dispatches to V1 or V2 readers based on partition type
class MilvusPartitionReaderFactory(
    schema: StructType,
    optionsMap: Map[String, String],  // Use Map instead of CaseInsensitiveStringMap for serialization
    pushedFilters: Array[Filter] = Array.empty[Filter]
) extends PartitionReaderFactory with Logging {

  // Reconstruct CaseInsensitiveStringMap for V1 reader
  @transient private lazy val readerOptions = {
    import scala.jdk.CollectionConverters._
    import java.util.HashMap
    val javaMap = new HashMap[String, String]()
    optionsMap.foreach { case (k, v) => javaMap.put(k, v) }
    MilvusS3Option(new CaseInsensitiveStringMap(javaMap))
  }

  /**
   * Load deleted primary keys from delete log files for V2 partitions
   */
  private def loadDeletedPKsForV2(deleteLogPaths: Seq[String]): Set[Any] = {
    if (deleteLogPaths.isEmpty) {
      return Set.empty
    }

    logInfo(s"Loading deleted PKs from ${deleteLogPaths.size} delete log files")
    val deletedSet = scala.collection.mutable.Set[Any]()
    val objectMapper = LogReader.getObjectMapper()

    deleteLogPaths.foreach { filePath =>
      var inputStream: java.io.InputStream = null
      var fileSystem: FileSystem = null
      try {
        val path = readerOptions.getFilePath(filePath)
        fileSystem = readerOptions.getFileSystem(path)
        inputStream = fileSystem.open(path)

        // Read descriptor event to get data type
        val descriptorEvent = LogReader.readDescriptorEvent(inputStream)
        val dataType = descriptorEvent.data.payloadDataType

        // Read delete events
        var deleteEvent = LogReader.readDeleteEvent(inputStream, objectMapper, dataType)
        while (deleteEvent != null) {
          deleteEvent.pks.foreach { pk =>
            deletedSet += pk
          }
          deleteEvent = LogReader.readDeleteEvent(inputStream, objectMapper, dataType)
        }
      } catch {
        case e: Exception =>
          logWarning(s"Error reading delete log $filePath: ${e.getMessage}")
      } finally {
        if (inputStream != null) {
          try { inputStream.close() } catch { case _: Exception => }
        }
        if (fileSystem != null) {
          try { fileSystem.close() } catch { case _: Exception => }
        }
      }
    }

    logInfo(s"Loaded ${deletedSet.size} deleted PKs")
    deletedSet.toSet
  }

  /**
   * Extract primary key value from a row
   */
  private def getPKValueFromRow(row: InternalRow, pkIndex: Int, rowSchema: StructType): Any = {
    if (row.isNullAt(pkIndex)) {
      return null
    }

    val field = rowSchema.fields(pkIndex)
    field.dataType match {
      case LongType => row.getLong(pkIndex)
      case IntegerType => row.getInt(pkIndex)
      case StringType => row.getUTF8String(pkIndex).toString
      case _ => row.get(pkIndex, field.dataType)
    }
  }

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case milvusPartition: MilvusInputPartition =>
        logInfo(s"Creating V1 reader for partition with segmentID=${milvusPartition.segmentID}, deleteLogPaths=${milvusPartition.deleteLogPaths.size}")
        // Create the V1 data reader with the file map, schema, and options
        new MilvusPartitionReader(
          schema,
          milvusPartition.fieldFiles,
          milvusPartition.partition,
          readerOptions,
          pushedFilters,
          milvusPartition.segmentID,
          milvusPartition.deleteLogPaths,
          milvusPartition.pkFieldIndex
        )

      case p: MilvusStorageV2InputPartition =>
        logInfo(s"Creating V2 reader for partition with deleteLogPaths=${p.deleteLogPaths.size}")

        // Load deleted PKs if delete logs are provided
        val deletedPKs: Set[Any] = if (p.deleteLogPaths.nonEmpty) {
          loadDeletedPKsForV2(p.deleteLogPaths)
        } else {
          Set.empty
        }

        // Storage V2 doesn't support system fields (row_id, timestamp) and extra metadata columns (segment_id, row_offset)
        // Filter them out from the schema for the underlying reader
        val v2Schema = StructType(schema.fields.filter { field =>
          field.name != "row_id" &&
          field.name != "timestamp" &&
          field.name != "segment_id" &&
          field.name != "row_offset"
        })

        // Deserialize the protobuf schema
        val milvusSchema = CollectionSchema.parseFrom(p.milvusSchemaBytes)

        // Find primary key field index in v2Schema
        val pkFieldIndex = if (p.pkFieldName.nonEmpty) {
          v2Schema.fieldNames.indexOf(p.pkFieldName)
        } else {
          -1
        }

        // Create MilvusLoonPartitionReader directly
        val underlyingReader = new MilvusLoonPartitionReader(
          v2Schema,
          p.manifestJson,
          milvusSchema,
          p.milvusOption,
          optionsMap,
          p.topK,
          p.queryVector,
          p.metricType,
          p.vectorColumn,
          pushedFilters
        )

        // If the expected schema includes system/metadata fields, wrap the reader to add them
        val hasRowId = schema.fieldNames.contains("row_id")
        val hasTimestamp = schema.fieldNames.contains("timestamp")
        val hasSegmentId = schema.fieldNames.contains("segment_id")
        val hasRowOffset = schema.fieldNames.contains("row_offset")

        // Create wrapper that handles both metadata fields and delete filtering
        new PartitionReader[InternalRow] {
          private var rowOffset: Long = 0L
          private var currentRow: InternalRow = _
          private var hasNextCalled = false

          override def next(): Boolean = {
            // Loop until we find a non-deleted row or exhaust all rows
            var foundValidRow = false
            while (!foundValidRow && underlyingReader.next()) {
              val row = underlyingReader.get()

              // Check if this row is deleted
              val isDeleted = if (deletedPKs.nonEmpty && pkFieldIndex >= 0 && pkFieldIndex < row.numFields) {
                val pkValue = getPKValueFromRow(row, pkFieldIndex, v2Schema)
                deletedPKs.contains(pkValue)
              } else {
                false
              }

              if (!isDeleted) {
                // Build result row
                currentRow = buildResultRow(row)
                hasNextCalled = true
                foundValidRow = true
              }
            }
            foundValidRow
          }

          private def buildResultRow(row: InternalRow): InternalRow = {
            if (!hasRowId && !hasTimestamp && !hasSegmentId && !hasRowOffset) {
              return row
            }

            val numFields = schema.fields.length
            val resultValues = new Array[Any](numFields)

            var writeIdx = 0
            var readIdx = 0

            // Add system fields with null values
            if (hasRowId) {
              resultValues(writeIdx) = null
              writeIdx += 1
            }
            if (hasTimestamp) {
              resultValues(writeIdx) = null
              writeIdx += 1
            }

            // Copy actual data from underlying reader
            while (readIdx < v2Schema.fields.length) {
              val value = row.get(readIdx, v2Schema.fields(readIdx).dataType)
              resultValues(writeIdx) = value
              readIdx += 1
              writeIdx += 1
            }

            // Add metadata fields (segment_id and row_offset)
            if (hasSegmentId) {
              resultValues(writeIdx) = p.segmentID
              writeIdx += 1
            }
            if (hasRowOffset) {
              resultValues(writeIdx) = rowOffset
              rowOffset += 1
              writeIdx += 1
            }

            InternalRow.fromSeq(resultValues.toSeq)
          }

          override def get(): InternalRow = {
            if (!hasNextCalled) {
              throw new IllegalStateException("next() must be called before get()")
            }
            hasNextCalled = false
            currentRow
          }

          override def close(): Unit = underlyingReader.close()
        }

      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported partition type: ${partition.getClass.getName}"
        )
    }
  }
}
