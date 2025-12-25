package com.zilliz.spark.connector.read

import org.apache.spark.sql.connector.read.InputPartition
import com.zilliz.spark.connector.MilvusOption

// V2 Storage InputPartition
case class MilvusStorageV2InputPartition(
    manifestJson: String,
    milvusSchemaBytes: Array[Byte],  // Serialized protobuf
    partitionName: String,
    milvusOption: MilvusOption,
    topK: Option[Int] = None,
    queryVector: Option[Array[Float]] = None,
    metricType: Option[String] = None,
    vectorColumn: Option[String] = None,
    segmentID: Long = -1L,  // Add segment ID tracking for V2
    deleteLogPaths: Seq[String] = Seq.empty,  // Delete log file paths for merge deletes
    pkFieldName: String = ""  // Primary key field name
) extends InputPartition

// V1 Binlog InputPartition
case class MilvusInputPartition(
    fieldFiles: Seq[Map[String, String]],
    partition: String = "",
    segmentID: Long = -1L,  // Add segment ID tracking
    deleteLogPaths: Seq[String] = Seq.empty,  // Delete log file paths for merge deletes
    pkFieldIndex: Int = 2  // Primary key field index in schema (default: 2 for V1 format after row_id, timestamp)
) extends InputPartition

