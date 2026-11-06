package com.zilliz.spark.connector

/** Independently binds Parquet and pinned Iceberg input to its own Hadoop
  * identity and resources without changing the target storage configuration or
  * owning a Milvus service connection. Iceberg input exists only on Spark lines
  * for which Iceberg publishes a runtime; on the others IcebergInput refuses it
  * before reading.
  */
package object input
