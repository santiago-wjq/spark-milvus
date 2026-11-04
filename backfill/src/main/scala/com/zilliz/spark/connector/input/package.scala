package com.zilliz.spark.connector

/** Independently binds Parquet and pinned Iceberg input to its own Hadoop
  * identity and resources without changing the target storage configuration or
  * owning a Milvus service connection.
  */
package object input
