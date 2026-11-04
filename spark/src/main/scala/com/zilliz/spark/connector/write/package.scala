package com.zilliz.spark.connector

/** Converts Spark values and resolved Milvus schemas to Arrow column batches.
  * Shared write primitives do not expose a DataSource append operation or own
  * service credentials, job registration or snapshot creation.
  */
package object write
