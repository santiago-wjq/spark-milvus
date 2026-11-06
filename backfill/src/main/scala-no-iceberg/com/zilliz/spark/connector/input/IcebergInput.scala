package com.zilliz.spark.connector.input

import org.apache.spark.sql.SparkSession
import org.apache.spark.SPARK_VERSION

/** Iceberg publishes no runtime for the selected Spark line, so Iceberg input
  * is refused before anything is read.
  */
private[connector] object IcebergInput {
  val available: Boolean = false

  private[input] def read(
      spark: SparkSession,
      identifier: String,
      options: InputOptions,
      scope: InputReadScope
  ): InputSource =
    throw new IllegalArgumentException(
      s"Iceberg input is not available on Spark $SPARK_VERSION: " +
        "Iceberg publishes no runtime for this Spark line yet"
    )
}
