package com.zilliz.spark.connector.input

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.zilliz.milvus.storage.write.WriterResources

final case class InputOptions(
    format: String,
    icebergSnapshotId: Option[String] = None
)

final class InputSource private[input] (
    val dataFrame: DataFrame,
    val icebergSnapshotId: Option[Long],
    scope: InputReadScope
) extends AutoCloseable {
  override def close(): Unit = scope.close()
}

/** External-format inputs are independent of Milvus collection row identity. */
object InputAdapter {
  def read(
      spark: SparkSession,
      path: String,
      options: InputOptions,
      binding: InputStorageBinding
  ): InputSource = {
    require(Option(path).exists(_.trim.nonEmpty), "Input path must be nonempty")
    val scope = new InputReadScope(binding)
    try
      options.format.trim match {
        case "parquet" =>
          require(
            options.icebergSnapshotId.isEmpty,
            "icebergSnapshotId requires iceberg input"
          )
          val normalized =
            if (path.startsWith("s3://"))
              "s3a://" + path.stripPrefix("s3://")
            else path
          val frame = spark.read
            .options(scope.optionsOver(spark.sessionState.newHadoopConf()))
            .parquet(normalized)
          require(
            frame.columns.nonEmpty,
            "Backfill parquet is empty (no columns)"
          )
          new InputSource(frame, None, scope)
        case "iceberg" => IcebergInput.read(spark, path, options, scope)
        case other =>
          throw new IllegalArgumentException(
            s"Input format '$other' is not implemented; only parquet and iceberg have a reader"
          )
      }
    catch {
      case error: Throwable =>
        WriterResources.closeAll(error, () => scope.close())
        throw error
    }
  }
}
