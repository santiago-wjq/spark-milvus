package com.zilliz.spark.connector.input

import org.apache.iceberg.hadoop.HadoopFileIO
import org.apache.iceberg.spark.Spark3Util
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
        case "iceberg" => readIceberg(spark, path, options, scope)
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

  private def readIceberg(
      spark: SparkSession,
      identifier: String,
      options: InputOptions,
      scope: InputReadScope
  ): InputSource = {
    require(
      !identifier.contains("://") && !identifier.startsWith("/"),
      "Iceberg input must be a catalog-qualified identifier (catalog.db.table), not a raw file/object-storage path"
    )
    val parts =
      spark.sessionState.sqlParser.parseMultipartIdentifier(identifier)
    require(
      parts.size >= 3,
      "Iceberg input must be a catalog-qualified identifier (catalog.db.table)"
    )
    val catalog = parts.head
    val prefix = s"spark.sql.catalog.$catalog"
    val settings = spark.conf.getAll.filter { case (key, _) =>
      key == prefix || key.startsWith(prefix + ".")
    }
    require(
      settings.get(prefix).contains("org.apache.iceberg.spark.SparkCatalog"),
      "Iceberg input requires a configured org.apache.iceberg.spark.SparkCatalog"
    )
    require(
      settings
        .get(prefix + ".io-impl")
        .forall(_ == classOf[HadoopFileIO].getName),
      "Iceberg input storage binding requires HadoopFileIO"
    )
    require(
      settings.get(prefix + ".type").forall(Set("hadoop", "hive")),
      "Iceberg input storage binding supports Hadoop and Hive catalogs"
    )
    require(
      settings
        .get(prefix + ".catalog-impl")
        .forall(
          Set(
            "org.apache.iceberg.hadoop.HadoopCatalog",
            "org.apache.iceberg.hive.HiveCatalog"
          )
        ),
      "Iceberg input storage binding does not support custom catalog implementations"
    )
    val requested = options.icebergSnapshotId.map { value =>
      val trimmed = value.trim
      require(
        trimmed.nonEmpty && trimmed.forall(_.isDigit),
        "Iceberg snapshot-id must be a positive integer"
      )
      val id = trimmed.toLong
      require(id > 0, "Iceberg snapshot-id must be a positive integer")
      id
    }

    // Each input owns its catalog and FileIO. Catalog initialization reads the
    // active session; withActive scopes only this thread and restores its caller.
    val session = spark.newSession()
    val inherited = session.sessionState.newHadoopConf()
    val hadoopPrefix = prefix + ".hadoop."
    (session.conf.getAll ++ settings).foreach { case (key, value) =>
      if (
        key.startsWith(hadoopPrefix) &&
        InputStorageBinding.isBindingKey(key.stripPrefix(hadoopPrefix))
      ) {
        inherited.set(key.stripPrefix(hadoopPrefix), value)
        session.conf.unset(key)
      }
    }
    settings
      .filterNot { case (key, _) =>
        key.startsWith(hadoopPrefix) &&
        InputStorageBinding.isBindingKey(key.stripPrefix(hadoopPrefix))
      }
      .foreach { case (key, value) => session.conf.set(key, value) }
    scope.optionsOver(inherited).foreach { case (key, value) =>
      session.conf.set(s"$prefix.hadoop.$key", value)
    }
    session.withActive {
      // Iceberg's identifier helper catches catalog initialization exceptions
      // and falls back to spark_catalog. Preserve the original storage failure.
      session.sessionState.catalogManager.catalog(catalog)
      val table = Spark3Util.loadIcebergTable(session, identifier)
      require(
        table.io().isInstanceOf[HadoopFileIO],
        "Iceberg input storage binding requires the table's actual FileIO to be HadoopFileIO"
      )
      val snapshot = requested match {
        case Some(id) =>
          Option(table.snapshot(id)).getOrElse(
            throw new IllegalArgumentException(
              s"Iceberg snapshot-id $id does not exist in $identifier"
            )
          )
        case None =>
          Option(table.currentSnapshot()).getOrElse(
            throw new IllegalArgumentException(
              s"Iceberg input $identifier has no current snapshot"
            )
          )
      }
      val frame = session.read
        .format("iceberg")
        .option("snapshot-id", snapshot.snapshotId().toString)
        .load(identifier)
      require(
        frame.columns.nonEmpty,
        "Backfill iceberg input is empty (no columns)"
      )
      new InputSource(frame, Some(snapshot.snapshotId()), scope)
    }
  }
}
