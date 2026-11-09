package com.zilliz.spark.connector.backfill

import java.nio.charset.StandardCharsets

import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.{
  NativeObjectStore,
  NativeWritableObjectStore
}
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.write.WriterResources

/** Native snapshot and result access through the target storage identity. */
object BackfillStorage {

  /** The output uses the target storage identity and never mutates Spark's
    * configuration.
    */
  def writeResultJson(
      spark: SparkSession,
      result: BackfillResult,
      rawOutputPath: String,
      config: BackfillConfig
  ): Either[BackfillError, Unit] = {
    try {
      val resolved = config.copy(storageProperties =
        config.resolveStorage(spark.sessionState.newHadoopConf())
      )
      writeResolvedResultJson(result, rawOutputPath, resolved)
    } catch {
      case e: Exception =>
        Left(
          WriteError(
            -1L,
            rawOutputPath,
            s"Failed to write result JSON: ${e.getMessage}",
            Some(e)
          )
        )
    }
  }

  /** Writes a durable result using an already resolved target storage identity.
    */
  def writeResolvedResultJson(
      result: BackfillResult,
      rawOutputPath: String,
      config: BackfillConfig
  ): Either[BackfillError, Unit] =
    writeResolvedJson(result.toJson, rawOutputPath, config)

  /** Recovery evidence has its own envelope and is never a submission result.
    */
  def writeResolvedRecoveryJson(
      recovery: BackfillRecovery,
      rawOutputPath: String,
      config: BackfillConfig
  ): Either[BackfillError, Unit] =
    writeResolvedJson(recovery.toJson, rawOutputPath, config)

  private def writeResolvedJson(
      json: String,
      rawOutputPath: String,
      config: BackfillConfig
  ): Either[BackfillError, Unit] = {
    try {
      val (properties, key) = location(rawOutputPath, config)
      val store = NativeWritableObjectStore.open(properties)
      WriterResources.closing(store.close()) {
        store.write(key, json.getBytes(StandardCharsets.UTF_8))
      }
      Right(())
    } catch {
      case e: Exception =>
        Left(
          WriteError(
            -1L,
            rawOutputPath,
            s"Failed to write result JSON: ${e.getMessage}",
            Some(e)
          )
        )
    }
  }

  /** Local paths are independent of the target segment filesystem root. */
  private def location(
      path: String,
      config: BackfillConfig
  ): (Map[String, String], String) = {
    val local = !path.contains("://") || path.startsWith("file://")
    if (local) {
      val absolute = (if (path.startsWith("file://"))
                        java.nio.file.Paths.get(java.net.URI.create(path))
                      else java.nio.file.Paths.get(path)).toAbsolutePath
        .normalize()
      (
        StorageProperties.from(
          Map(
            StorageProperties.StorageType -> "local",
            StorageProperties.RootPath -> "/"
          )
        ),
        absolute.toString.stripPrefix("/")
      )
    } else {
      val options = config.targetStorageOptions
      val located = StoragePath.parseMilvus(
        path,
        options.getOrElse(StorageProperties.BucketName, ""),
        options.getOrElse(StorageProperties.Address, "")
      )
      (
        StorageProperties.from(
          options + (StorageProperties.BucketName -> located.bucket)
        ),
        located.key
      )
    }
  }

  def storageScheme(config: BackfillConfig): String =
    config.targetStorageOptions
      .getOrElse(StorageProperties.CloudProvider, config.s3CloudProvider)
      .trim match {
      case "aliyun" => "oss"
      case "gcp"    => "gs"
      case _        => "s3a"
    }

  /** Read snapshot JSON through the same native filesystem boundary as
    * manifests.
    */
  private[backfill] def readSnapshotJson(
      snapshotPath: String,
      config: BackfillConfig
  ): Either[BackfillError, String] = {
    try {
      val (properties, key) = location(snapshotPath, config)
      val store = NativeObjectStore.Factory(properties).open()
      Right(WriterResources.closing(store.close()) {
        new String(store.readAll(key), StandardCharsets.UTF_8)
      })
    } catch {
      case e: Exception =>
        Left(
          DataReadError(
            snapshotPath,
            s"Failed to read snapshot file: ${e.getMessage}",
            Some(e)
          )
        )
    }
  }
}
