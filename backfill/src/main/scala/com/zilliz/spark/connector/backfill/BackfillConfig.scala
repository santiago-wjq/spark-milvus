package com.zilliz.spark.connector.backfill

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.spark.connector.options.{HadoopStorageKeys, StorageOptions}
import com.zilliz.spark.connector.options.MilvusOption

/** Configuration for backfill operation
  *
  * @param databaseName
  *   Milvus database name
  * @param collectionName
  *   Milvus collection name to backfill
  * @param partitionName
  *   Optional specific partition name to backfill
  * @param s3Endpoint
  *   S3/Minio endpoint (e.g., "localhost:9000")
  * @param s3BucketName
  *   S3 bucket name
  * @param s3AccessKey
  *   S3 access key ID
  * @param s3SecretKey
  *   S3 secret access key
  * @param s3UseSSL
  *   Whether to use SSL for S3 connections
  * @param s3RootPath
  *   Root path in S3 bucket
  * @param s3Region
  *   S3 region
  * @param s3CloudProvider
  *   Native storage provider for the Milvus storage bucket
  * @param batchSize
  *   Batch size for writing data
  * @param customOutputPath
  *   Optional custom output path override
  * @param joinKey
  *   Row identity used to match snapshot rows with backfill input rows
  */
case class BackfillConfig(
    databaseName: String = "default",
    collectionName: String = "",
    partitionName: Option[String] = None,

    // S3 storage configuration
    s3Endpoint: String = "",
    s3BucketName: String = "",
    s3AccessKey: String = "",
    s3SecretKey: String = "",
    s3UseSSL: Boolean = false,
    s3RootPath: String = "files",
    s3Region: String = "us-east-1",
    s3CloudProvider: String = BackfillConfig.DefaultCloudProvider,
    // When true, both Milvus FFI and Spark Hadoop S3A use the default credentials
    // chain (env / web identity / instance profile) instead of static AK/SK.
    s3UseIam: Boolean = false,

    // Optional separate credentials for the backfill *input* parquet bucket.
    // When set, these are used (via Hadoop per-bucket S3A config) to read the
    // backfill data parquet, while the main s3* fields above continue to be
    // used for snapshot reads and segment writes (Milvus storage bucket).
    // Leave as None to reuse the main credentials for both buckets.
    sourceS3Endpoint: Option[String] = None,
    sourceS3AccessKey: Option[String] = None,
    sourceS3SecretKey: Option[String] = None,
    sourceS3UseSSL: Option[Boolean] = None,
    sourceS3UseIam: Option[Boolean] = None,
    sourceS3Region: Option[String] = None,

    // Input data source format. "parquet" and "iceberg" have readers; "lance"
    // is recognized as a valid format name but has no reader yet, so
    // validate() rejects it until a reader lands.
    inputFormat: String = BackfillConfig.DefaultInputFormat,

    // Iceberg input read options. snapshot-id time-travels the input table to
    // a specific Iceberg snapshot so a long-running backfill reads a stable
    // view; ignored when inputFormat != "iceberg" (validate() rejects that
    // combination).
    icebergSnapshotId: Option[String] = None,

    // Writer configuration
    batchSize: Int = 1024,
    customOutputPath: Option[String] = None,

    // Optional mapping: parquet column name -> Milvus field name.
    // When set, the backfill parquet is reprojected through this map before
    // join/write: parquet columns not listed as keys are dropped, and each
    // listed column is renamed to its target. The value set must contain the
    // configured join field so the identity column can be consumed after
    // renaming. With the default primary-key join and no mapping, legacy
    // behavior applies: the parquet must contain a literal "pk" column plus
    // one or more field columns. With a physical join field and no mapping,
    // the parquet must contain that exact field name.
    columnMapping: Option[Map[String, String]] = None,

    // Merge mode for backfill values:
    //   "coalesce" (default) — read the target field's current value from
    //     the snapshot and pick coalesce(snapshot, parquet) per row per field (snapshot
    //     wins when non-null; parquet fills nulls). Unmatched snapshot rows
    //     keep their original target values.
    //   "overwrite" — parquet overrides the target field for rows whose join
    //     key matches (null included). Unmatched snapshot rows keep their
    //     original target values.
    //   "replace" — parquet is the absolute source of truth: every snapshot
    //     row's target field becomes the parquet value (null if the join key
    //     has no parquet match). Destructive on unmatched rows.
    mode: String = BackfillConfig.ModeCoalesce,

    // Optional main-storage AssumeRole settings. BackfillApp derives these
    // from an existing Hadoop S3A configuration when the runtime platform has
    // already selected a data role for the job.
    s3RoleArn: Option[String] = None,
    s3RoleSessionName: Option[String] = None,
    s3ExternalId: Option[String] = None,

    // Row identity used to join snapshot rows with backfill data. The primary
    // key remains the default for backward compatibility.
    joinKey: BackfillJoinKey = BackfillJoinKey.PrimaryKey,
    // Explicit native storage binding takes precedence over legacy S3 fields.
    storageProperties: Map[String, String] = Map.empty,
    // Explicit deployment capability, not inferred from a service version.
    // Delta output requires every receiving DataCoord to understand manifest ops.
    receiverCapability: String = BackfillConfig.LegacyReceiverCapability
) {

  def emitsManifestDelta: Boolean =
    receiverCapability == BackfillConfig.ManifestDeltaReceiverCapability

  /** Whether the merge path needs to read each target field from the snapshot
    * too. Coalesce and overwrite both compare snapshot and parquet values per
    * row at join time; replace takes parquet verbatim and therefore only needs
    * the join key + segment tracking columns from the snapshot.
    */
  def readsSnapshotFields: Boolean =
    mode != BackfillConfig.ModeReplace

  /** Path inputs require both target/writer and independent input settings. */
  def validate(): Either[String, Unit] =
    validateTarget().flatMap(_ => validateInput())

  /** A borrowed DataFrame already owns its source plan and input identity. */
  def validateTarget(): Either[String, Unit] = {
    val normalizedCloudProvider = s3CloudProvider.trim
    val normalizedRoleArn = s3RoleArn.map(_.trim).filter(_.nonEmpty)
    val hasRoleDetails =
      s3RoleSessionName.exists(_.trim.nonEmpty) || s3ExternalId.exists(
        _.trim.nonEmpty
      )

    if (storageProperties.nonEmpty) {
      try StorageProperties.from(storageProperties)
      catch { case e: IllegalArgumentException => return Left(e.getMessage) }
    }
    val legacyStorage = storageProperties.isEmpty
    if (legacyStorage && s3Endpoint.isEmpty) {
      Left("s3Endpoint cannot be empty")
    } else if (legacyStorage && s3BucketName.isEmpty) {
      Left("s3BucketName cannot be empty")
    } else if (
      legacyStorage && !BackfillConfig.AllowedCloudProviders.contains(
        normalizedCloudProvider
      )
    ) {
      Left(
        s"s3CloudProvider must be one of ${BackfillConfig.AllowedCloudProviders
            .mkString("[", ", ", "]")} (got '$s3CloudProvider')"
      )
    } else if (
      !BackfillConfig.ReceiverCapabilities.contains(receiverCapability)
    ) {
      Left(
        s"receiverCapability must be '${BackfillConfig.LegacyReceiverCapability}' or " +
          s"'${BackfillConfig.ManifestDeltaReceiverCapability}' (got '$receiverCapability')"
      )
    } else if (batchSize <= 0) {
      Left("batchSize must be positive")
    } else if (
      mode != BackfillConfig.ModeReplace &&
      mode != BackfillConfig.ModeCoalesce &&
      mode != BackfillConfig.ModeOverwrite
    ) {
      Left(
        s"mode must be one of '${BackfillConfig.ModeReplace}', " +
          s"'${BackfillConfig.ModeCoalesce}', " +
          s"'${BackfillConfig.ModeOverwrite}' (got '$mode')"
      )
    } else if (
      joinKey match {
        case BackfillJoinKey.PhysicalField(name) =>
          Option(name).forall(_.trim.isEmpty)
        case _ => false
      }
    ) {
      Left("physical join-key field name cannot be blank")
    } else if (
      legacyStorage && !s3UseIam && (s3AccessKey.isEmpty || s3SecretKey.isEmpty)
    ) {
      // Hard invariant: must use IAM or supply both AK and SK. Half-set
      // static credentials are never valid — they would silently fall back
      // to the default provider chain and mask config mistakes.
      Left(
        "s3AccessKey and s3SecretKey must both be set unless s3UseIam=true"
      )
    } else if (legacyStorage && normalizedRoleArn.nonEmpty && !s3UseIam) {
      Left("s3RoleArn requires s3UseIam=true")
    } else if (
      legacyStorage && normalizedRoleArn.nonEmpty &&
      !BackfillConfig.NativeAssumeRoleCloudProviders.contains(
        normalizedCloudProvider
      )
    ) {
      Left(
        s"s3RoleArn is supported only when s3CloudProvider is one of " +
          s"${BackfillConfig.NativeAssumeRoleCloudProviders.mkString("[", ", ", "]")} " +
          s"(got '$s3CloudProvider')"
      )
    } else if (legacyStorage && normalizedRoleArn.isEmpty && hasRoleDetails) {
      Left("s3RoleSessionName and s3ExternalId require s3RoleArn")
    } else Right(())
  }

  private def validateInput(): Either[String, Unit] = {
    if (!BackfillConfig.AllowedInputFormats.contains(inputFormat.trim)) {
      Left(
        s"inputFormat must be one of ${BackfillConfig.AllowedInputFormats
            .mkString("[", ", ", "]")} (got '$inputFormat')"
      )
    } else if (
      !BackfillConfig.ImplementedInputFormats.contains(inputFormat.trim)
    ) {
      Left(
        s"inputFormat '$inputFormat' is recognized but not yet implemented; " +
          s"only ${BackfillConfig.ImplementedInputFormats.mkString("[", ", ", "]")} " +
          "have a reader"
      )
    } else if (
      inputFormat.trim != "iceberg" && icebergSnapshotId.exists(_.trim.nonEmpty)
    ) {
      Left("icebergSnapshotId requires inputFormat='iceberg'")
    } else if (
      inputFormat.trim == "iceberg" && icebergSnapshotId.exists { id =>
        id.trim.nonEmpty && scala.util.Try(id.trim.toLong).isFailure
      }
    ) {
      Left(
        s"icebergSnapshotId must be an integer snapshot id " +
          s"(got '${icebergSnapshotId.get}')"
      )
    } else {
      // Same invariant for the source (input parquet) bucket. Any field
      // left as None falls back to the main credentials, which we already
      // validated above, so we only fail when an asymmetric override would
      // produce half-set static credentials.
      val srcUseIam = sourceS3UseIam.getOrElse(s3UseIam)
      val srcAk = sourceS3AccessKey.getOrElse(s3AccessKey)
      val srcSk = sourceS3SecretKey.getOrElse(s3SecretKey)
      val sourceOverride =
        sourceS3AccessKey.isDefined || sourceS3SecretKey.isDefined || sourceS3UseIam.isDefined
      if (
        (storageProperties.isEmpty || sourceOverride) && !srcUseIam && (srcAk.isEmpty || srcSk.isEmpty)
      ) {
        Left(
          "source bucket: sourceS3AccessKey and sourceS3SecretKey must both " +
            "be set unless sourceS3UseIam=true (or fall back to main)"
        )
      } else {
        Right(())
      }
    }
  }

  /** Get base Milvus read options as a Map for DataSource. The resolved join
    * field IDs are added by `MilvusBackfill` to minimize data transfer.
    */
  def getMilvusReadOptions: Map[String, String] = {
    targetStorageOptions ++ Map(
      MilvusOption.MilvusExtraColumns -> Seq(
        MilvusOption.MilvusExtraColumnSegmentID,
        MilvusOption.MilvusExtraColumnRowOffset
      ).mkString(",")
    )
  }

  def targetStorageOptions: Map[String, String] =
    if (storageProperties.nonEmpty) storageProperties
    else
      withS3Authentication(
        Map(
          StorageProperties.StorageType -> "remote",
          StorageProperties.Address -> s3Endpoint,
          StorageProperties.BucketName -> s3BucketName,
          StorageProperties.RootPath -> s3RootPath,
          StorageProperties.UseSSL -> s3UseSSL.toString,
          StorageProperties.UseIam -> s3UseIam.toString,
          StorageProperties.Region -> s3Region,
          StorageProperties.CloudProvider -> s3CloudProvider.trim
        )
      )

  /** Resolve once on the driver; tasks receive only this immutable map. */
  private[backfill] def resolveStorage(
      conf: Configuration
  ): Map[String, String] = {
    val options = targetStorageOptions
    val bucket =
      if (options.get(StorageProperties.StorageType).contains("local")) ""
      else options.getOrElse(StorageProperties.BucketName, "")
    val cloud = options.getOrElse(StorageProperties.CloudProvider, "")
    val storageConf =
      if (
        storageProperties.isEmpty && s3UseIam && !s3RoleArn.exists(
          _.trim.nonEmpty
        )
      )
        BackfillConfig.legacyHadoopStorageConf(conf, bucket, cloud)
      else BackfillConfig.hadoopConfForCloud(conf, cloud)
    StorageOptions.storagePropertiesFor(
      storageConf,
      bucket,
      options
    )
  }

  /** Get S3 write options as a Map for MilvusV3WriterFactory
    */
  def getS3WriteOptions(
      collectionId: Long,
      partitionId: Long,
      segmentId: Long
  ): Map[String, String] = {
    val outputPath = customOutputPath.getOrElse(
      s"$s3RootPath/insert_log/$collectionId/$partitionId/$segmentId"
    )
    getS3WriteOptionsForBasePath(outputPath)
  }

  /** Get S3 write options using a specific segment base path (e.g., from
    * manifest)
    */
  def getS3WriteOptionsForBasePath(
      segmentBasePath: String
  ): Map[String, String] =
    targetStorageOptions ++ Map(
      "milvus.writer.customPath" -> segmentBasePath,
      "milvus.writer.commitType" -> "addfield",
      "milvus.insertMaxBatchSize" -> batchSize.toString
    )

  private[backfill] def withHadoopStorageAssumeRole(
      hadoopConf: Configuration,
      defaultSessionName: String
  ): BackfillConfig = {
    if (
      storageProperties.nonEmpty || !s3UseIam || s3RoleArn.exists(
        _.trim.nonEmpty
      ) ||
      !BackfillConfig.NativeAssumeRoleCloudProviders(s3CloudProvider.trim)
    ) this
    else {
      val translated = HadoopStorageKeys.toFsProperties(
        BackfillConfig.legacyHadoopStorageConf(
          hadoopConf,
          s3BucketName,
          s3CloudProvider.trim,
          defaultSessionName
        ),
        s3BucketName,
        declared = Map(StorageProperties.CloudProvider -> s3CloudProvider.trim)
      )
      require(
        s3CloudProvider.trim == "tencent" || !translated
          .get(StorageProperties.CloudProvider)
          .contains("tencent"),
        "Tencent platform roles require s3CloudProvider=tencent for native storage"
      )
      require(
        s3CloudProvider.trim != "tencent" || (translated
          .get(StorageProperties.CloudProvider)
          .contains("tencent") && translated.contains(
          StorageProperties.RoleArn
        )),
        "Tencent IAM storage requires TencentS3RoleCredentialsProvider in Hadoop configuration"
      )
      translated
        .get(StorageProperties.RoleArn)
        .map { arn =>
          copy(
            s3RoleArn = Some(arn),
            s3RoleSessionName = Some(
              BackfillConfig.normalizeRoleSessionName(
                translated
                  .getOrElse(StorageProperties.SessionName, defaultSessionName)
              )
            ),
            s3ExternalId = translated.get(StorageProperties.ExternalId)
          )
        }
        .getOrElse(this)
    }
  }

  private def withS3Authentication(
      options: Map[String, String]
  ): Map[String, String] = {
    val credentialOptions = if (s3UseIam) {
      options
    } else {
      options ++ Map(
        StorageProperties.AccessKeyId -> s3AccessKey,
        StorageProperties.AccessKeyValue -> s3SecretKey
      )
    }

    Seq(
      StorageProperties.RoleArn -> s3RoleArn,
      StorageProperties.SessionName -> s3RoleSessionName,
      StorageProperties.ExternalId -> s3ExternalId
    ).foldLeft(credentialOptions) { case (result, (key, value)) =>
      value.map(_.trim).filter(_.nonEmpty) match {
        case Some(normalized) => result + (key -> normalized)
        case None             => result
      }
    }
  }
}

object BackfillConfig {

  val LegacyReceiverCapability = "legacy"
  val ManifestDeltaReceiverCapability = "manifest-delta-v1"
  private[backfill] val ReceiverCapabilities =
    Set(LegacyReceiverCapability, ManifestDeltaReceiverCapability)

  val ModeCoalesce = "coalesce"
  val ModeOverwrite = "overwrite"
  val ModeReplace = "replace"

  private[backfill] val DefaultCloudProvider = "aws"
  private[backfill] val AllowedCloudProviders =
    Set("aws", "gcp", "aliyun", "azure", "tencent", "huawei")
  private[backfill] val NativeAssumeRoleCloudProviders =
    Set("aws", "aliyun", "tencent")

  private[backfill] val DefaultInputFormat = "parquet"
  private[backfill] val AllowedInputFormats =
    Set("parquet", "iceberg", "lance")
  // Subset of AllowedInputFormats with a working reader. validate() rejects
  // recognized-but-unimplemented formats here (fail-fast at config time), and
  // readBackfillData guards its per-format dispatch on the same set so the two
  // cannot drift. Each new reader extends this set and adds a dispatch arm.
  private[backfill] val ImplementedInputFormats = Set("parquet", "iceberg")

  private[backfill] val HadoopS3CredentialsProvider =
    "fs.s3a.aws.credentials.provider"
  private[backfill] val HadoopS3AssumedRoleArn =
    "fs.s3a.assumed.role.arn"
  private[backfill] val HadoopS3AssumedRoleSessionName =
    "fs.s3a.assumed.role.session.name"
  private[backfill] val HadoopS3AssumedRoleExternalId =
    "fs.s3a.assumed.role.external.id"
  private[backfill] val HadoopS3AssumedRoleProvider =
    "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"
  private[backfill] val HadoopOssAssumedRoleArn =
    "fs.oss.assumed.role.arn"
  private[backfill] val HadoopOssCredentialsProvider =
    "fs.oss.credentials.provider"
  private[backfill] val HadoopOssSecurityToken = "fs.oss.securityToken"
  private[backfill] val HadoopOssAssumedRoleSessionName =
    "fs.oss.assumed.role.session.name"
  private[backfill] val HadoopOssAssumedRoleExternalId =
    "fs.oss.assumed.role.external.id"
  private[backfill] val HadoopOssAssumedRoleProvider =
    "com.zilliz.cloud.hadoop.AliyunOSSRoleCredentialsProvider"

  /** Retain the application's legacy role contract around the public key
    * translator: S3A must select a role provider, and a selected role must have
    * an ARN. The translator still chooses the identity for a provider chain.
    */
  private[backfill] def legacyHadoopStorageConf(
      conf: Configuration,
      bucket: String,
      cloud: String,
      defaultSessionName: String = "spark-backfill"
  ): Configuration = {
    val copy = hadoopConfForCloud(conf, cloud)
    if (cloud != "aws" && cloud != "tencent" && cloud != "aliyun") return copy
    val prefix = if (cloud == "aliyun") "fs.oss" else "fs.s3a"
    val bucketPrefix = if (bucket.isEmpty) prefix else s"$prefix.bucket.$bucket"
    def lookup(suffix: String): Option[String] =
      Option(copy.getTrimmed(s"$bucketPrefix.$suffix"))
        .filter(_.nonEmpty)
        .orElse(Option(copy.getTrimmed(s"$prefix.$suffix")).filter(_.nonEmpty))
    val providerSuffix =
      if (cloud == "aliyun") "credentials.provider"
      else "aws.credentials.provider"
    val provider = lookup(providerSuffix)
    val roleProvider =
      if (cloud == "aliyun") HadoopOssAssumedRoleProvider
      else HadoopS3AssumedRoleProvider
    val namesRole =
      provider.exists(_.split(',').map(_.trim).contains(roleProvider))

    // A real session-name default makes a selected role visible even when its
    // ARN is missing. Static/environment identities omit all role properties,
    // so their fallback chains are still judged only by the shared translator.
    if (namesRole && lookup("assumed.role.session.name").isEmpty) {
      copy.set(
        s"$bucketPrefix.assumed.role.session.name",
        normalizeRoleSessionName(defaultSessionName)
      )
    }
    val translated = HadoopStorageKeys.toFsProperties(
      copy,
      bucket,
      declared = Map(StorageProperties.CloudProvider -> cloud)
    )
    require(
      !namesRole || !translated.contains(StorageProperties.SessionName) ||
        translated.contains(StorageProperties.RoleArn),
      s"Effective AssumeRole configuration requires $bucketPrefix.assumed.role.arn " +
        s"or $prefix.assumed.role.arn"
    )
    if (cloud == "aws" && provider.isEmpty) {
      Seq("arn", "session.name", "external.id").foreach { suffix =>
        copy.unset(s"$bucketPrefix.assumed.role.$suffix")
        copy.unset(s"$prefix.assumed.role.$suffix")
      }
    }
    copy
  }

  /** The selected backend owns role resolution. A second cloud's session
    * namespace must not replace its role when the shared translator merges
    * keys. All non-filesystem settings, including Hadoop credential-provider
    * paths, stay on this independent copy.
    */
  private[backfill] def hadoopConfForCloud(
      conf: Configuration,
      cloud: String
  ): Configuration = {
    val copy = new Configuration(conf)
    val ignored = cloud match {
      case "aws" | "tencent" => Seq("fs.oss.")
      case "aliyun"          => Seq("fs.s3a.")
      case ""                => Seq.empty
      case _                 => Seq("fs.s3a.", "fs.oss.")
    }
    copy
      .iterator()
      .asScala
      .map(_.getKey)
      .filter(key => ignored.exists(key.startsWith))
      .toVector
      .foreach(copy.unset)
    copy
  }

  private[backfill] def normalizeRoleSessionName(value: String): String = {
    val normalized = Option(value)
      .getOrElse("")
      .replaceAll("[^A-Za-z0-9+=,.@-]", "-")
    val nonEmpty = if (normalized.nonEmpty) normalized else "spark-backfill"
    nonEmpty.take(64)
  }

  /** Create a minimal config for testing
    */
  def forTest(
      collectionName: String,
      s3Endpoint: String = "localhost:9000",
      s3BucketName: String = "a-bucket"
  ): BackfillConfig = {
    BackfillConfig(
      collectionName = collectionName,
      s3Endpoint = s3Endpoint,
      s3BucketName = s3BucketName,
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin"
    )
  }
}
