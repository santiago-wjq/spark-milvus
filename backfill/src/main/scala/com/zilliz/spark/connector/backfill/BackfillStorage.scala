package com.zilliz.spark.connector.backfill

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.{
  NativeWritableObjectStore,
  WritableObjectStore
}
import com.zilliz.milvus.storage.io.NativeObjectStore
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.write.WriterResources
import com.zilliz.spark.connector.input.InputStorageBinding
import com.zilliz.spark.connector.options.HadoopStorageKeys

/** Native snapshot/result access and independently bound input Hadoop options.
  */
object BackfillStorage {

  private val logger = LoggerFactory.getLogger(getClass)

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
      val store = new NativeWritableObjectStore(
        NativeObjectStore.Factory(properties).open()
      )
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

  /** Local CLI paths are independent of the target segment filesystem root. */
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

  private[backfill] def inputBinding(
      spark: SparkSession,
      path: String,
      config: BackfillConfig
  ): InputStorageBinding = {
    val conf = new Configuration(spark.sessionState.newHadoopConf())
    val hasSourceOverrides =
      config.sourceS3Endpoint.isDefined || config.sourceS3AccessKey.isDefined ||
        config.sourceS3SecretKey.isDefined || config.sourceS3UseIam.isDefined ||
        config.sourceS3UseSSL.isDefined || config.sourceS3Region.isDefined
    if (config.storageProperties.isEmpty || hasSourceOverrides) {
      val normalized = normalizeObjectStorageScheme(path, config)
      if (config.inputFormat.trim == "iceberg") {
        clearOverriddenCatalogBucketSettings(conf, config)
        if (config.s3CloudProvider.trim == "aliyun")
          configureHadoopOssForPath(conf, "oss://input/", config, true)
        else configureHadoopS3ForPath(conf, "", config, true)
      } else configureHadoopStorageForPath(conf, normalized, config, true)
    }
    InputStorageBinding.capture(conf)
  }

  /** A catalog resolves its bucket lazily. Only explicitly selected source
    * settings may displace inherited per-bucket defaults on this private copy.
    * Managed IAM/role providers remain selected unless static credentials were
    * explicitly supplied for the input.
    */
  private def clearOverriddenCatalogBucketSettings(
      conf: Configuration,
      config: BackfillConfig
  ): Unit = {
    val legacy = config.storageProperties.isEmpty
    val useIam = config.sourceS3UseIam.getOrElse(config.s3UseIam)
    val staticIdentity = !useIam && config.sourceS3AccessKey
      .getOrElse(config.s3AccessKey)
      .nonEmpty &&
      config.sourceS3SecretKey.getOrElse(config.s3SecretKey).nonEmpty
    val suffixes = Set.newBuilder[String]
    if (legacy || config.sourceS3Endpoint.isDefined)
      suffixes ++= Set(
        "endpoint",
        "path.style.access",
        "bucket.probe",
        "multiobjectdelete.enable"
      )
    if (legacy || config.sourceS3Region.isDefined)
      suffixes ++= Set("region", "endpoint.region")
    if (legacy || config.sourceS3UseSSL.isDefined)
      suffixes += "connection.ssl.enabled"
    if (staticIdentity)
      suffixes ++= Set(
        "access.key",
        "secret.key",
        "session.token",
        "aws.credentials.provider"
      )
    val selected = suffixes.result()
    val keys = conf
      .iterator()
      .asScala
      .map(_.getKey)
      .filter(_.startsWith("fs.s3a.bucket."))
      .toVector
    keys
      .filter(key => selected.exists(suffix => key.endsWith("." + suffix)))
      .foreach(conf.unset)
  }

  /** Normalize an `s3://` URL to `s3a://`. The legacy `s3://` scheme maps to
    * Hadoop's S3FileSystem (or, on 3.4.x, an alias that does NOT honor
    * `fs.s3a.bucket.<b>.*` per-bucket config), so per-bucket credentials we
    * write would be silently ignored. All read/write code paths in this object
    * route through this helper before touching Hadoop FS APIs.
    */
  private[backfill] def normalizeS3Scheme(path: String): String = {
    if (path == null) null
    else if (path.startsWith("s3://")) "s3a://" + path.stripPrefix("s3://")
    else path
  }

  private[backfill] def normalizeObjectStorageScheme(
      path: String,
      config: BackfillConfig
  ): String = {
    if (path == null) null
    else if (config.s3CloudProvider.trim.equalsIgnoreCase("aliyun")) {
      if (path.startsWith("s3://")) "oss://" + path.stripPrefix("s3://")
      else if (path.startsWith("s3a://")) "oss://" + path.stripPrefix("s3a://")
      else path
    } else normalizeS3Scheme(path)
  }

  def storageScheme(config: BackfillConfig): String =
    config.targetStorageOptions
      .getOrElse(StorageProperties.CloudProvider, config.s3CloudProvider)
      .trim match {
      case "aliyun" => "oss"
      case "gcp"    => "gs"
      case _        => "s3a"
    }

  private[backfill] def storagePath(
      config: BackfillConfig,
      suffix: String
  ): String = {
    storageScheme(config) + "://" + config.s3BucketName + "/" + suffix
      .stripPrefix("/")
  }

  private[backfill] def configureHadoopStorageForPath(
      hadoopConf: Configuration,
      path: String,
      config: BackfillConfig,
      isSource: Boolean
  ): Unit = {
    val normalized = path
    if (normalized != null && normalized.startsWith("oss://")) {
      configureHadoopOssForPath(hadoopConf, normalized, config, isSource)
    } else {
      configureHadoopS3ForPath(hadoopConf, normalized, config, isSource)
    }
  }

  private[backfill] def configureHadoopOssForPath(
      hadoopConf: Configuration,
      path: String,
      config: BackfillConfig,
      isSource: Boolean
  ): Unit = {
    if (path == null || !path.startsWith("oss://")) return
    val withoutScheme = path.stripPrefix("oss://")
    val slash = withoutScheme.indexOf('/')
    val bucket =
      if (slash < 0) withoutScheme else withoutScheme.substring(0, slash)
    if (bucket.isEmpty) return

    val endpoint =
      if (isSource) config.sourceS3Endpoint.getOrElse(config.s3Endpoint)
      else config.s3Endpoint
    val accessKey =
      if (isSource) config.sourceS3AccessKey.getOrElse(config.s3AccessKey)
      else config.s3AccessKey
    val secretKey =
      if (isSource) config.sourceS3SecretKey.getOrElse(config.s3SecretKey)
      else config.s3SecretKey
    val useSSL =
      if (isSource) config.sourceS3UseSSL.getOrElse(config.s3UseSSL)
      else config.s3UseSSL
    val useIam =
      if (isSource) config.sourceS3UseIam.getOrElse(config.s3UseIam)
      else config.s3UseIam
    hadoopConf.set(
      "fs.oss.impl",
      "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem"
    )
    if (endpoint != null && endpoint.nonEmpty)
      hadoopConf.set("fs.oss.endpoint", endpoint)
    if (useSSL) hadoopConf.set("fs.oss.connection.secure.enabled", "true")
    else hadoopConf.unset("fs.oss.connection.secure.enabled")
    if (useIam) {
      val provider = Option(
        hadoopConf.getTrimmed(BackfillConfig.HadoopOssCredentialsProvider)
      ).filter(_.nonEmpty)
      if (provider.isEmpty) {
        throw new IllegalArgumentException(
          s"${BackfillConfig.HadoopOssCredentialsProvider} must be configured for OSS IAM access"
        )
      }
      hadoopConf.unset("fs.oss.accessKeyId")
      hadoopConf.unset("fs.oss.accessKeySecret")
    } else {
      hadoopConf.set("fs.oss.accessKeyId", accessKey)
      hadoopConf.set("fs.oss.accessKeySecret", secretKey)
      hadoopConf.unset(BackfillConfig.HadoopOssSecurityToken)
      // hadoop-aliyun constructs AliyunCredentialsProvider itself from the
      // accessKeyId/accessKeySecret properties. The class has no (URI,
      // Configuration) or no-arg constructor, so configuring it explicitly
      // causes filesystem initialization to fail.
      hadoopConf.unset(BackfillConfig.HadoopOssCredentialsProvider)
    }
  }

  /** Configure Hadoop S3A credentials for the bucket referenced by `path`.
    *
    * Uses per-bucket keys (`fs.s3a.bucket.<bucket>.*`) so that the backfill
    * *source* bucket and the Milvus storage bucket (snapshot / segments) can
    * each use their own endpoint and credentials within the same Spark session.
    * No-op for non-S3 paths.
    *
    * @param isSource
    *   true when configuring the backfill input bucket — in that case we
    *   consult the `sourceS3*` overrides and fall back to the main credentials
    *   when a particular field is unset.
    */
  private[backfill] def configureHadoopS3ForPath(
      hadoopConf: Configuration,
      path: String,
      config: BackfillConfig,
      isSource: Boolean
  ): Unit = {
    if (path == null) return
    if (
      path.nonEmpty && !(path.startsWith("s3://") || path.startsWith("s3a://"))
    ) return

    // Extract bucket name from s3(a)://bucket/key
    val withoutScheme = path.stripPrefix("s3a://").stripPrefix("s3://")
    val bucket = {
      val slash = withoutScheme.indexOf('/')
      if (slash < 0) withoutScheme else withoutScheme.substring(0, slash)
    }
    // A catalog input binds global defaults on this private configuration.

    // Resolve the effective credentials for this bucket. For the source bucket
    // any unset override falls back to the main credentials, preserving the
    // existing single-bucket behavior.
    val endpoint =
      if (isSource) config.sourceS3Endpoint.getOrElse(config.s3Endpoint)
      else config.s3Endpoint
    val accessKey =
      if (isSource) config.sourceS3AccessKey.getOrElse(config.s3AccessKey)
      else config.s3AccessKey
    val secretKey =
      if (isSource) config.sourceS3SecretKey.getOrElse(config.s3SecretKey)
      else config.s3SecretKey
    val useSSL =
      if (isSource) config.sourceS3UseSSL.getOrElse(config.s3UseSSL)
      else config.s3UseSSL
    val useIam =
      if (isSource) config.sourceS3UseIam.getOrElse(config.s3UseIam)
      else config.s3UseIam
    val region =
      if (isSource) config.sourceS3Region.getOrElse(config.s3Region)
      else config.s3Region

    val prefix = if (bucket.isEmpty) "fs.s3a" else s"fs.s3a.bucket.$bucket"

    val translated =
      if (useIam)
        HadoopStorageKeys.toFsProperties(
          BackfillConfig.legacyHadoopStorageConf(hadoopConf, bucket, "aws"),
          bucket,
          declared = Map(
            StorageProperties.CloudProvider -> (if (
                                                  config.s3CloudProvider.trim == "tencent"
                                                ) "tencent"
                                                else "aws")
          )
        )
      else Map.empty[String, String]
    val tencentRole =
      if (translated.get(StorageProperties.CloudProvider).contains("tencent"))
        translated.get(StorageProperties.RoleArn)
      else None
    // Source overrides may point at another cloud. Its provider/endpoint,
    // rather than the target bucket's cloud alone, determines COS behavior.
    val endpointHost = Option(endpoint)
      .getOrElse("")
      .stripPrefix("https://")
      .stripPrefix("http://")
    val isTencent = tencentRole.nonEmpty ||
      (config.s3CloudProvider.trim == "tencent" && (!isSource || config.sourceS3Endpoint.isEmpty)) ||
      (endpointHost.startsWith("cos.") && endpointHost.endsWith(
        ".myqcloud.com"
      ))
    require(
      !useIam || !isTencent || tencentRole.nonEmpty,
      "Tencent IAM storage requires TencentS3RoleCredentialsProvider in Hadoop configuration"
    )

    // Endpoint + path style + SSL are safe to set in both IAM and static modes
    if (endpoint != null && endpoint.nonEmpty) {
      hadoopConf.set(s"$prefix.endpoint", endpoint)
    }
    if (region != null && region.nonEmpty) {
      // Newer hadoop-aws (3.3.x+) reads endpoint.region; set both keys for
      // compatibility with older versions that only honor `region`.
      hadoopConf.set(s"$prefix.endpoint.region", region)
      hadoopConf.set(s"$prefix.region", region)
    }
    hadoopConf.set(s"$prefix.path.style.access", (!isTencent).toString)
    if (isTencent) {
      hadoopConf.set(s"$prefix.bucket.probe", "0")
      hadoopConf.set(s"$prefix.multiobjectdelete.enable", "false")
    }
    hadoopConf.set(
      s"$prefix.connection.ssl.enabled",
      if (useSSL) "true" else "false"
    )

    if (useIam) {
      val bucketProviderKey = s"$prefix.aws.credentials.provider"
      val assumedRole = translated.get(StorageProperties.RoleArn)

      // Build an explicit IRSA/EKS-friendly provider chain instead of the
      // v1 DefaultAWSCredentialsProviderChain, which has historically been
      // unreliable on EKS pods (it does not always pick up the projected
      // service-account web-identity token before falling back to the
      // EC2 instance profile of the node — leaking the node's role).
      //
      // Order matters:
      //   1. WebIdentityTokenCredentialsProvider — IRSA / GKE Workload Identity
      //   2. EnvironmentVariableCredentialsProvider — local dev / CI overrides
      //   3. IAMInstanceCredentialsProvider — EC2 / EKS node role fallback
      // A managed runtime may already have selected an AssumeRole provider
      // globally for its data role or per bucket for an external volume. Do
      // not replace that security boundary with the pod's ambient identity.
      if (
        assumedRole.isEmpty && tencentRole.isEmpty &&
        Option(hadoopConf.getTrimmed(bucketProviderKey)).forall(_.isEmpty) &&
        Option(
          hadoopConf.getTrimmed(BackfillConfig.HadoopS3CredentialsProvider)
        ).forall(_.isEmpty)
      ) {
        hadoopConf.set(
          bucketProviderKey,
          Seq(
            "software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider",
            "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider",
            "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider"
          ).mkString(",")
        )
      }
    } else if (accessKey.nonEmpty && secretKey.nonEmpty) {
      hadoopConf.set(s"$prefix.access.key", accessKey)
      hadoopConf.set(s"$prefix.secret.key", secretKey)
      hadoopConf.unset(s"$prefix.session.token")
      // Force the simple static-credentials provider for this bucket so it
      // doesn't get shadowed by a globally configured provider chain.
      hadoopConf.set(
        s"$prefix.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
      )
    }

    logger.info(
      s"Configured Hadoop S3A for bucket '$bucket' " +
        s"(endpoint=$endpoint, useIam=$useIam, isSource=$isSource)"
    )
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
