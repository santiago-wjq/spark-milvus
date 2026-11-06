package com.zilliz.spark.connector.backfill

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Storage path and per-input Hadoop configuration contracts. */
class BackfillStorageTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private var configuration: Configuration = _
  override def beforeEach(): Unit = { configuration = new Configuration(false) }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("BackfillStorageTest")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("configureHadoopS3ForPath is a no-op for non-s3 paths") {
    val cfg = BackfillConfig(
      s3Endpoint = "ep",
      s3BucketName = "b",
      s3AccessKey = "ak",
      s3SecretKey = "sk"
    )
    val key = "fs.s3a.bucket.never.endpoint"
    configuration.unset(key)
    BackfillStorage.configureHadoopS3ForPath(
      configuration,
      "file:///tmp/x",
      cfg,
      isSource = false
    )
    configuration.get(key) shouldBe null
  }

  test("configureHadoopS3ForPath writes per-bucket static credentials") {
    val cfg = BackfillConfig(
      s3Endpoint = "minio:9000",
      s3BucketName = "main-bucket",
      s3AccessKey = "ak1",
      s3SecretKey = "sk1",
      s3UseSSL = false
    )
    BackfillStorage.configureHadoopS3ForPath(
      configuration,
      "s3a://main-bucket/path/to/file",
      cfg,
      isSource = false
    )
    val hc = configuration
    hc.get("fs.s3a.bucket.main-bucket.endpoint") shouldBe "minio:9000"
    hc.get("fs.s3a.bucket.main-bucket.path.style.access") shouldBe "true"
    hc.get(
      "fs.s3a.bucket.main-bucket.connection.ssl.enabled"
    ) shouldBe "false"
    hc.get("fs.s3a.bucket.main-bucket.access.key") shouldBe "ak1"
    hc.get("fs.s3a.bucket.main-bucket.secret.key") shouldBe "sk1"
    hc.get(
      "fs.s3a.bucket.main-bucket.aws.credentials.provider"
    ) shouldBe "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
  }

  test("configureHadoopS3ForPath uses IAM provider when useIam=true") {
    val cfg = BackfillConfig(
      s3Endpoint = "s3.amazonaws.com",
      s3BucketName = "irsa-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )
    BackfillStorage.configureHadoopS3ForPath(
      configuration,
      "s3a://irsa-bucket/key",
      cfg,
      isSource = false
    )
    val hc = configuration
    val provider =
      hc.get("fs.s3a.bucket.irsa-bucket.aws.credentials.provider")
    // The IRSA-friendly chain must include WebIdentityTokenCredentialsProvider
    // (used by both EKS service account tokens and GKE Workload Identity)
    // and IAMInstanceCredentialsProvider for the EC2/EKS node fallback.
    // The legacy v1 DefaultAWSCredentialsProviderChain must NOT be used
    // because it has been unreliable on EKS pods.
    provider should include(
      "software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider"
    )
    provider should include(
      "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider"
    )
    provider should not include "DefaultAWSCredentialsProviderChain"
    // No static keys should be written in IAM mode
    hc.get("fs.s3a.bucket.irsa-bucket.access.key") shouldBe null
    hc.get("fs.s3a.bucket.irsa-bucket.secret.key") shouldBe null
  }

  test("configureHadoopS3ForPath preserves a bucket AssumeRole provider") {
    val cfg = BackfillConfig(
      s3Endpoint = "s3.us-west-2.amazonaws.com",
      s3BucketName = "external-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )
    val hc = configuration
    val prefix = "fs.s3a.bucket.external-bucket"
    val providerKey = s"$prefix.aws.credentials.provider"
    val roleArnKey = s"$prefix.assumed.role.arn"
    val externalIdKey = s"$prefix.assumed.role.external.id"
    hc.set(providerKey, BackfillConfig.HadoopS3AssumedRoleProvider)
    hc.set(roleArnKey, "arn:aws:iam::123456789012:role/customer-role")
    hc.set(externalIdKey, "external-id")

    try {
      BackfillStorage.configureHadoopS3ForPath(
        configuration,
        "s3a://external-bucket/data/input.parquet",
        cfg,
        isSource = true
      )

      hc.get(providerKey) shouldBe BackfillConfig.HadoopS3AssumedRoleProvider
      hc.get(roleArnKey) shouldBe
        "arn:aws:iam::123456789012:role/customer-role"
      hc.get(externalIdKey) shouldBe "external-id"
    } finally {
      Seq(providerKey, roleArnKey, externalIdKey).foreach(hc.unset)
    }
  }

  test("configureHadoopS3ForPath preserves a global AssumeRole provider") {
    val cfg = BackfillConfig(
      s3Endpoint = "s3.us-west-2.amazonaws.com",
      s3BucketName = "managed-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )
    val hc = configuration
    val bucketProviderKey =
      "fs.s3a.bucket.managed-bucket.aws.credentials.provider"
    hc.unset(bucketProviderKey)
    hc.set(
      BackfillConfig.HadoopS3CredentialsProvider,
      BackfillConfig.HadoopS3AssumedRoleProvider
    )
    hc.set(
      BackfillConfig.HadoopS3AssumedRoleArn,
      "arn:aws:iam::123456789012:role/data-role"
    )

    try {
      BackfillStorage.configureHadoopS3ForPath(
        configuration,
        "s3a://managed-bucket/snapshot.json",
        cfg,
        isSource = false
      )

      hc.get(bucketProviderKey) shouldBe null
      hc.get(BackfillConfig.HadoopS3CredentialsProvider) shouldBe
        BackfillConfig.HadoopS3AssumedRoleProvider
    } finally {
      hc.unset(bucketProviderKey)
      hc.unset(BackfillConfig.HadoopS3CredentialsProvider)
      hc.unset(BackfillConfig.HadoopS3AssumedRoleArn)
    }
  }

  test("configureHadoopS3ForPath rejects incomplete AssumeRole config") {
    val cfg = BackfillConfig(
      s3Endpoint = "s3.us-west-2.amazonaws.com",
      s3BucketName = "incomplete-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )
    val hc = configuration
    val providerKey =
      "fs.s3a.bucket.incomplete-bucket.aws.credentials.provider"
    hc.set(providerKey, BackfillConfig.HadoopS3AssumedRoleProvider)

    try {
      val error = intercept[IllegalArgumentException] {
        BackfillStorage.configureHadoopS3ForPath(
          configuration,
          "s3a://incomplete-bucket/data",
          cfg,
          isSource = true
        )
      }
      error.getMessage should include(
        "fs.s3a.bucket.incomplete-bucket.assumed.role.arn"
      )
    } finally {
      hc.unset(providerKey)
    }
  }

  test(
    "configureHadoopS3ForPath accepts global provider with bucket role ARN"
  ) {
    val cfg = BackfillConfig(
      s3Endpoint = "s3.us-west-2.amazonaws.com",
      s3BucketName = "mixed-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )
    val hc = configuration
    val prefix = "fs.s3a.bucket.mixed-bucket"
    val bucketProviderKey = s"$prefix.aws.credentials.provider"
    val bucketRoleArnKey = s"$prefix.assumed.role.arn"
    hc.unset(bucketProviderKey)
    hc.set(
      BackfillConfig.HadoopS3CredentialsProvider,
      BackfillConfig.HadoopS3AssumedRoleProvider
    )
    hc.set(
      bucketRoleArnKey,
      "arn:aws:iam::123456789012:role/bucket-role"
    )

    try {
      BackfillStorage.configureHadoopS3ForPath(
        configuration,
        "s3a://mixed-bucket/data",
        cfg,
        isSource = false
      )

      hc.get(bucketProviderKey) shouldBe null
      hc.get(bucketRoleArnKey) shouldBe
        "arn:aws:iam::123456789012:role/bucket-role"
    } finally {
      hc.unset(bucketProviderKey)
      hc.unset(bucketRoleArnKey)
      hc.unset(BackfillConfig.HadoopS3CredentialsProvider)
    }
  }

  test(
    "configureHadoopS3ForPath accepts bucket provider with global role ARN"
  ) {
    val cfg = BackfillConfig(
      s3Endpoint = "s3.us-west-2.amazonaws.com",
      s3BucketName = "mixed-bucket-2",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )
    val hc = configuration
    val prefix = "fs.s3a.bucket.mixed-bucket-2"
    val bucketProviderKey = s"$prefix.aws.credentials.provider"
    hc.set(bucketProviderKey, BackfillConfig.HadoopS3AssumedRoleProvider)
    hc.set(
      BackfillConfig.HadoopS3AssumedRoleArn,
      "arn:aws:iam::123456789012:role/global-role"
    )

    try {
      BackfillStorage.configureHadoopS3ForPath(
        configuration,
        "s3a://mixed-bucket-2/data",
        cfg,
        isSource = false
      )

      hc.get(bucketProviderKey) shouldBe
        BackfillConfig.HadoopS3AssumedRoleProvider
    } finally {
      hc.unset(bucketProviderKey)
      hc.unset(BackfillConfig.HadoopS3AssumedRoleArn)
    }
  }

  test("explicit static credentials replace a bucket AssumeRole provider") {
    val cfg = BackfillConfig(
      s3Endpoint = "s3.us-west-2.amazonaws.com",
      s3BucketName = "static-bucket",
      s3AccessKey = "explicit-ak",
      s3SecretKey = "explicit-sk"
    )
    val hc = configuration
    val prefix = "fs.s3a.bucket.static-bucket"
    val providerKey = s"$prefix.aws.credentials.provider"
    hc.set(providerKey, BackfillConfig.HadoopS3AssumedRoleProvider)

    try {
      BackfillStorage.configureHadoopS3ForPath(
        configuration,
        "s3a://static-bucket/data",
        cfg,
        isSource = false
      )

      hc.get(providerKey) shouldBe
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
      hc.get(s"$prefix.access.key") shouldBe "explicit-ak"
      hc.get(s"$prefix.secret.key") shouldBe "explicit-sk"
    } finally {
      Seq(providerKey, s"$prefix.access.key", s"$prefix.secret.key").foreach(
        hc.unset
      )
    }
  }

  // ============ normalizeS3Scheme ============

  test("normalizeS3Scheme rewrites s3:// to s3a://") {
    BackfillStorage.normalizeS3Scheme(
      "s3://bucket/key.parquet"
    ) shouldBe "s3a://bucket/key.parquet"
  }

  test("normalizeS3Scheme leaves s3a:// and other schemes alone") {
    BackfillStorage.normalizeS3Scheme(
      "s3a://bucket/key"
    ) shouldBe "s3a://bucket/key"
    BackfillStorage.normalizeS3Scheme(
      "file:///tmp/x"
    ) shouldBe "file:///tmp/x"
    BackfillStorage.normalizeS3Scheme("/local/path") shouldBe "/local/path"
    BackfillStorage.normalizeS3Scheme(null) shouldBe null
  }

  test("normalizeObjectStorageScheme selects OSS aliases for Alibaba") {
    val cfg = BackfillConfig(
      s3Endpoint = "oss-cn-hangzhou-internal.aliyuncs.com",
      s3BucketName = "managed-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3CloudProvider = "aliyun",
      s3UseIam = true
    )
    BackfillStorage.normalizeObjectStorageScheme(
      "s3a://managed-bucket/files/snapshot.json",
      cfg
    ) shouldBe "oss://managed-bucket/files/snapshot.json"
    BackfillStorage.normalizeObjectStorageScheme(
      "s3://managed-bucket/files/snapshot.json",
      cfg
    ) shouldBe "oss://managed-bucket/files/snapshot.json"
    BackfillStorage.normalizeObjectStorageScheme(
      "oss://managed-bucket/files/snapshot.json",
      cfg
    ) shouldBe "oss://managed-bucket/files/snapshot.json"
  }

  test("storagePath selects OSS for Alibaba") {
    val cfg = BackfillConfig(
      s3Endpoint = "oss-cn-hangzhou-internal.aliyuncs.com",
      s3BucketName = "managed-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3CloudProvider = "aliyun",
      s3UseIam = true
    )
    BackfillStorage.storagePath(
      cfg,
      "files"
    ) shouldBe "oss://managed-bucket/files"
  }

  test("configureHadoopOssForPath preserves managed IAM provider") {
    val cfg = BackfillConfig(
      s3Endpoint = "oss-cn-hangzhou-internal.aliyuncs.com",
      s3BucketName = "managed-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3CloudProvider = "aliyun",
      s3UseIam = true
    )
    val hc = configuration
    val provider = BackfillConfig.HadoopOssAssumedRoleProvider
    hc.set(BackfillConfig.HadoopOssCredentialsProvider, provider)
    hc.set(BackfillConfig.HadoopOssSecurityToken, "managed-token")
    try {
      BackfillStorage.configureHadoopOssForPath(
        configuration,
        "oss://managed-bucket/files/snapshot.json",
        cfg,
        isSource = false
      )
      hc.get(
        "fs.oss.impl"
      ) shouldBe "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem"
      hc.get("fs.oss.endpoint") shouldBe "oss-cn-hangzhou-internal.aliyuncs.com"
      hc.get(BackfillConfig.HadoopOssCredentialsProvider) shouldBe provider
      hc.get(BackfillConfig.HadoopOssSecurityToken) shouldBe "managed-token"
      hc.get("fs.oss.accessKeyId") shouldBe null
      hc.get("fs.oss.accessKeySecret") shouldBe null
    } finally {
      Seq(
        BackfillConfig.HadoopOssCredentialsProvider,
        BackfillConfig.HadoopOssSecurityToken,
        "fs.oss.impl",
        "fs.oss.endpoint",
        "fs.oss.connection.secure.enabled",
        "fs.oss.accessKeyId",
        "fs.oss.accessKeySecret"
      ).foreach(hc.unset)
    }
  }

  test(
    "configureHadoopOssForPath uses built-in static credential construction"
  ) {
    val cfg = BackfillConfig(
      s3Endpoint = "oss-cn-hangzhou.aliyuncs.com",
      s3BucketName = "static-bucket",
      s3AccessKey = "static-ak",
      s3SecretKey = "static-sk",
      s3CloudProvider = "aliyun",
      s3UseIam = false
    )
    val hc = configuration
    hc.set(
      BackfillConfig.HadoopOssCredentialsProvider,
      BackfillConfig.HadoopOssAssumedRoleProvider
    )
    hc.set(BackfillConfig.HadoopOssSecurityToken, "stale-target-token")
    try {
      BackfillStorage.configureHadoopOssForPath(
        configuration,
        "oss://static-bucket/files/data.parquet",
        cfg,
        isSource = false
      )
      hc.get("fs.oss.accessKeyId") shouldBe "static-ak"
      hc.get("fs.oss.accessKeySecret") shouldBe "static-sk"
      hc.get(BackfillConfig.HadoopOssCredentialsProvider) shouldBe null
      hc.get(BackfillConfig.HadoopOssSecurityToken) shouldBe null
      hc.get("fs.oss.connection.secure.enabled") shouldBe null
      val uri = new java.net.URI("oss://static-bucket/files/data.parquet")
      val provider = org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils
        .getCredentialsProvider(uri, hc)
      provider should not be null
      provider.getCredentials.getAccessKeyId shouldBe "static-ak"
      provider.getCredentials.getSecretAccessKey shouldBe "static-sk"
    } finally {
      Seq(
        BackfillConfig.HadoopOssCredentialsProvider,
        BackfillConfig.HadoopOssSecurityToken,
        "fs.oss.impl",
        "fs.oss.endpoint",
        "fs.oss.connection.secure.enabled",
        "fs.oss.accessKeyId",
        "fs.oss.accessKeySecret"
      ).foreach(hc.unset)
    }
  }

  test(
    "independent OSS input binding keeps managed session identity unchanged"
  ) {
    val conf = spark.sparkContext.hadoopConfiguration
    val keys = Seq(
      BackfillConfig.HadoopOssCredentialsProvider,
      "fs.oss.endpoint",
      "fs.oss.accessKeyId",
      "fs.oss.accessKeySecret"
    )
    val previous = keys.map(key => key -> Option(conf.get(key))).toMap
    try {
      conf.set(
        BackfillConfig.HadoopOssCredentialsProvider,
        BackfillConfig.HadoopOssAssumedRoleProvider
      )
      conf.set("fs.oss.endpoint", "managed-endpoint")
      val config = BackfillConfig
        .forTest("c")
        .copy(
          s3CloudProvider = "aliyun",
          sourceS3Endpoint = Some("source-endpoint"),
          sourceS3AccessKey = Some("source-ak"),
          sourceS3SecretKey = Some("source-sk"),
          sourceS3UseIam = Some(false)
        )
      val bound = BackfillStorage
        .inputBinding(spark, "oss://same-bucket/input", config)
        .newHadoopConfiguration()
      bound.get("fs.oss.endpoint") shouldBe "source-endpoint"
      bound.get("fs.oss.accessKeyId") shouldBe "source-ak"
      bound.get(BackfillConfig.HadoopOssCredentialsProvider) shouldBe null
      bound.get("fs.oss.impl.disable.cache") shouldBe "true"
      conf.get("fs.oss.endpoint") shouldBe "managed-endpoint"
      conf.get(
        BackfillConfig.HadoopOssCredentialsProvider
      ) shouldBe BackfillConfig.HadoopOssAssumedRoleProvider
    } finally
      previous.foreach { case (key, value) =>
        value match {
          case Some(text) => conf.set(key, text)
          case None       => conf.unset(key)
        }
      }
  }

  test(
    "same-bucket S3 input binding retains its own credentials after session changes"
  ) {
    val config = BackfillConfig
      .forTest("c")
      .copy(
        sourceS3AccessKey = Some("source-ak"),
        sourceS3SecretKey = Some("source-sk"),
        sourceS3Endpoint = Some("source-endpoint")
      )
    val bound =
      BackfillStorage.inputBinding(spark, "s3a://a-bucket/input", config)
    val conf = spark.sparkContext.hadoopConfiguration
    val key = "fs.s3a.bucket.a-bucket.access.key"
    val previous = Option(conf.get(key))
    try {
      conf.set(key, "later-identity")
      val captured = bound.newHadoopConfiguration()
      captured.get(key) shouldBe "source-ak"
      captured.get("fs.s3a.bucket.a-bucket.endpoint") shouldBe "source-endpoint"
      captured.get("fs.s3a.impl.disable.cache") shouldBe "true"
      bound.toString should not include "source-ak"
      conf.get(key) shouldBe "later-identity"
    } finally
      previous match {
        case Some(value) => conf.set(key, value)
        case None        => conf.unset(key)
      }
  }

  test(
    "configureHadoopS3ForPath with isSource=true honors source overrides and falls back to main"
  ) {
    val cfg = BackfillConfig(
      s3Endpoint = "minio:9000",
      s3BucketName = "main-bucket",
      s3AccessKey = "main-ak",
      s3SecretKey = "main-sk",
      sourceS3Endpoint = Some("src:9000"),
      sourceS3AccessKey = Some("src-ak"),
      sourceS3SecretKey = Some("src-sk")
      // sourceS3UseSSL/UseIam left None — should fall back to main
    )
    BackfillStorage.configureHadoopS3ForPath(
      configuration,
      "s3a://source-bucket/data.parquet",
      cfg,
      isSource = true
    )
    val hc = configuration
    hc.get("fs.s3a.bucket.source-bucket.endpoint") shouldBe "src:9000"
    hc.get("fs.s3a.bucket.source-bucket.access.key") shouldBe "src-ak"
    hc.get("fs.s3a.bucket.source-bucket.secret.key") shouldBe "src-sk"
  }

  // ============ COS source and target identities ============

  test(
    "COS configuration preserves independent source and target role identities"
  ) {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "com.zilliz.cloud.hadoop.TencentS3RoleCredentialsProvider"
    )
    conf.set("fs.s3a.assumed.role.arn", "qcs::cam::uin/123456:roleName/target")
    conf.set(
      "fs.s3a.bucket.source.aws.credentials.provider",
      "com.zilliz.cloud.hadoop.TencentS3RoleCredentialsProvider"
    )
    conf.set(
      "fs.s3a.bucket.source.assumed.role.arn",
      "qcs::cam::uin/234567:roleName/source"
    )
    conf.set("fs.s3a.bucket.source.assumed.role.external.id", "source-tenant")
    val config = BackfillConfig(
      s3Endpoint = "cos.ap-shanghai.myqcloud.com",
      s3BucketName = "target",
      s3AccessKey = "",
      s3SecretKey = "",
      s3CloudProvider = "tencent",
      s3UseIam = true
    )
    BackfillStorage.configureHadoopS3ForPath(
      conf,
      "s3a://source/input",
      config,
      isSource = true
    )
    BackfillStorage.configureHadoopS3ForPath(
      conf,
      "s3a://target/snapshot",
      config,
      isSource = false
    )
    Seq("source", "target").foreach { bucket =>
      conf.get(s"fs.s3a.bucket.$bucket.path.style.access") shouldBe "false"
      conf.get(s"fs.s3a.bucket.$bucket.bucket.probe") shouldBe "0"
      conf.get(
        s"fs.s3a.bucket.$bucket.multiobjectdelete.enable"
      ) shouldBe "false"
    }
    conf.get(
      "fs.s3a.aws.credentials.provider"
    ) shouldBe "com.zilliz.cloud.hadoop.TencentS3RoleCredentialsProvider"
    conf.get(
      "fs.s3a.bucket.source.aws.credentials.provider"
    ) shouldBe "com.zilliz.cloud.hadoop.TencentS3RoleCredentialsProvider"
    conf.get(
      "fs.s3a.bucket.source.assumed.role.arn"
    ) shouldBe "qcs::cam::uin/234567:roleName/source"
    conf.get(
      "fs.s3a.bucket.source.assumed.role.external.id"
    ) shouldBe "source-tenant"
    conf.get(
      "fs.s3a.assumed.role.arn"
    ) shouldBe "qcs::cam::uin/123456:roleName/target"
  }

  test(
    "COS IAM without its platform provider fails before changing Hadoop configuration"
  ) {
    val conf = new Configuration(false)
    val config = BackfillConfig(
      s3Endpoint = "cos.ap-shanghai.myqcloud.com",
      s3BucketName = "target",
      s3AccessKey = "",
      s3SecretKey = "",
      s3CloudProvider = "tencent",
      s3UseIam = true
    )
    intercept[IllegalArgumentException](
      BackfillStorage.configureHadoopS3ForPath(
        conf,
        "s3a://target/snapshot",
        config,
        isSource = false
      )
    ).getMessage should include("TencentS3RoleCredentialsProvider")
    conf.get("fs.s3a.bucket.target.aws.credentials.provider") shouldBe null
    conf.get("fs.s3a.bucket.target.endpoint") shouldBe null
  }

  test(
    "COS static credentials override the platform role while retaining COS Hadoop settings"
  ) {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "com.zilliz.cloud.hadoop.TencentS3RoleCredentialsProvider"
    )
    val config = BackfillConfig(
      s3Endpoint = "cos.ap-shanghai.myqcloud.com",
      s3BucketName = "target",
      s3AccessKey = "ak",
      s3SecretKey = "sk",
      s3CloudProvider = "tencent"
    )
    BackfillStorage.configureHadoopS3ForPath(
      conf,
      "s3a://target/snapshot",
      config,
      isSource = false
    )
    conf.get(
      "fs.s3a.bucket.target.aws.credentials.provider"
    ) shouldBe "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    conf.get("fs.s3a.bucket.target.access.key") shouldBe "ak"
    conf.get("fs.s3a.bucket.target.secret.key") shouldBe "sk"
    conf.get("fs.s3a.bucket.target.path.style.access") shouldBe "false"
    conf.get("fs.s3a.bucket.target.bucket.probe") shouldBe "0"
  }

  test(
    "a Tencent target does not impose COS settings or credentials on an AWS source"
  ) {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "com.zilliz.cloud.hadoop.TencentS3RoleCredentialsProvider"
    )
    conf.set("fs.s3a.assumed.role.arn", "qcs::cam::uin/123456:roleName/target")
    conf.set(
      "fs.s3a.bucket.source.aws.credentials.provider",
      BackfillConfig.HadoopS3AssumedRoleProvider
    )
    conf.set(
      "fs.s3a.bucket.source.assumed.role.arn",
      "arn:aws:iam::123456789012:role/source"
    )
    val config = BackfillConfig(
      s3Endpoint = "cos.ap-shanghai.myqcloud.com",
      s3BucketName = "target",
      s3AccessKey = "",
      s3SecretKey = "",
      s3CloudProvider = "tencent",
      s3UseIam = true,
      sourceS3Endpoint = Some("s3.us-west-2.amazonaws.com"),
      sourceS3Region = Some("us-west-2")
    )
    BackfillStorage.configureHadoopS3ForPath(
      conf,
      "s3a://source/input",
      config,
      isSource = true
    )
    conf.get(
      "fs.s3a.bucket.source.aws.credentials.provider"
    ) shouldBe BackfillConfig.HadoopS3AssumedRoleProvider
    conf.get("fs.s3a.bucket.source.path.style.access") shouldBe "true"
    conf.get("fs.s3a.bucket.source.bucket.probe") shouldBe null
    conf.get("fs.s3a.bucket.source.multiobjectdelete.enable") shouldBe null
    conf.get(
      "fs.s3a.bucket.source.assumed.role.arn"
    ) shouldBe "arn:aws:iam::123456789012:role/source"
    conf.get(
      "fs.s3a.assumed.role.arn"
    ) shouldBe "qcs::cam::uin/123456:roleName/target"
  }
  test(
    "catalog source static overrides displace inherited bucket defaults only in its private binding"
  ) {
    val conf = spark.sparkContext.hadoopConfiguration
    val prefix = "fs.s3a.bucket.shared."
    val settings = Map(
      prefix + "access.key" -> "target-ak",
      prefix + "secret.key" -> "target-sk",
      prefix + "session.token" -> "target-token",
      prefix + "aws.credentials.provider" -> BackfillConfig.HadoopS3AssumedRoleProvider,
      prefix + "assumed.role.arn" -> "arn:aws:iam::123456789012:role/target",
      prefix + "endpoint" -> "target-endpoint",
      prefix + "endpoint.region" -> "target-region",
      prefix + "connection.ssl.enabled" -> "true"
    )
    val previous = settings.keys.map(key => key -> Option(conf.get(key))).toMap
    try {
      settings.foreach { case (key, value) => conf.set(key, value) }
      val config = BackfillConfig(
        inputFormat = "iceberg",
        storageProperties = Map("fs.storage_type" -> "local"),
        sourceS3AccessKey = Some("source-ak"),
        sourceS3SecretKey = Some("source-sk"),
        sourceS3Endpoint = Some("source-endpoint"),
        sourceS3Region = Some("source-region"),
        sourceS3UseSSL = Some(false)
      )
      val bound = BackfillStorage
        .inputBinding(spark, "catalog.db.table", config)
        .newHadoopConfiguration()
      bound.get("fs.s3a.access.key") shouldBe "source-ak"
      bound.get("fs.s3a.secret.key") shouldBe "source-sk"
      bound.get("fs.s3a.endpoint") shouldBe "source-endpoint"
      bound.get("fs.s3a.endpoint.region") shouldBe "source-region"
      bound.get("fs.s3a.connection.ssl.enabled") shouldBe "false"
      bound.get(prefix + "access.key") shouldBe null
      bound.get(prefix + "session.token") shouldBe null
      bound.get(prefix + "aws.credentials.provider") shouldBe null
      bound.get(prefix + "endpoint") shouldBe null
      bound.get(prefix + "assumed.role.arn") shouldBe settings(
        prefix + "assumed.role.arn"
      )
      settings.foreach { case (key, value) => conf.get(key) shouldBe value }
    } finally
      previous.foreach { case (key, value) =>
        value match {
          case Some(text) => conf.set(key, text)
          case None       => conf.unset(key)
        }
      }
  }

  test(
    "catalog source endpoint overrides preserve the platform's managed IAM role"
  ) {
    val conf = spark.sparkContext.hadoopConfiguration
    val prefix = "fs.s3a.bucket.shared."
    val settings = Map(
      prefix + "aws.credentials.provider" -> BackfillConfig.HadoopS3AssumedRoleProvider,
      prefix + "assumed.role.arn" -> "arn:aws:iam::123456789012:role/source",
      prefix + "endpoint" -> "old-endpoint"
    )
    val previous = settings.keys.map(key => key -> Option(conf.get(key))).toMap
    try {
      settings.foreach { case (key, value) => conf.set(key, value) }
      val config = BackfillConfig(
        inputFormat = "iceberg",
        storageProperties = Map("fs.storage_type" -> "local"),
        sourceS3UseIam = Some(true),
        sourceS3Endpoint = Some("new-endpoint")
      )
      val bound = BackfillStorage
        .inputBinding(spark, "catalog.db.table", config)
        .newHadoopConfiguration()
      bound.get(
        prefix + "aws.credentials.provider"
      ) shouldBe BackfillConfig.HadoopS3AssumedRoleProvider
      bound.get(prefix + "assumed.role.arn") shouldBe settings(
        prefix + "assumed.role.arn"
      )
      bound.get(prefix + "endpoint") shouldBe null
      bound.get("fs.s3a.endpoint") shouldBe "new-endpoint"
    } finally
      previous.foreach { case (key, value) =>
        value match {
          case Some(text) => conf.set(key, text)
          case None       => conf.unset(key)
        }
      }
  }

  test("source S3 IAM validation ignores another backend's OSS namespace") {
    val conf = new Configuration(false)
    conf.set(
      BackfillConfig.HadoopS3CredentialsProvider,
      BackfillConfig.HadoopS3AssumedRoleProvider
    )
    conf.set(
      BackfillConfig.HadoopS3AssumedRoleArn,
      "arn:aws:iam::123456789012:role/source"
    )
    // An incomplete unrelated OSS role must not decide S3 input identity.
    conf.set(
      BackfillConfig.HadoopOssCredentialsProvider,
      BackfillConfig.HadoopOssAssumedRoleProvider
    )
    val config = BackfillConfig
      .forTest("c")
      .copy(s3AccessKey = "", s3SecretKey = "", s3UseIam = true)
    BackfillStorage.configureHadoopS3ForPath(
      conf,
      "s3a://source/data",
      config,
      isSource = true
    )
    conf.get(
      BackfillConfig.HadoopS3CredentialsProvider
    ) shouldBe BackfillConfig.HadoopS3AssumedRoleProvider
    conf.get(
      BackfillConfig.HadoopS3AssumedRoleArn
    ) shouldBe "arn:aws:iam::123456789012:role/source"
    conf.get(
      BackfillConfig.HadoopOssCredentialsProvider
    ) shouldBe BackfillConfig.HadoopOssAssumedRoleProvider
  }

}
