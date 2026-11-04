package com.zilliz.spark.connector.input

import java.nio.file.Files
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.s3a.S3AUtils
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.iceberg.hadoop.HadoopFileIO
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach

import com.zilliz.milvus.storage.write.WriterResources

final class InputAdapterTest
    extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach {
  private var spark: SparkSession = _
  private val directory = Files.createTempDirectory("offline-input-")
  private var inputs = Vector.empty[InputSource]

  private def openInput(
      spark: SparkSession,
      path: String,
      options: InputOptions,
      binding: InputStorageBinding
  ): InputSource = {
    val source = InputAdapter.read(spark, path, options, binding)
    inputs :+= source
    source
  }

  override def afterEach(): Unit = {
    try
      WriterResources.closeAll(
        null,
        inputs.reverse.map(input => () => input.close()): _*
      )
    finally {
      inputs = Vector.empty
      super.afterEach()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("offline-input-test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set(
      "spark.sql.catalog.inputs",
      "org.apache.iceberg.spark.SparkCatalog"
    )
    spark.conf.set("spark.sql.catalog.inputs.type", "hadoop")
    spark.conf.set(
      "spark.sql.catalog.inputs.warehouse",
      directory.resolve("warehouse").toUri.toString
    )
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
      val files = Files.walk(directory)
      try files.iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
      finally files.close()
    } finally super.afterAll()
  }

  private def binding =
    InputStorageBinding.capture(spark.sparkContext.hadoopConfiguration)

  test(
    "input identity is immutable, filesystem-only and does not share filesystem caches"
  ) {
    val configuration = new Configuration(false)
    configuration.set("fs.s3a.bucket.shared.access.key", "source")
    configuration.set("fs.s3a.bucket.shared.secret.key", "private-secret")
    configuration.set("fs.oss.credentials.provider", "selected-provider")
    configuration.set("milvus.token", "service-token")
    val captured = InputStorageBinding.capture(configuration)
    configuration.set("fs.s3a.bucket.shared.access.key", "target")
    val first = captured.newHadoopConfiguration()
    assert(first.get("fs.s3a.bucket.shared.access.key") == "source")
    assert(first.get("fs.oss.credentials.provider") == "selected-provider")
    assert(first.get("milvus.token") == null)
    assert(first.getBoolean("fs.s3a.impl.disable.cache", false))
    assert(first.getBoolean("fs.oss.impl.disable.cache", false))
    first.set("fs.s3a.bucket.shared.access.key", "changed")
    assert(
      captured
        .newHadoopConfiguration()
        .get("fs.s3a.bucket.shared.access.key") == "source"
    )
    assert(!captured.toString.contains("private-secret"))
  }

  test("capture expands configuration substitutions once") {
    val configuration = new Configuration(false)
    configuration.set("selected.input.key", "source-key")
    configuration.set("fs.s3a.access.key", "${selected.input.key}")
    val captured = InputStorageBinding.capture(configuration)
    configuration.set("selected.input.key", "target-key")
    assert(
      captured.newHadoopConfiguration().get("fs.s3a.access.key") == "source-key"
    )
    assert(captured.newHadoopConfiguration().get("selected.input.key") == null)
  }

  test(
    "Parquet retains NULL values and can recompute without a local checkpoint"
  ) {
    val path = directory.resolve("parquet").toString
    spark
      .sql(
        "SELECT 1L AS id, CAST(NULL AS STRING) AS value UNION ALL SELECT 2L, 'two'"
      )
      .write
      .parquet(path)
    val identity = binding
    val source = openInput(spark, path, InputOptions("parquet"), identity)
    assert(source.icebergSnapshotId.isEmpty)
    val expected = Seq(1L -> None, 2L -> Some("two"))
    def values = source.dataFrame
      .orderBy("id")
      .collect()
      .toSeq
      .map(row => row.getLong(0) -> Option(row.getString(1)))
    assert(values == expected)
    assert(values == expected)
    assert(spark.sparkContext.getPersistentRDDs.isEmpty)
    val independent = openInput(spark, path, InputOptions("parquet"), identity)
    source.close()
    assert(independent.dataFrame.count() == 2L)
  }

  test(
    "Hadoop credential provider files remain available to the captured input"
  ) {
    val configuration = new Configuration(false)
    configuration.set(
      "hadoop.security.credential.provider.path",
      "jceks://file" + directory.resolve("input-credentials.jceks").toString
    )
    configuration.set("hadoop.security.credential.clear-text-fallback", "false")
    val provider = CredentialProviderFactory.getProviders(configuration).get(0)
    provider.createCredentialEntry(
      "fs.s3a.secret.key",
      "provider-secret".toCharArray
    )
    provider.flush()
    val captured =
      InputStorageBinding.capture(configuration).newHadoopConfiguration()
    assert(
      new String(captured.getPassword("fs.s3a.secret.key")) == "provider-secret"
    )
    assert(
      !captured.getBoolean(
        "hadoop.security.credential.clear-text-fallback",
        true
      )
    )
  }

  test(
    "S3A's actual bucket propagation cannot revive a removed default identity"
  ) {
    val inherited = new Configuration(false)
    inherited.set("fs.s3a.bucket.same.bucket.access.key", "target-key")
    inherited.set("fs.s3a.bucket.same.bucket.secret.key", "target-secret")
    inherited.set("fs.s3a.bucket.same.bucket.session.token", "target-token")
    inherited.set(
      "fs.s3a.bucket.same.bucket.aws.credentials.provider",
      "target-provider"
    )
    inherited.set("fs.s3a.vectored.read.max.merged.size", "2M")
    val explicit = new Configuration(false)
    explicit.set("fs.s3a.access.key", "source-key")
    explicit.set("fs.s3a.secret.key", "source-secret")
    explicit.set("fs.s3a.aws.credentials.provider", "source-provider")
    val binding = InputStorageBinding.capture(explicit)
    val configured = new Configuration(inherited)
    binding.optionsOver(inherited).foreach { case (key, value) =>
      configured.set(key, value)
    }
    InputStorageBinding.removeInherited(configured)
    val effective = S3AUtils.propagateBucketOptions(configured, "same.bucket")
    assert(effective.get("fs.s3a.access.key") == "source-key")
    assert(effective.get("fs.s3a.secret.key") == "source-secret")
    assert(
      effective.get("fs.s3a.aws.credentials.provider") == "source-provider"
    )
    assert(Option(effective.get("fs.s3a.session.token")).forall(_.isEmpty))
    assert(
      effective.getLongBytes("fs.s3a.vectored.read.max.merged.size", 32L) == 32L
    )
    assert(
      inherited.get("fs.s3a.bucket.same.bucket.secret.key") == "target-secret"
    )

    val managed = InputStorageBinding.capture(inherited)
    assert(
      managed.optionsOver(explicit)(
        "fs.s3a.bucket.same.bucket.aws.credentials.provider"
      ) == "target-provider"
    )
  }

  test(
    "Iceberg fixes the current snapshot and keeps it after subsequent commits"
  ) {
    spark.sql("CREATE TABLE inputs.db.pinned (id BIGINT) USING iceberg")
    spark.sql("INSERT INTO inputs.db.pinned VALUES (1)")
    val shared = spark.sparkContext.hadoopConfiguration
      .iterator()
      .asScala
      .map(entry => entry.getKey -> entry.getValue)
      .toMap
    val first =
      openInput(spark, "inputs.db.pinned", InputOptions("iceberg"), binding)
    val id = first.icebergSnapshotId.get
    spark.sql("INSERT INTO inputs.db.pinned VALUES (2)")
    assert(first.dataFrame.collect().map(_.getLong(0)).toSeq == Seq(1L))
    assert(first.dataFrame.collect().map(_.getLong(0)).toSeq == Seq(1L))
    val explicit = openInput(
      spark,
      "inputs.db.pinned",
      InputOptions("iceberg", Some(id.toString)),
      binding
    )
    assert(explicit.icebergSnapshotId.contains(id))
    assert(explicit.dataFrame.collect().map(_.getLong(0)).toSeq == Seq(1L))
    val latest =
      openInput(spark, "inputs.db.pinned", InputOptions("iceberg"), binding)
    assert(latest.icebergSnapshotId != first.icebergSnapshotId)
    assert(latest.dataFrame.count() == 2L)
    assert(
      spark.sparkContext.hadoopConfiguration
        .iterator()
        .asScala
        .map(entry => entry.getKey -> entry.getValue)
        .toMap == shared
    )
  }

  test(
    "Iceberg refuses invalid or missing snapshots and unsupported identities before a scan"
  ) {
    spark.sql("CREATE TABLE inputs.db.empty (id BIGINT) USING iceberg")
    assert(intercept[IllegalArgumentException] {
      openInput(spark, "inputs.db.empty", InputOptions("iceberg"), binding)
    }.getMessage.contains("no current snapshot"))
    Seq("", "-1", "0", "not-a-number", "999999999999999999999999").foreach {
      id =>
        intercept[IllegalArgumentException] {
          openInput(
            spark,
            "inputs.db.empty",
            InputOptions("iceberg", Some(id)),
            binding
          )
        }
    }
    assert(intercept[IllegalArgumentException] {
      openInput(
        spark,
        "inputs.db.empty",
        InputOptions("iceberg", Some("1")),
        binding
      )
    }.getMessage.contains("does not exist"))
    Seq("s3://bucket/table", "/tmp/table", "db.table").foreach { path =>
      intercept[IllegalArgumentException] {
        openInput(spark, path, InputOptions("iceberg"), binding)
      }
    }
    spark.conf.set("spark.sql.catalog.inputs.io-impl", "example.UnboundFileIO")
    try
      assert(intercept[IllegalArgumentException] {
        openInput(spark, "inputs.db.empty", InputOptions("iceberg"), binding)
      }.getMessage.contains("HadoopFileIO"))
    finally spark.conf.unset("spark.sql.catalog.inputs.io-impl")
    spark.conf.set(
      "spark.sql.catalog.inputs.catalog-impl",
      "example.UnboundCatalog"
    )
    try
      assert(intercept[IllegalArgumentException] {
        openInput(spark, "inputs.db.empty", InputOptions("iceberg"), binding)
      }.getMessage.contains("custom catalog"))
    finally spark.conf.unset("spark.sql.catalog.inputs.catalog-impl")
  }

  test(
    "Iceberg catalog defaults cannot override the captured filesystem identity"
  ) {
    spark.sql("CREATE TABLE inputs.db.bound (id BIGINT) USING iceberg")
    spark.sql("INSERT INTO inputs.db.bound VALUES (1)")
    val defaultKey =
      "spark.sql.catalog.inputs.hadoop.fs.s3a.bucket.shared.secret.key"
    val providerKey =
      "spark.sql.catalog.inputs.hadoop.hadoop.security.credential.provider.path"
    spark.conf.set(defaultKey, "target-default")
    spark.conf.set(providerKey, "jceks://file/missing-target-credentials")
    try {
      val configuration =
        new Configuration(spark.sparkContext.hadoopConfiguration)
      configuration.set("fs.s3a.secret.key", "source-explicit")
      val source = openInput(
        spark,
        "inputs.db.bound",
        InputOptions("iceberg"),
        InputStorageBinding.capture(configuration)
      )
      val session = source.dataFrame.sparkSession
      val fileIO = session.withActive {
        Spark3Util
          .loadIcebergTable(session, "inputs.db.bound")
          .io()
          .asInstanceOf[HadoopFileIO]
      }
      assert(fileIO.getConf.get("fs.s3a.secret.key") == "source-explicit")
      assert(
        fileIO.getConf.get(
          "fs.s3a.bucket.shared.secret.key"
        ) == "source-explicit"
      )
      assert(
        Option(fileIO.getConf.get("hadoop.security.credential.provider.path"))
          .forall(_.isEmpty)
      )
      assert(source.dataFrame.count() == 1L)
      assert(spark.conf.get(defaultKey) == "target-default")
      assert(
        spark.conf.get(providerKey) == "jceks://file/missing-target-credentials"
      )
    } finally {
      spark.conf.unset(defaultKey)
      spark.conf.unset(providerKey)
    }
  }

  test("Lance and snapshot-id on Parquet are rejected before storage access") {
    Seq(InputOptions("lance"), InputOptions("parquet", Some("1"))).foreach {
      options =>
        intercept[IllegalArgumentException] {
          openInput(spark, "unsupported://never-opened", options, binding)
        }
    }
  }
}
