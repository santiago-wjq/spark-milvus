package com.zilliz.spark.connector.input

import java.nio.file.Files
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.hadoop.HadoopFileIO
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach

import com.zilliz.milvus.storage.write.WriterResources

final class IcebergInputTest
    extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach {
  private var spark: SparkSession = _
  private val directory = Files.createTempDirectory("offline-iceberg-input-")
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
      .appName("offline-iceberg-input-test")
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
}
