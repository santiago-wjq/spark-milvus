package com.zilliz.spark.connector.input

import java.util.UUID
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach

import com.zilliz.milvus.storage.write.WriterResources

/** Real S3A requests: two identities on the same endpoint and bucket. */
final class InputIdentityNativeTest
    extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach {
  private var spark: SparkSession = _
  private var bucket: String = _
  private var source: InputStorageBinding = _
  private var target: InputStorageBinding = _
  private val run = UUID.randomUUID().toString
  private var inputs = Vector.empty[InputSource]

  private def openInput(
      spark: SparkSession,
      path: String,
      options: InputOptions,
      binding: InputStorageBinding
  ): InputSource = {
    val input = InputAdapter.read(spark, path, options, binding)
    inputs :+= input
    input
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

  private def required(name: String): String = sys.env
    .get(name)
    .filter(_.nonEmpty)
    .getOrElse(
      throw new IllegalArgumentException(s"Input identity tests require $name")
    )

  override def beforeAll(): Unit = {
    super.beforeAll()
    val endpoint = required("BACKFILL_TEST_S3_ENDPOINT")
    bucket = required("BACKFILL_TEST_S3_BUCKET")
    def identity(name: String): InputStorageBinding = {
      val configuration = new Configuration()
      val prefix = s"fs.s3a.bucket.$bucket."
      configuration.set(prefix + "endpoint", endpoint)
      configuration.set(prefix + "endpoint.region", "us-east-1")
      configuration.set(prefix + "path.style.access", "true")
      configuration.set(prefix + "bucket.probe", "0")
      configuration.set(prefix + "connection.ssl.enabled", "false")
      configuration.set(
        prefix + "access.key",
        required(s"BACKFILL_TEST_S3_${name}_ACCESS_KEY")
      )
      configuration.set(
        prefix + "secret.key",
        required(s"BACKFILL_TEST_S3_${name}_SECRET_KEY")
      )
      configuration.set(
        prefix + "aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
      )
      InputStorageBinding.capture(configuration)
    }
    source = identity("SOURCE")
    target = identity("TARGET")
    val inheritedKey =
      s"spark.sql.catalog.sourceinput.hadoop.fs.s3a.bucket.$bucket.access.key"
    val inheritedSecret =
      s"spark.sql.catalog.sourceinput.hadoop.fs.s3a.bucket.$bucket.secret.key"
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("input-identity-native")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config(inheritedKey, required("BACKFILL_TEST_S3_TARGET_ACCESS_KEY"))
      .config(inheritedSecret, required("BACKFILL_TEST_S3_TARGET_SECRET_KEY"))
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  private def sharedConfiguration = spark.sparkContext.hadoopConfiguration
    .iterator()
    .asScala
    .map(entry => entry.getKey -> entry.getValue)
    .toMap

  test(
    "storage policy rejects unscoped listing and cross-prefix listing or reads"
  ) {
    val identities = Seq("source" -> source, "target" -> target)
    val filesystems = identities.map { case (prefix, identity) =>
      prefix -> org.apache.hadoop.fs.FileSystem.newInstance(
        java.net.URI.create(s"s3a://$bucket"),
        identity.newHadoopConfiguration()
      )
    }
    def directory(prefix: String) =
      new org.apache.hadoop.fs.Path(s"s3a://$bucket/$prefix/$run/policy")
    def file(prefix: String) =
      new org.apache.hadoop.fs.Path(directory(prefix), "value")
    def denied(operation: => Unit): Unit = {
      val error = intercept[Exception](operation)
      val messages = Iterator
        .iterate[Throwable](error)(_.getCause)
        .takeWhile(_ != null)
        .map(e => Option(e.getMessage).getOrElse(""))
        .mkString(" ")
        .toLowerCase
      assert(
        messages.contains("403") || messages.contains(
          "accessdenied"
        ) || messages.contains("access denied")
      )
    }
    try {
      filesystems.foreach { case (prefix, fs) =>
        val out = fs.create(file(prefix), true)
        try out.write(42)
        finally out.close()
      }
      filesystems.foreach { case (prefix, fs) =>
        val other = if (prefix == "source") "target" else "source"
        assert(
          fs.listStatus(directory(prefix)).map(_.getPath.getName).toSeq == Seq(
            "value"
          )
        )
        val own = fs.open(file(prefix))
        try assert(own.read() == 42)
        finally own.close()
        denied {
          fs.listStatus(new org.apache.hadoop.fs.Path(s"s3a://$bucket/")); ()
        }
        denied { fs.listStatus(directory(other)); () }
        denied {
          val input = fs.open(file(other))
          try { input.read(); () }
          finally input.close()
        }
      }
    } finally
      WriterResources.closeAll(
        null,
        filesystems.map { case (_, fs) => () => fs.close() }: _*
      )
  }

  test(
    "same-bucket Parquet inputs retain distinct identities during lazy concurrent and repeated reads"
  ) {
    val sourcePath = s"s3a://$bucket/source/$run/parquet"
    val targetPath = s"s3a://$bucket/target/$run/parquet"
    spark
      .sql("SELECT 11L AS id")
      .write
      .options(source.hadoopOptions)
      .parquet(sourcePath)
    spark
      .sql("SELECT 22L AS id")
      .write
      .options(target.hadoopOptions)
      .parquet(targetPath)
    val before = sharedConfiguration
    val left = openInput(spark, sourcePath, InputOptions("parquet"), source)
    val right = openInput(spark, targetPath, InputOptions("parquet"), target)
    implicit val executionContext: ExecutionContext = ExecutionContext.global
    val results = Await.result(
      Future.sequence(
        Seq(
          Future(left.dataFrame.collect().head.getLong(0)),
          Future(right.dataFrame.collect().head.getLong(0))
        )
      ),
      90.seconds
    )
    assert(results == Seq(11L, 22L))
    assert(left.dataFrame.collect().head.getLong(0) == 11L)
    assert(right.dataFrame.collect().head.getLong(0) == 22L)
    intercept[Exception] {
      openInput(spark, sourcePath, InputOptions("parquet"), target).dataFrame
        .collect()
    }
    intercept[Exception] {
      openInput(spark, targetPath, InputOptions("parquet"), source).dataFrame
        .collect()
    }
    assert(sharedConfiguration == before)
  }

  test(
    "Iceberg metadata and executor data reads use the bound identity and pinned snapshot"
  ) {
    Seq(
      ("sourceinput", "source", source, 11),
      ("targetinput", "target", target, 22)
    ).foreach { case (catalog, prefix, identity, value) =>
      val property = s"spark.sql.catalog.$catalog"
      spark.conf.set(property, "org.apache.iceberg.spark.SparkCatalog")
      spark.conf.set(property + ".type", "hadoop")
      spark.conf.set(
        property + ".warehouse",
        s"s3a://$bucket/$prefix/$run/warehouse"
      )
      identity.hadoopOptions.foreach { case (key, setting) =>
        spark.conf.set(s"$property.hadoop.$key", setting)
      }
      spark.sql(s"CREATE TABLE $catalog.db.rows (id BIGINT) USING iceberg")
      spark.sql(s"INSERT INTO $catalog.db.rows VALUES ($value)")
    }
    val before = sharedConfiguration
    val left =
      openInput(spark, "sourceinput.db.rows", InputOptions("iceberg"), source)
    val right =
      openInput(spark, "targetinput.db.rows", InputOptions("iceberg"), target)
    spark.sql("INSERT INTO sourceinput.db.rows VALUES (33)")
    assert(left.icebergSnapshotId.nonEmpty && right.icebergSnapshotId.nonEmpty)
    assert(left.dataFrame.collect().map(_.getLong(0)).toSeq == Seq(11L))
    assert(right.dataFrame.collect().map(_.getLong(0)).toSeq == Seq(22L))
    assert(left.dataFrame.collect().map(_.getLong(0)).toSeq == Seq(11L))
    intercept[Exception] {
      openInput(
        spark,
        "sourceinput.db.rows",
        InputOptions("iceberg"),
        target
      ).dataFrame.collect()
    }
    intercept[Exception] {
      openInput(
        spark,
        "targetinput.db.rows",
        InputOptions("iceberg"),
        source
      ).dataFrame.collect()
    }
    assert(sharedConfiguration == before)
  }
}
