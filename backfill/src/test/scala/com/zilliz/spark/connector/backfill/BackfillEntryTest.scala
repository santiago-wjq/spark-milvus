package com.zilliz.spark.connector.backfill

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.storage.snapshot.json.SnapshotJson

/** Target validation, storage options and the checks every caller of the
  * DataFrame entry passes before computation.
  */
class BackfillEntryTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("BackfillEntryTest")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test(
    "physical-row preflight rejects missing segments, count mismatch and offset corruption before writing"
  ) {
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{LongType, StructField, StructType}
    val schema = StructType(
      Seq(
        StructField(BackfillSegmentWrite.SegmentIdCol, LongType, false),
        StructField(BackfillSegmentWrite.RowOffsetCol, LongType, false)
      )
    )
    def check(rows: Seq[Row], expected: Map[Long, Long]): Unit =
      BackfillSegmentWrite.validatePhysicalRows(
        spark.createDataFrame(spark.sparkContext.parallelize(rows, 2), schema),
        expected
      )
    val rows = Seq(Row(1L, 0L), Row(1L, 1L), Row(2L, 0L))
    check(rows, Map(1L -> 2L, 2L -> 1L, 3L -> 0L))
    Seq(
      Map(1L -> 1L, 2L -> 1L),
      Map(1L -> 2L, 2L -> 1L, 3L -> 1L),
      Map(1L -> 2L)
    )
      .foreach(expected =>
        intercept[IllegalArgumentException](check(rows, expected))
      )
    Seq(Seq(Row(1L, 0L), Row(1L, 0L)), Seq(Row(1L, 0L), Row(1L, 2L)))
      .foreach(broken =>
        intercept[IllegalArgumentException](check(broken, Map(1L -> 2L)))
      )
  }

  test("validate fails on empty s3Endpoint") {
    val cfg = BackfillConfig(
      s3Endpoint = "",
      s3BucketName = "b",
      s3AccessKey = "ak",
      s3SecretKey = "sk"
    )
    cfg.validate() shouldBe Left("s3Endpoint cannot be empty")
  }

  test("validate fails on empty s3BucketName") {
    val cfg = BackfillConfig(
      s3Endpoint = "ep",
      s3BucketName = "",
      s3AccessKey = "ak",
      s3SecretKey = "sk"
    )
    cfg.validate() shouldBe Left("s3BucketName cannot be empty")
  }

  test("validate fails on non-positive batchSize") {
    val cfg = BackfillConfig(
      s3Endpoint = "ep",
      s3BucketName = "b",
      s3AccessKey = "ak",
      s3SecretKey = "sk",
      batchSize = 0
    )
    cfg.validate() shouldBe Left("batchSize must be positive")
  }

  test("validate accepts empty AK/SK (IAM/IRSA)") {
    val cfg = BackfillConfig(
      s3Endpoint = "ep",
      s3BucketName = "b",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )
    cfg.validate() shouldBe Right(())
  }

  test("snapshot metadata base paths strip the configured Milvus endpoint") {
    val metadata = SnapshotJson
      .parse(
        """{
          |  "snapshot_info": {
          |    "name": "s1",
          |    "collection_id": 10,
          |    "partition_ids": [20],
          |    "create_ts": 1
          |  },
          |  "collection": {"schema": {"name": "c", "fields": []}},
          |  "manifest_list": [],
          |  "storagev2_manifest_list": [{
          |    "segmentID": 30,
          |    "manifest": "{\"ver\":7,\"base_path\":\"s3://minio:9000/managed-bucket/files/insert_log/10/20/30\"}"
          |  }]
          |}""".stripMargin
      )
      .toOption
      .get

    val (collectionId, partitions, basePaths) =
      MilvusBackfill.extractMetadataFromSnapshot(
        metadata,
        bucket = "managed-bucket",
        endpoint = "minio:9000"
      )

    collectionId shouldBe 10L
    partitions shouldBe Map(30L -> 20L)
    basePaths shouldBe Map(30L -> "files/insert_log/10/20/30")
  }

  test("getMilvusReadOptions omits AK/SK when s3UseIam=true") {
    val cfg = BackfillConfig(
      s3Endpoint = "s3.us-west-2.amazonaws.com",
      s3BucketName = "irsa-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )
    val opts = cfg.getMilvusReadOptions
    opts.get("fs.access_key_id") shouldBe None
    opts.get("fs.access_key_value") shouldBe None
    opts("fs.use_iam") shouldBe "true"
  }

  test("MilvusBackfill.run derives Hadoop AssumeRole for embedded callers") {
    val hc = spark.sparkContext.hadoopConfiguration
    hc.set(
      HadoopKeys.S3CredentialsProvider,
      BackfillConfig.HadoopS3AssumedRoleProvider
    )
    val cfg = BackfillConfig(
      s3Endpoint = "s3.us-west-2.amazonaws.com",
      s3BucketName = "embedded-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )

    try {
      // The target binds before the snapshot is read.
      MilvusBackfill.run(spark.range(1).toDF("id"), "unused.json", cfg) match {
        case Left(error) =>
          error.message should include(
            "Invalid Hadoop storage AssumeRole configuration"
          )
          error.message should include(HadoopKeys.S3AssumedRoleArn)
        case Right(_) => fail("expected invalid Hadoop AssumeRole config")
      }
    } finally {
      hc.unset(HadoopKeys.S3CredentialsProvider)
      hc.unset(HadoopKeys.S3AssumedRoleArn)
    }
  }

  test("MilvusBackfill.run requires a snapshot before reading anything") {
    for {
      joinKey <- Seq(
        BackfillJoinKey.PrimaryKey,
        BackfillJoinKey.PhysicalField("external_id")
      )
      snapshotPath <- Seq(null, "", " \t")
    } {
      val cfg = BackfillConfig(
        collectionName = "collection",
        s3Endpoint = "s3.us-west-2.amazonaws.com",
        s3BucketName = "embedded-bucket",
        s3AccessKey = "",
        s3SecretKey = "",
        s3UseIam = true,
        joinKey = joinKey
      )
      // The configured online collection is never consulted before the
      // required snapshot has been supplied.
      withClue(s"joinKey=$joinKey, snapshotPath=$snapshotPath: ") {
        MilvusBackfill.run(
          spark.range(1).toDF("id"),
          snapshotPath,
          cfg
        ) match {
          case Left(error: SchemaValidationError) =>
            error.message shouldBe "Backfill requires an existing snapshot path"
          case other =>
            fail(s"expected missing-snapshot validation, got $other")
        }
      }
    }
  }

  test("getMilvusReadOptions includes AK/SK when s3UseIam=false") {
    val cfg = BackfillConfig(
      s3Endpoint = "minio:9000",
      s3BucketName = "b",
      s3AccessKey = "ak",
      s3SecretKey = "sk"
    )
    val opts = cfg.getMilvusReadOptions
    opts("fs.access_key_id") shouldBe "ak"
    opts("fs.access_key_value") shouldBe "sk"
  }

  test("getS3WriteOptionsForBasePath omits AK/SK when s3UseIam=true") {
    val cfg = BackfillConfig(
      s3Endpoint = "s3.us-west-2.amazonaws.com",
      s3BucketName = "irsa-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )
    val opts = cfg.getS3WriteOptionsForBasePath("base/path")
    opts.get("fs.access_key_id") shouldBe None
    opts.get("fs.access_key_value") shouldBe None
    opts("fs.use_iam") shouldBe "true"
  }

  test("Backfill native options include the configured AssumeRole settings") {
    val cfg = BackfillConfig(
      s3Endpoint = "oss-cn-shanghai.aliyuncs.com",
      s3BucketName = "managed-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3CloudProvider = "aliyun",
      s3UseIam = true,
      s3RoleArn = Some("acs:ram::123456789012:role/data-role"),
      s3RoleSessionName = Some("spark-job"),
      s3ExternalId = Some("external-id")
    )

    Seq(
      cfg.getMilvusReadOptions,
      cfg.getS3WriteOptionsForBasePath("base/path")
    ).foreach { opts =>
      opts("fs.role_arn") shouldBe
        "acs:ram::123456789012:role/data-role"
      opts("fs.session_name") shouldBe "spark-job"
      opts("fs.external_id") shouldBe "external-id"
      opts("fs.cloud_provider") shouldBe "aliyun"
      opts.get("fs.access_key_id") shouldBe None
      opts.get("fs.access_key_value") shouldBe None
    }
  }
}
