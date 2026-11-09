package com.zilliz.spark.connector.backfill

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

final class BackfillResourcesTest extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("BackfillResourcesTest")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  private def localConfig = BackfillConfig(storageProperties =
    Map("fs.storage_type" -> "local", "fs.root_path" -> "/tmp/backfill")
  )

  test(
    "a caller's release runs once when the input is rejected before computation"
  ) {
    var releases = 0
    val input = spark.range(1).toDF("id")
    val rejected = Seq(
      MilvusBackfill.run(
        input,
        " ",
        localConfig,
        release = () => releases += 1
      ),
      MilvusBackfill.run(
        null,
        "snapshot.json",
        localConfig,
        release = () => releases += 1
      ),
      MilvusBackfill.runPrepared(
        null,
        "snapshot.json",
        localConfig,
        10L,
        "source",
        () => fail("a rejected input never reaches the write guard"),
        BackfillSnapshotAdapter.V3Only,
        _ => fail("a rejected input has no result"),
        _ => fail("a rejected input has no recovery"),
        release = () => releases += 1
      )
    )
    assert(releases == 3)
    rejected.foreach { outcome =>
      val error = outcome.left.toOption.get
      assert(error.isInstanceOf[SchemaValidationError])
      assert(error.cleanupComplete.isEmpty)
    }
  }

  test(
    "a failed release after an early rejection keeps the rejection and marks cleanup incomplete"
  ) {
    val failure = new IllegalStateException("input release failed")
    var releases = 0
    val outcome = MilvusBackfill.run(
      spark.range(1).toDF("id"),
      "",
      localConfig,
      release = () => { releases += 1; throw failure }
    )
    val error = outcome.left.toOption.get
    assert(releases == 1)
    assert(error.isInstanceOf[SchemaValidationError])
    assert(error.message.contains("existing snapshot path"))
    assert(error.cause.contains(failure))
    assert(error.cleanupComplete.contains(false))
  }

  test(
    "a successful run releases cached rows before input and reports close failures"
  ) {
    val released = ArrayBuffer.empty[String]
    val unpersist = new IllegalStateException("unpersist failed")
    val inputClose = new IllegalStateException("input close failed")
    val result = MilvusBackfill.withCleanup(
      () => { released += "cached"; throw unpersist },
      () => { released += "input"; throw inputClose }
    )(Right(7))
    assert(released.toSeq == Seq("cached", "input"))
    assert(result.left.toOption.get.cause.contains(unpersist))
    assert(result.left.toOption.get.stage == "cleanup")
    assert(result.left.toOption.get.cleanupComplete.contains(false))
    assert(unpersist.getSuppressed.toSeq == Seq(inputClose))
  }

  test(
    "an early typed business error remains primary when closing input fails"
  ) {
    val primary = new IllegalArgumentException("invalid target type")
    val error = SchemaValidationError("invalid target type", Some(primary))
    val close = new IllegalStateException("input close failed")
    def execute(): Either[BackfillError, Int] = { return Left(error) }
    val result = MilvusBackfill.withCleanup(() => throw close)(execute())
    assert(result.left.toOption.get.isInstanceOf[SchemaValidationError])
    assert(result.left.toOption.get.cause.contains(primary))
    assert(result.left.toOption.get.cleanupComplete.contains(false))
    assert(primary.getSuppressed.toSeq == Seq(close))
  }

  test(
    "an early business error without a throwable retains its type and gains close diagnostics"
  ) {
    val close = new IllegalStateException("input close failed")
    val result = MilvusBackfill.withCleanup(() => throw close)(
      Left(SchemaValidationError("missing field"))
    )
    val error = result.left.toOption.get
    assert(
      error
        .isInstanceOf[SchemaValidationError] && error.message == "missing field"
    )
    assert(error.cause.get.getSuppressed.toSeq == Seq(close))
  }

  test("an operation exception remains primary and all resources are closed") {
    val primary = new IllegalStateException("scan failed")
    val close = new IllegalStateException("input close failed")
    var released = false
    val result = MilvusBackfill.withCleanup(
      () => { released = true },
      () => throw close
    )(throw primary)
    assert(released)
    assert(result.left.toOption.get.cause.contains(primary))
    assert(primary.getSuppressed.toSeq == Seq(close))
  }
}
