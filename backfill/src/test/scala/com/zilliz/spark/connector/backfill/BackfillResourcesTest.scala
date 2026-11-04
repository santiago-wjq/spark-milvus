package com.zilliz.spark.connector.backfill

import scala.collection.mutable.ArrayBuffer

import org.scalatest.funsuite.AnyFunSuite

final class BackfillResourcesTest extends AnyFunSuite {
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
