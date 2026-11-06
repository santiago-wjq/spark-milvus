package com.zilliz.milvus.storage.segcore

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Paths
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.UnknownFieldSet
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.read.exec.PhysicalColumnGroup
import com.zilliz.milvus.storage.snapshot.{
  DeleteFiles,
  Segment,
  SegmentIndex,
  SegmentLayout
}

/** The load info read back field by field with protobuf's own parser, against
  * the numbers of Milvus's `pkg/proto/segcore.proto`.
  */
class SegcoreLoadInfoTest extends AnyFunSuite with Matchers {

  private val basePath = "files/insert_log/3/2/1"
  private val indexPath =
    "files/insert_log/3/2/1/_index/12/1/milvus_packed_inverted_index.v3"

  private def v3Segment(
      layout: SegmentLayout = SegmentLayout.Manifest(basePath, 4L),
      storageVersion: Int = 3,
      rows: Option[Long] = Some(2048L)
  ): Segment =
    Segment(
      id = 1L,
      partitionId = 2L,
      storageVersion = storageVersion,
      rows = rows,
      layout = layout,
      deletes = DeleteFiles.InManifest
    )

  private def index(
      parameters: Map[String, String] = Map("index_type" -> "INVERTED"),
      currentIndexVersion: Option[Int] = Some(10),
      currentScalarIndexVersion: Option[Int] = Some(3),
      indexStorePathVersion: Option[Int] = Some(2),
      segmentId: Long = 1L
  ): SegmentIndex =
    SegmentIndex(
      collectionId = 3L,
      partitionId = 2L,
      segmentId = segmentId,
      fieldId = 105L,
      indexId = 11L,
      buildId = 12L,
      name = "i64_inverted",
      parameters = parameters,
      filePaths = Vector(indexPath),
      rowCount = 2048L,
      serializedSize = 42072L,
      indexVersion = 1L,
      currentIndexVersion = currentIndexVersion,
      currentScalarIndexVersion = currentScalarIndexVersion,
      indexStorePathVersion = indexStorePathVersion
    )

  private def numbers(message: UnknownFieldSet): Set[Int] =
    message.asMap.keySet.asScala.map(_.intValue).toSet

  private def varints(message: UnknownFieldSet, number: Int): Seq[Long] =
    message.getField(number).getVarintList.asScala.map(_.longValue).toSeq

  private def strings(message: UnknownFieldSet, number: Int): Seq[String] =
    message
      .getField(number)
      .getLengthDelimitedList
      .asScala
      .map(_.toString(UTF_8))
      .toSeq

  private def messages(
      message: UnknownFieldSet,
      number: Int
  ): Seq[UnknownFieldSet] =
    message
      .getField(number)
      .getLengthDelimitedList
      .asScala
      .map(UnknownFieldSet.parseFrom)
      .toSeq

  private def params(indexInfo: UnknownFieldSet): Seq[(String, String)] =
    messages(indexInfo, 6).map(pair =>
      strings(pair, 1).head -> strings(pair, 2).head
    )

  /** manifest_path: the JSON of Milvus's packed manifest reference. */
  private def manifest(loadInfo: UnknownFieldSet): (Long, String) = {
    val node = new ObjectMapper().readTree(strings(loadInfo, 21).head)
    (node.get("ver").asLong, node.get("base_path").asText)
  }

  private def encode(
      segment: Segment = v3Segment(),
      indexes: Seq[SegmentIndex] = Seq(index()),
      localRoot: Option[String] = None
  ): UnknownFieldSet =
    UnknownFieldSet.parseFrom(
      SegcoreLoadInfo.encode(3L, segment, indexes, localRoot.map(Paths.get(_)))
    )

  test("a V3 segment carries its identity, rows, manifest and indexes") {
    val loadInfo = encode()
    numbers(loadInfo) shouldBe Set(1, 2, 3, 7, 11, 15, 21)
    varints(loadInfo, 1) shouldBe Seq(1L)
    varints(loadInfo, 2) shouldBe Seq(2L)
    varints(loadInfo, 3) shouldBe Seq(3L)
    varints(loadInfo, 7) shouldBe Seq(2048L)
    varints(loadInfo, 15) shouldBe Seq(3L)
    manifest(loadInfo) shouldBe (4L, basePath)

    val Seq(indexInfo) = messages(loadInfo, 11)
    numbers(indexInfo) shouldBe Set(1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14)
    varints(indexInfo, 1) shouldBe Seq(105L)
    strings(indexInfo, 3) shouldBe Seq("i64_inverted")
    varints(indexInfo, 4) shouldBe Seq(11L)
    varints(indexInfo, 5) shouldBe Seq(12L)
    params(indexInfo) shouldBe Seq(
      "index_type" -> "INVERTED",
      "load_priority" -> "HIGH"
    )
    strings(indexInfo, 7) shouldBe Seq(indexPath)
    varints(indexInfo, 8) shouldBe Seq(42072L)
    varints(indexInfo, 9) shouldBe Seq(1L)
    varints(indexInfo, 10) shouldBe Seq(2048L)
    varints(indexInfo, 11) shouldBe Seq(10L)
    varints(indexInfo, 13) shouldBe Seq(3L)
    varints(indexInfo, 14) shouldBe Seq(2L)
  }

  test("a segment loaded without indexes lists none") {
    val loadInfo = encode(indexes = Seq.empty)
    numbers(loadInfo) shouldBe Set(1, 2, 3, 7, 15, 21)
  }

  test("local storage makes every key absolute") {
    val loadInfo = encode(localRoot = Some("/data/fixture"))
    manifest(loadInfo) shouldBe (4L, s"/data/fixture/$basePath")
    strings(messages(loadInfo, 11).head, 7) shouldBe Seq(
      s"/data/fixture/$indexPath"
    )
  }

  test("an absent version is left out, not written as zero") {
    val Seq(indexInfo) = messages(
      encode(indexes =
        Seq(
          index(
            currentIndexVersion = None,
            currentScalarIndexVersion = None,
            indexStorePathVersion = None
          )
        )
      ),
      11
    )
    numbers(indexInfo) shouldBe Set(1, 3, 4, 5, 6, 7, 8, 9, 10)
  }

  test("params follow in key order, and the load priority replaces one") {
    val Seq(indexInfo) = messages(
      encode(indexes =
        Seq(
          index(parameters =
            Map(
              "load_priority" -> "LOW",
              "mmap.enabled" -> "false",
              "index_type" -> "INVERTED"
            )
          )
        )
      ),
      11
    )
    params(indexInfo) shouldBe Seq(
      "index_type" -> "INVERTED",
      "mmap.enabled" -> "false",
      "load_priority" -> "HIGH"
    )
  }

  test("only a pinned V3 manifest with a row count can be loaded") {
    val physical = SegmentLayout.PhysicalColumnGroups(
      Seq(PhysicalColumnGroup(Seq("id"), Seq("files/0.parquet"), Seq(1L))),
      """{"fields":[]}""",
      Map(100L -> "id"),
      Map.empty
    )
    an[IllegalArgumentException] should be thrownBy encode(
      v3Segment(layout = physical)
    )
    an[IllegalArgumentException] should be thrownBy encode(
      v3Segment(storageVersion = 2)
    )
    an[IllegalArgumentException] should be thrownBy encode(
      v3Segment(layout = SegmentLayout.Manifest(basePath))
    )
    an[IllegalArgumentException] should be thrownBy encode(
      v3Segment(rows = None)
    )
    an[IllegalArgumentException] should be thrownBy encode(
      indexes = Seq(index(segmentId = 9L))
    )
  }
}
