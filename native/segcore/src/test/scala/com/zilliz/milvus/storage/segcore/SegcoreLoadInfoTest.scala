package com.zilliz.milvus.storage.segcore

import java.nio.file.Paths

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
import io.milvus.grpc.common.KeyValuePair

import milvus.proto.index.index_coord.IndexStorePathVersion
import milvus.proto.segcore.segcore.{FieldIndexInfo, SegmentLoadInfo}

/** The load info as segcore parses it: written, serialized and read back
  * through the messages generated from Milvus's `pkg/proto/segcore.proto`.
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
      indexStorePathVersion: Option[Int] = Some(1),
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

  /** What segcore receives: the serialized message, parsed again. */
  private def loadInfo(
      segment: Segment = v3Segment(),
      indexes: Seq[SegmentIndex] = Seq(index()),
      localRoot: Option[String] = None
  ): SegmentLoadInfo =
    SegmentLoadInfo.parseFrom(
      SegcoreLoadInfo(
        3L,
        segment,
        indexes,
        localRoot.map(Paths.get(_))
      ).toByteArray
    )

  /** manifest_path: the JSON of Milvus's packed manifest reference. */
  private def manifest(info: SegmentLoadInfo): (Long, String) = {
    val node = new ObjectMapper().readTree(info.manifestPath)
    (node.get("ver").asLong, node.get("base_path").asText)
  }

  private def params(info: FieldIndexInfo): Seq[(String, String)] =
    info.indexParams.map(pair => pair.key -> pair.value)

  test("a V3 segment carries its identity, rows, manifest and indexes") {
    val info = loadInfo()
    info.segmentID shouldBe 1L
    info.partitionID shouldBe 2L
    info.collectionID shouldBe 3L
    info.numOfRows shouldBe 2048L
    info.storageVersion shouldBe 3L
    manifest(info) shouldBe (4L, basePath)
    info.binlogPaths shouldBe empty
    info.deltalogs shouldBe empty

    val Seq(indexInfo) = info.indexInfos
    indexInfo.fieldID shouldBe 105L
    indexInfo.indexName shouldBe "i64_inverted"
    indexInfo.indexID shouldBe 11L
    indexInfo.buildID shouldBe 12L
    params(indexInfo) shouldBe Seq(
      "index_type" -> "INVERTED",
      "load_priority" -> "HIGH"
    )
    indexInfo.indexFilePaths shouldBe Seq(indexPath)
    indexInfo.indexSize shouldBe 42072L
    indexInfo.indexVersion shouldBe 1L
    indexInfo.numRows shouldBe 2048L
    indexInfo.currentIndexVersion shouldBe 10
    indexInfo.currentScalarIndexVersion shouldBe 3
    indexInfo.indexStorePathVersion shouldBe
      IndexStorePathVersion.INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED
  }

  test("a segment loaded without indexes lists none") {
    loadInfo(indexes = Seq.empty).indexInfos shouldBe empty
  }

  test("local storage makes every key absolute") {
    val info = loadInfo(localRoot = Some("/data/fixture"))
    manifest(info) shouldBe (4L, s"/data/fixture/$basePath")
    info.indexInfos.head.indexFilePaths shouldBe Seq(
      s"/data/fixture/$indexPath"
    )
  }

  test("an absent version is not written") {
    val written = SegcoreLoadInfo(
      3L,
      v3Segment(),
      Seq(
        index(
          currentIndexVersion = None,
          currentScalarIndexVersion = None,
          indexStorePathVersion = None
        )
      ),
      None
    ).indexInfos.head
    val fields = UnknownFieldSet.parseFrom(written.toByteArray)
    Seq(
      FieldIndexInfo.CURRENT_INDEX_VERSION_FIELD_NUMBER,
      FieldIndexInfo.CURRENT_SCALAR_INDEX_VERSION_FIELD_NUMBER,
      FieldIndexInfo.INDEX_STORE_PATH_VERSION_FIELD_NUMBER
    ).foreach(number => fields.hasField(number) shouldBe false)
    fields.hasField(FieldIndexInfo.INDEX_VERSION_FIELD_NUMBER) shouldBe true
  }

  test("params follow in key order, and the load priority replaces one") {
    val info = loadInfo(indexes =
      Seq(
        index(parameters =
          Map(
            "load_priority" -> "LOW",
            "mmap.enabled" -> "false",
            "index_type" -> "INVERTED"
          )
        )
      )
    )
    params(info.indexInfos.head) shouldBe Seq(
      "index_type" -> "INVERTED",
      "mmap.enabled" -> "false",
      "load_priority" -> "HIGH"
    )
    info.indexInfos.head.indexParams.last shouldBe KeyValuePair(
      key = SegcoreLoadInfo.LoadPriorityKey,
      value = SegcoreLoadInfo.LoadPriority
    )
  }

  test("only a pinned V3 manifest with a row count can be loaded") {
    val physical = SegmentLayout.PhysicalColumnGroups(
      Seq(PhysicalColumnGroup(Seq("id"), Seq("files/0.parquet"), Seq(1L))),
      """{"fields":[]}""",
      Map(100L -> "id"),
      Map.empty
    )
    an[IllegalArgumentException] should be thrownBy loadInfo(
      v3Segment(layout = physical)
    )
    an[IllegalArgumentException] should be thrownBy loadInfo(
      v3Segment(storageVersion = 2)
    )
    an[IllegalArgumentException] should be thrownBy loadInfo(
      v3Segment(layout = SegmentLayout.Manifest(basePath))
    )
    an[IllegalArgumentException] should be thrownBy loadInfo(
      v3Segment(rows = None)
    )
    an[IllegalArgumentException] should be thrownBy loadInfo(
      indexes = Seq(index(segmentId = 9L))
    )
  }
}
