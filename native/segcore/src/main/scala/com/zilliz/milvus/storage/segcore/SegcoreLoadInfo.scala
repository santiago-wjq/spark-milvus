package com.zilliz.milvus.storage.segcore

import java.nio.file.Path

import com.fasterxml.jackson.databind.ObjectMapper

import com.zilliz.milvus.storage.snapshot.{Segment, SegmentIndex, SegmentLayout}
import io.milvus.grpc.common.KeyValuePair

import milvus.proto.index.index_coord.IndexStorePathVersion
import milvus.proto.segcore.segcore.{FieldIndexInfo, SegmentLoadInfo}

/** The `segcorepb.SegmentLoadInfo` of a V3 snapshot segment, the way QueryCoord
  * describes a sealed segment to QueryNode: its manifest path, storage version
  * 3, row count and one `FieldIndexInfo` per index to load.
  */
object SegcoreLoadInfo {

  /** QueryCoord appends the load's priority to every index's params under this
    * key, and segcore reads it for each index it loads.
    */
  val LoadPriorityKey = "load_priority"

  /** The priority QueryCoord gives a load a user asks for; the load info's own
    * `priority` field keeps its default, which is also HIGH.
    */
  val LoadPriority = "HIGH"

  private val StorageV3 = 3

  private val mapper = new ObjectMapper()

  /** The load info for `segment` and the `indexes` loaded with it.
    *
    * @param localRoot
    *   None for an object store: segcore's file system and its remote chunk
    *   manager are rooted at the bucket, so keys, which start with the root
    *   path, are given as they are. For local storage, the directory the keys
    *   are relative to: segcore roots local storage at `/`, so every key, the
    *   manifest's and the index files', becomes an absolute path.
    */
  def apply(
      collectionId: Long,
      segment: Segment,
      indexes: Seq[SegmentIndex],
      localRoot: Option[Path]
  ): SegmentLoadInfo = {
    val manifest = segment.layout match {
      case manifest: SegmentLayout.Manifest => manifest
      case _: SegmentLayout.PhysicalColumnGroups =>
        throw new IllegalArgumentException(
          s"segment ${segment.id} has resolved column groups, not a V3 manifest segcore can load"
        )
    }
    require(
      segment.storageVersion == StorageV3,
      s"segment ${segment.id} has storage version ${segment.storageVersion}; segcore loads V3 segments from their manifests"
    )
    require(
      manifest.readVersion >= 0L,
      s"segment ${segment.id} reads manifest version ${manifest.readVersion}; segcore loads a pinned version"
    )
    val rows = segment.rows.getOrElse(
      throw new IllegalArgumentException(
        s"segment ${segment.id} has no row count, which segcore's load info carries"
      )
    )
    indexes.foreach { index =>
      require(
        index.segmentId == segment.id,
        s"index ${index.indexId} belongs to segment ${index.segmentId}, not ${segment.id}"
      )
    }
    def located(key: String): String =
      localRoot.fold(key)(_.resolve(key).toAbsolutePath.toString)

    SegmentLoadInfo(
      segmentID = segment.id,
      partitionID = segment.partitionId,
      collectionID = collectionId,
      numOfRows = rows,
      indexInfos = indexes.map(fieldIndexInfo(_, located)),
      storageVersion = StorageV3.toLong,
      manifestPath =
        manifestPath(manifest.readVersion, located(manifest.basePath))
    )
  }

  /** segcore takes the manifest as the JSON Milvus's `packed` package writes.
    */
  private def manifestPath(version: Long, basePath: String): String = {
    val node = mapper.createObjectNode()
    node.put("ver", version)
    node.put("base_path", basePath)
    mapper.writeValueAsString(node)
  }

  /** The index's own params in key order, then the load priority, which
    * replaces any the params carry: it describes this load, not the index. An
    * absent version is proto3's default, 0, which is not written.
    */
  private def fieldIndexInfo(
      index: SegmentIndex,
      located: String => String
  ): FieldIndexInfo = {
    val params = index.parameters.toSeq
      .filter { case (key, _) => key != LoadPriorityKey }
      .sortBy(_._1) :+ (LoadPriorityKey -> LoadPriority)
    FieldIndexInfo(
      fieldID = index.fieldId,
      indexName = index.name,
      indexID = index.indexId,
      buildID = index.buildId,
      indexParams = params.map { case (key, value) =>
        KeyValuePair(key = key, value = value)
      },
      indexFilePaths = index.filePaths.map(located),
      indexSize = index.serializedSize,
      indexVersion = index.indexVersion,
      numRows = index.rowCount,
      currentIndexVersion = index.currentIndexVersion.getOrElse(0),
      currentScalarIndexVersion = index.currentScalarIndexVersion.getOrElse(0),
      indexStorePathVersion = IndexStorePathVersion.fromValue(
        index.indexStorePathVersion.getOrElse(0)
      )
    )
  }
}
