package com.zilliz.milvus.storage.segcore

import java.io.ByteArrayOutputStream
import java.nio.file.Path

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.CodedOutputStream

import com.zilliz.milvus.storage.snapshot.{Segment, SegmentIndex, SegmentLayout}

/** The `segcorepb.SegmentLoadInfo` of a V3 snapshot segment, the way QueryCoord
  * describes a sealed segment to QueryNode: its manifest path, storage version
  * 3, row count and one `FieldIndexInfo` per index to load.
  *
  * The field numbers are those of Milvus's `pkg/proto/segcore.proto`. Protobuf
  * never gives a field number another meaning, so they hold across `milvus`
  * gitlink moves; the differential test loads what this writes.
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

  private object LoadInfoField {
    val SegmentId = 1
    val PartitionId = 2
    val CollectionId = 3
    val NumOfRows = 7
    val IndexInfos = 11
    val StorageVersion = 15
    val ManifestPath = 21
  }

  private object IndexInfoField {
    val FieldId = 1
    val IndexName = 3
    val IndexId = 4
    val BuildId = 5
    val IndexParams = 6
    val IndexFilePaths = 7
    val IndexSize = 8
    val IndexVersion = 9
    val NumRows = 10
    val CurrentIndexVersion = 11
    val CurrentScalarIndexVersion = 13
    val IndexStorePathVersion = 14
  }

  // common.KeyValuePair
  private val PairKey = 1
  private val PairValue = 2

  private val mapper = new ObjectMapper()

  /** The serialized load info.
    *
    * @param localRoot
    *   None for an object store: segcore's file system and its remote chunk
    *   manager are rooted at the bucket, so keys, which start with the root
    *   path, are given as they are. For local storage, the directory the keys
    *   are relative to: segcore roots local storage at `/`, so every key, the
    *   manifest's and the index files', becomes an absolute path.
    */
  def encode(
      collectionId: Long,
      segment: Segment,
      indexes: Seq[SegmentIndex],
      localRoot: Option[Path]
  ): Array[Byte] = {
    val manifest = segment.layout match {
      case manifest: SegmentLayout.Manifest => manifest
      case _: SegmentLayout.PhysicalColumnGroups =>
        throw new IllegalArgumentException(
          s"segment ${segment.id} has resolved column groups, not a V3 manifest segcore can load"
        )
    }
    require(
      segment.storageVersion == 3,
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

    message { out =>
      out.writeInt64(LoadInfoField.SegmentId, segment.id)
      out.writeInt64(LoadInfoField.PartitionId, segment.partitionId)
      out.writeInt64(LoadInfoField.CollectionId, collectionId)
      out.writeInt64(LoadInfoField.NumOfRows, rows)
      indexes.foreach { index =>
        out.writeByteArray(
          LoadInfoField.IndexInfos,
          fieldIndexInfo(index, located)
        )
      }
      out.writeInt64(LoadInfoField.StorageVersion, 3L)
      out.writeString(
        LoadInfoField.ManifestPath,
        manifestPath(manifest.readVersion, located(manifest.basePath))
      )
    }
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
    * replaces any the params carry: it describes this load, not the index.
    */
  private def fieldIndexInfo(
      index: SegmentIndex,
      located: String => String
  ): Array[Byte] = message { out =>
    out.writeInt64(IndexInfoField.FieldId, index.fieldId)
    out.writeString(IndexInfoField.IndexName, index.name)
    out.writeInt64(IndexInfoField.IndexId, index.indexId)
    out.writeInt64(IndexInfoField.BuildId, index.buildId)
    val params = index.parameters.toSeq
      .filter { case (key, _) => key != LoadPriorityKey }
      .sortBy(_._1) :+ (LoadPriorityKey -> LoadPriority)
    params.foreach { case (key, value) =>
      out.writeByteArray(
        IndexInfoField.IndexParams,
        message { pair =>
          pair.writeString(PairKey, key)
          pair.writeString(PairValue, value)
        }
      )
    }
    index.filePaths.foreach { path =>
      out.writeString(IndexInfoField.IndexFilePaths, located(path))
    }
    out.writeInt64(IndexInfoField.IndexSize, index.serializedSize)
    out.writeInt64(IndexInfoField.IndexVersion, index.indexVersion)
    out.writeInt64(IndexInfoField.NumRows, index.rowCount)
    index.currentIndexVersion.foreach(
      out.writeInt32(IndexInfoField.CurrentIndexVersion, _)
    )
    index.currentScalarIndexVersion.foreach(
      out.writeInt32(IndexInfoField.CurrentScalarIndexVersion, _)
    )
    index.indexStorePathVersion.foreach(
      out.writeEnum(IndexInfoField.IndexStorePathVersion, _)
    )
  }

  private def message(write: CodedOutputStream => Unit): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out = CodedOutputStream.newInstance(bytes)
    write(out)
    out.flush()
    bytes.toByteArray
  }
}
