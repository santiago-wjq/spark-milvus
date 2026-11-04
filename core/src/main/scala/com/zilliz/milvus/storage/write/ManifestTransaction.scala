package com.zilliz.milvus.storage.write

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import io.milvus.storage.{MilvusStorageProperties, MilvusStorageTransaction}

/** Records written column groups in a V3 segment's manifest: one transaction on
  * the latest revision by default, or an explicitly pinned revision for legacy
  * backfill. Concurrent advancement fails instead of replacing a newer base.
  */
object ManifestTransaction {

  sealed trait Change

  /** New files of existing columns: an append to the segment. */
  case object AppendFiles extends Change

  /** Backfill: the groups replace `columns` (manifest column names, which are
    * Milvus field ids). Each column is dropped and the new groups added in the
    * same transaction, so the swap is atomic per column.
    */
  final case class ReplaceColumns(columns: Seq[String]) extends Change

  /** A statistics entry of the manifest: `key` (`bloom_filter.<field id>`), the
    * files under the segment's `_stats/` that hold it, and metadata such as
    * `memory_size`. Committing it replaces any entry of the same key.
    */
  final case class Stat(
      key: String,
      files: Seq[String],
      metadata: Map[String, String]
  )

  /** @return the committed manifest version. */
  def commit(
      basePath: String,
      properties: Map[String, String],
      groups: WrittenColumnGroups,
      change: Change,
      stats: Seq[Stat] = Seq.empty,
      readVersion: Long = -1L
  ): Long = {
    require(
      readVersion == -1L || readVersion > 0L,
      "readVersion must be latest (-1) or a committed manifest version"
    )
    NativeStorageLibrary.load()
    // Ordinary writes read latest. Legacy backfill pins the snapshot revision;
    // FAIL rejects any intervening revision instead of rebasing a stale result.
    val nativeProperties = new MilvusStorageProperties()
    var transaction: MilvusStorageTransaction = null
    val version =
      try {
        nativeProperties.create(properties)
        transaction = new MilvusStorageTransaction()
        transaction.begin(basePath, nativeProperties.getPtr, readVersion, 0, 1)
        change match {
          case AppendFiles =>
            transaction.appendFiles(groups.nativeHandle)
          case ReplaceColumns(columns) =>
            columns.foreach(transaction.dropColumn)
            transaction.addColumnGroups(groups.nativeHandle)
        }
        stats.foreach { stat =>
          transaction.updateStat(
            stat.key,
            stat.files.toArray,
            stat.metadata
          )
        }
        transaction.commit()
      } finally {
        try if (transaction != null) transaction.destroy()
        finally nativeProperties.free()
      }
    if (version < 0) {
      throw new IllegalStateException(
        s"committing the manifest at $basePath failed (version $version)"
      )
    }
    version
  }
}
