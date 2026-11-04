package com.zilliz.spark.connector.backfill

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.TaskContext
import org.slf4j.LoggerFactory

import com.zilliz.milvus.storage.write.WriterResources
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.write.{WriteOptions, WriteSchema}
import io.milvus.grpc.schema.CollectionSchema

/** Distributes merged rows by segment and writes the new columns in snapshot
  * row order. V3 returns receiver-gated delta operations or commits a revision
  * from the pinned snapshot base. Optional snapshot adapters share this same
  * ordering and provenance engine.
  */
private[backfill] object BackfillSegmentWrite {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Deliberately excludes service credentials, input identity and driver
    * configuration.
    */
  private[backfill] final case class WriteContext(
      params: WriteOptions,
      storage: Map[String, String],
      readsSnapshotFields: Boolean,
      receiverCapability: String
  ) extends Serializable

  /** Job-internal columns in the write projection, never user target fields.
    * The marker is added to the backfill side before the left join so we can
    * count how many snapshot rows found a join-key match (non-null marker →
    * matched). Kept on the DataFrame all the way down to the per-segment
    * partition function, and stripped from the projection written to parquet.
    */
  private[backfill] val MatchFlagCol = "__bf_matched__"
  private[backfill] val SegmentIdCol = MilvusOption.MilvusExtraColumnSegmentID
  private[backfill] val RowOffsetCol = MilvusOption.MilvusExtraColumnRowOffset

  /** Per-field flag columns added in any mode that reads snapshot-side target
    * values (coalesce + overwrite). For each new field, one boolean marker
    * records whether the written value came from the snapshot and another
    * whether it came from the backfill data file. Semantics differ per mode:
    *   - coalesce: usedSnapshot = snapshot non-null; usedBf = snapshot null AND
    *     bf non-null
    *   - overwrite: usedSnapshot = join key unmatched; usedBf = join key
    *     matched
    * Used by the writer to tally the per-field `usedSourceByField` /
    * `usedDataFileByField` counts surfaced in `SegmentBackfillResult`.
    */
  private[backfill] def usedSnapshotCol(field: String): String =
    s"__bf_used_src_${field}__"
  private[backfill] def usedBfCol(field: String): String =
    s"__bf_used_bf_${field}__"

  /** Process each segment separately by distributing to Spark executors Each
    * segment is processed by exactly one FFI writer on a single executor
    * Supports multi-partition collections by tracking partition ID per segment
    */
  def write(
      spark: SparkSession,
      joinedDF: DataFrame,
      collectionID: Long,
      segmentToPartitionMap: Map[Long, Long],
      segmentBasePathMap: Map[Long, String],
      segmentSourceVersionMap: Map[Long, Long],
      additionalSegments: BackfillAdditionalSegments,
      config: BackfillConfig,
      newFieldNames: Seq[String],
      collectionSchema: CollectionSchema,
      targetFieldOverrides: Map[
        String,
        StructField
      ]
  ): Either[BackfillError, Map[Long, SegmentBackfillResult]] = {

    try {
      // Prepare data: select only needed columns and add _segment_id for
      // partitioning. Trailing column is the backfill match flag — retained
      // here so executors can count join-key-matched rows, stripped from the
      // projection that reaches the writer. In any mode that reads snapshot fields
      // (coalesce / overwrite), two extra boolean flag columns per new field
      // follow the match flag (usedSnapshot, usedBf interleaved, in
      // `newFieldNames` order) so the writer can compute per-field
      // usedSourceByField / usedDataFileByField counts.
      val readsSnapshotFields = config.readsSnapshotFields
      val flagColNames: Seq[String] =
        if (readsSnapshotFields)
          newFieldNames.flatMap(n => Seq(usedSnapshotCol(n), usedBfCol(n)))
        else Seq.empty
      val preparedDF = joinedDF
        .select(
          (Seq(SegmentIdCol, RowOffsetCol) ++ newFieldNames ++ Seq(
            MatchFlagCol
          ) ++ flagColNames).map(col): _*
        )

      // Get the schema for new fields only (without _segment_id, _row_offset,
      // or the match flag)
      // The write schema: the new columns checked against the collection and
      // carrying their field ids and dimensions, which is what names the
      // columns in the written files.
      val targetSchema =
        try
          WriteSchema.resolve(
            StructType(
              newFieldNames.map(fieldName =>
                targetFieldOverrides.getOrElse(
                  fieldName,
                  preparedDF.schema.fields.find(_.name == fieldName).get
                )
              )
            ),
            collectionSchema,
            WriteSchema.Mode.Columns
          )
        catch {
          case e: IllegalArgumentException =>
            return Left(SchemaValidationError(e.getMessage))
        }

      val segmentIds = segmentToPartitionMap.keys.toArray
      if (segmentIds.isEmpty) return Right(Map.empty)
      val context = WriteContext(
        WriteOptions(
          batchRows = config.batchSize,
          replaceColumns = true
        ),
        config.targetStorageOptions,
        config.readsSnapshotFields,
        config.receiverCapability
      )
      val segmentPartitioner = new SegmentPartitioner(segmentIds)

      // Repartition using custom partitioner, then sort by _row_offset within each partition
      // CRITICAL: .copy() is required because queryExecution.toRdd produces an iterator
      // that reuses the same UnsafeRow buffer. Without copy, keyBy/partitionBy's
      // ExternalSorter stores references to the same mutable buffer, causing all
      // rows to contain the last row's data.
      val repartitionedRDD = preparedDF.queryExecution.toRdd
        .map(_.copy()) // Materialize each row to avoid UnsafeRow reuse
        .keyBy(_.getLong(0)) // _segment_id is at index 0
        .partitionBy(segmentPartitioner)
        .values
        .mapPartitions(iter =>
          iter.toSeq.sortBy(_.getLong(1)).iterator
        ) // Sort by _row_offset

      // Broadcast configuration to executors
      val broadcastContext = spark.sparkContext.broadcast(context)
      val broadcastCollectionID = spark.sparkContext.broadcast(collectionID)
      val broadcastSegmentToPartitionMap =
        spark.sparkContext.broadcast(segmentToPartitionMap)
      val broadcastSegmentBasePathMap =
        spark.sparkContext.broadcast(segmentBasePathMap)
      val broadcastSegmentSourceVersionMap =
        spark.sparkContext.broadcast(segmentSourceVersionMap)
      val broadcastAdditionalSegments =
        spark.sparkContext.broadcast(additionalSegments)
      val broadcastTargetSchema = spark.sparkContext.broadcast(targetSchema)

      val results = WriterResources.closing {
        WriterResources.closeAll(
          null,
          () => broadcastContext.destroy(),
          () => broadcastCollectionID.destroy(),
          () => broadcastSegmentToPartitionMap.destroy(),
          () => broadcastSegmentBasePathMap.destroy(),
          () => broadcastSegmentSourceVersionMap.destroy(),
          () => broadcastAdditionalSegments.destroy(),
          () => broadcastTargetSchema.destroy()
        )
      } {
        repartitionedRDD
          .mapPartitions { iter =>
            if (!iter.hasNext) Iterator.empty
            else
              processSegmentPartition(
                iter,
                broadcastContext.value,
                broadcastCollectionID.value,
                broadcastSegmentToPartitionMap.value,
                broadcastSegmentBasePathMap.value,
                broadcastSegmentSourceVersionMap.value,
                broadcastAdditionalSegments.value,
                broadcastTargetSchema.value
              )
          }
          .collect()

      }

      // Check for failures
      val failures = results.filter(_._2.isDefined)
      if (failures.nonEmpty) {
        val firstFailure = failures.head
        val error = firstFailure._2.get
        return Left(
          WriteError(
            segmentId = firstFailure._1.segmentId,
            outputPath = firstFailure._1.outputPath,
            message =
              s"Failed to write ${failures.length} segment(s): ${error.getMessage}",
            cause = Some(error)
          )
        )
      }

      // Extract successful results
      val successfulResults = results.map { case (result, _) =>
        result.segmentId -> result
      }.toMap

      // Log summary statistics
      val totalTime = results.map(_._1.executionTimeMs).sum
      val avgTime = if (results.nonEmpty) totalTime / results.length else 0
      val totalRows = results.map(_._1.rowCount).sum
      val totalSnapshotRows = results.map(_._1.sourceRowCount).sum
      val totalMatched = results.map(_._1.matchedRowCount).sum
      val matchRateStr =
        if (totalSnapshotRows > 0)
          f"${totalMatched.toDouble / totalSnapshotRows * 100}%.2f%%"
        else "n/a"

      logger.info("=== Backfill Summary ===")
      logger.info(s"Total segments: ${results.length}")
      logger.info(s"Total source rows: $totalSnapshotRows")
      logger.info(
        s"Total matched rows: $totalMatched (match rate: $matchRateStr)"
      )
      logger.info(s"Total rows written: $totalRows")
      logger.info(s"Total time for all segments: ${totalTime}ms")
      logger.info(s"Average time per segment: ${avgTime}ms")
      if (readsSnapshotFields) {
        logger.info(
          s"Per-field provenance (mode=${config.mode}, aggregated):"
        )
        newFieldNames.foreach { f =>
          val snapshotCount =
            results.map(_._1.usedSourceByField.getOrElse(f, 0L)).sum
          val df = results.map(_._1.usedDataFileByField.getOrElse(f, 0L)).sum
          val nullOut = totalSnapshotRows - snapshotCount - df
          logger.info(
            s"  $f: source=$snapshotCount, dataFile=$df, null=$nullOut"
          )
        }
      }
      results.sortBy(_._1.segmentId).foreach { case (r, _) =>
        val segRate =
          if (r.sourceRowCount > 0)
            f"${r.matchedRowCount.toDouble / r.sourceRowCount * 100}%.2f%%"
          else "n/a"
        logger.info(
          s"Segment ${r.segmentId}: source=${r.sourceRowCount}, matched=${r.matchedRowCount} ($segRate), written=${r.rowCount}"
        )
      }

      Right(successfulResults)

    } catch {
      case e: Exception =>
        logger.error("Failed to process segments", e)
        Left(
          SegmentProcessingError(
            segmentId = -1,
            message = s"Failed to process segments: ${e.getMessage}",
            cause = Some(e)
          )
        )
    }
  }

  /** The writer of one segment's new column groups, built on the executor. The
    * rows go into the segment's own directory. The explicit `fs.*` of `storage`
    * is the whole storage configuration. The caller owns publishing the
    * returned result to a receiver.
    */
  private[backfill] def segmentWriter(
      targetSchema: StructType,
      params: WriteOptions,
      storage: Map[String, String],
      basePath: String,
      sourceVersion: Long,
      receiverCapability: String
  ): DataWriter[InternalRow] = {
    val task = TaskContext.get()
    require(task != null, "segment writers must run inside a Spark task")
    new BackfillV3Writer(
      targetSchema,
      params,
      storage,
      basePath,
      sourceVersion,
      receiverCapability
    )
  }

  private def processSegmentPartition(
      iter: Iterator[InternalRow],
      context: WriteContext,
      collectionID: Long,
      segmentToPartitionMap: Map[Long, Long],
      segmentBasePathMap: Map[Long, String],
      segmentSourceVersionMap: Map[Long, Long],
      additionalSegments: BackfillAdditionalSegments,
      targetSchema: StructType
  ): Iterator[(SegmentBackfillResult, Option[Throwable])] = {

    val firstRow = iter.next()
    val segmentID = firstRow.getLong(0)
    val partitionID = segmentToPartitionMap(segmentID)
    val startTime = System.currentTimeMillis()

    val additional = additionalSegments.partitions.contains(segmentID)
    val outputPath =
      if (additional)
        additionalSegments.outputPath(
          collectionID,
          partitionID,
          segmentID,
          context.storage
        )
      else
        segmentBasePathMap.getOrElse(
          segmentID,
          throw new IllegalArgumentException(
            s"Missing V3 base path for segment $segmentID"
          )
        )
    val writer =
      if (additional)
        additionalSegments.writer(
          collectionID,
          partitionID,
          segmentID,
          targetSchema,
          context.params,
          context.storage
        )
      else
        segmentWriter(
          targetSchema,
          context.params,
          context.storage,
          outputPath,
          segmentSourceVersionMap.getOrElse(
            segmentID,
            throw new IllegalArgumentException(
              s"Missing snapshot manifest version for segment $segmentID"
            )
          ),
          context.receiverCapability
        )

    try {
      val counts = writeRows(
        Iterator.single(firstRow) ++ iter,
        targetSchema,
        context.readsSnapshotFields
      )(writer.write)

      val commitMessage = writer.commit()
      val (
        committedVersion,
        sourceVersion,
        manifestOps,
        manifestPaths,
        artifact
      ) = commitMessage match {
        case msg: BackfillV3CommitMessage =>
          (
            msg.committedVersion,
            msg.sourceVersion,
            msg.manifestOps,
            if (msg.committedVersion > 0L) Seq(outputPath) else Seq.empty,
            Option.empty[BackfillSegmentArtifact]
          )
        case msg: BackfillAdapterCommitMessage =>
          (
            -1L,
            Option.empty[Long],
            Seq.empty[BackfillManifestOp],
            msg.manifestPaths,
            Some(msg.artifact)
          )
        case other =>
          throw new IllegalStateException(
            s"Unexpected segment commit message: $other"
          )
      }

      writer.close()

      Iterator.single(
        (
          SegmentBackfillResult(
            segmentId = segmentID,
            rowCount = counts.rows,
            manifestPaths = manifestPaths,
            outputPath = outputPath,
            executionTimeMs = System.currentTimeMillis() - startTime,
            committedVersion = committedVersion,
            sourceVersion = sourceVersion,
            manifestOps = manifestOps,
            columnGroupArtifact = artifact,
            sourceRowCount = counts.rows,
            matchedRowCount = counts.matched,
            usedSourceByField = counts.usedSnapshot,
            usedDataFileByField = counts.usedBackfill
          ),
          None
        )
      )

    } catch {
      case e: Exception =>
        WriterResources.closeAll(e, () => writer.abort(), () => writer.close())
        Iterator.single(
          (
            SegmentBackfillResult(
              segmentId = segmentID,
              rowCount = 0,
              manifestPaths = Seq.empty,
              outputPath = outputPath,
              executionTimeMs = System.currentTimeMillis() - startTime
            ),
            Some(e)
          )
        )
    }
  }

  private[backfill] final case class RowCounts(
      rows: Long,
      matched: Long,
      usedSnapshot: Map[String, Long],
      usedBackfill: Map[String, Long]
  )

  /** Strip the internal columns and count provenance for either storage
    * version. The input is already in snapshot row order. Count a written row
    * only after the writer accepts it; a write failure propagates to that
    * writer's abort.
    */
  private[backfill] def writeRows(
      rows: Iterator[InternalRow],
      schema: StructType,
      readsSnapshotFields: Boolean
  )(write: InternalRow => Unit): RowCounts = {
    val fields = schema.fields
    val fieldIndices = fields.indices
    val matchFlag = 2 + fields.length
    val usedSnapshot = Array.fill(fields.length)(0L)
    val usedBackfill = Array.fill(fields.length)(0L)
    var written = 0L
    var matched = 0L
    rows.foreach { row =>
      val values =
        fieldIndices.map(i => row.get(i + 2, fields(i).dataType)).toArray
      if (!row.isNullAt(matchFlag)) matched += 1
      if (readsSnapshotFields) {
        var i = 0
        while (i < fields.length) {
          val snapshotFlag = matchFlag + 1 + 2 * i
          val backfillFlag = snapshotFlag + 1
          if (!row.isNullAt(snapshotFlag) && row.getBoolean(snapshotFlag))
            usedSnapshot(i) += 1
          if (!row.isNullAt(backfillFlag) && row.getBoolean(backfillFlag))
            usedBackfill(i) += 1
          i += 1
        }
      }
      write(new GenericInternalRow(values))
      written += 1
    }
    val names = schema.fieldNames
    RowCounts(
      written,
      matched,
      if (readsSnapshotFields) names.zip(usedSnapshot).toMap else Map.empty,
      if (readsSnapshotFields) names.zip(usedBackfill).toMap else Map.empty
    )
  }
}
