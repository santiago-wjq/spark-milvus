package com.zilliz.spark.connector.backfill

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.manifest.SnapshotSegmentReader
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.schema.SchemaMapper
import com.zilliz.milvus.storage.snapshot.json.{
  CollectionSchemaJson,
  FieldJson,
  ManifestContentJson,
  SegmentListJson,
  SnapshotJson
}
import com.zilliz.milvus.storage.write.WriterResources
import com.zilliz.spark.connector.backfill.BackfillSegmentWrite.{
  usedBfCol,
  usedSnapshotCol,
  MatchFlagCol,
  RowOffsetCol,
  SegmentIdCol
}
import com.zilliz.spark.connector.input.{
  InputAdapter,
  InputOptions,
  InputSource
}
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.table.SnapshotSparkSchema
import io.milvus.grpc.schema.{CollectionSchema, DataType => MilvusDataType}

/** Backfill operation for Milvus collections
  *
  * This object provides functionality to backfill new fields into existing
  * Milvus collections by reading snapshot rows, joining with new field data,
  * and writing per-segment binlog files.
  */
object MilvusBackfill {

  private sealed trait BackfillInput
  private final case class PathInput(path: String, config: BackfillConfig)
      extends BackfillInput
  private final case class FrameInput(frame: DataFrame) extends BackfillInput

  private[backfill] def resolveResultPartitionId(
      partitionIDs: Set[Long]
  ): Long =
    if (partitionIDs.size == 1) partitionIDs.head else 0L

  private val logger = LoggerFactory.getLogger(getClass)

  private[backfill] val ApplyDeletesToSnapshotRows = false

  private val SupportedPhysicalJoinMilvusTypes: Set[Int] = Set(
    MilvusDataType.Int8.value,
    MilvusDataType.Int16.value,
    MilvusDataType.Int32.value,
    MilvusDataType.Int64.value,
    MilvusDataType.String.value,
    MilvusDataType.VarChar.value
  )

  private val SystemFieldIdsByName: Map[String, Long] = Map(
    "RowID" -> SchemaMapper.RowIdFieldId,
    "Timestamp" -> SchemaMapper.TimestampFieldId
  )

  private val ReservedJoinKeyFieldNames: Set[String] = Set(
    "$segment_id",
    "$row_offset",
    MilvusOption.MilvusExtraColumnSegmentID,
    MilvusOption.MilvusExtraColumnRowOffset
  )

  /** Resolve the public join-key specification against an exact snapshot schema
    * field. Physical keys deliberately support only stable scalar equality
    * types and never rely on Spark's case-insensitive resolver.
    */
  private[backfill] def resolveJoinKey(
      schema: CollectionSchemaJson,
      spec: BackfillJoinKey
  ): Either[BackfillError, ResolvedJoinKey] = {
    val normalizedSpec = spec match {
      case BackfillJoinKey.PhysicalField(name) =>
        BackfillJoinKey.PhysicalField(Option(name).map(_.trim).getOrElse(""))
      case BackfillJoinKey.PrimaryKey => BackfillJoinKey.PrimaryKey
    }

    val selected = normalizedSpec match {
      case BackfillJoinKey.PrimaryKey =>
        schema.fields
          .find(_.isPrimaryKey.contains(true))
          .toRight(
            SchemaValidationError(
              "No primary key field found in snapshot schema"
            )
          )
      case BackfillJoinKey.PhysicalField(name) if name.isEmpty =>
        Left(
          SchemaValidationError("Physical join-key field name cannot be blank")
        )
      case BackfillJoinKey.PhysicalField(name) =>
        schema.fields
          .find(_.name == name)
          .toRight(
            SchemaValidationError(
              s"Physical join-key field '$name' was not found in snapshot schema. " +
                s"Available fields: ${schema.fields.map(_.name).mkString(", ")}"
            )
          )
    }

    selected.flatMap { field =>
      normalizedSpec match {
        case _ if ReservedJoinKeyFieldNames.contains(field.name) =>
          Left(
            SchemaValidationError(
              "Snapshot field '" + field.name + "' cannot be used as the backfill join key because " +
                "its name is reserved for required metadata columns '" +
                MilvusOption.MilvusExtraColumnSegmentID + "' and '" +
                MilvusOption.MilvusExtraColumnRowOffset + "'. " +
                "Choose another persisted field with --join-key."
            )
          )
        case _: BackfillJoinKey.PhysicalField
            if !SupportedPhysicalJoinMilvusTypes.contains(field.dataType) =>
          Left(
            SchemaValidationError(
              s"Physical join-key field '${field.name}' has unsupported type " +
                s"(Milvus type ${field.dataType}). Supported types are Int8, Int16, Int32, " +
                "Int64, String, and VarChar."
            )
          )
        case _: BackfillJoinKey.PhysicalField
            if field.nullable.contains(true) =>
          Left(
            SchemaValidationError(
              s"Physical join-key field '${field.name}' is nullable. " +
                "Physical join keys must be non-nullable because Spark equality joins do not match NULL values."
            )
          )
        case _ =>
          val sparkFieldResult =
            try {
              Right(SnapshotSparkSchema.fieldToStructField(field))
            } catch {
              case e: Exception =>
                Left(
                  SchemaValidationError(
                    s"Failed to resolve join-key field '${field.name}': ${e.getMessage}",
                    Some(e)
                  )
                )
            }

          sparkFieldResult.map { sparkField =>
            normalizedSpec match {
              case BackfillJoinKey.PrimaryKey =>
                ResolvedJoinKey.primaryKey(
                  field.name,
                  field.getFieldIDAsLong,
                  Some(sparkField)
                )
              case _: BackfillJoinKey.PhysicalField =>
                ResolvedJoinKey.physicalField(
                  field.name,
                  field.getFieldIDAsLong,
                  sparkField
                )
            }
          }
      }
    }
  }

  /** Resolve requested writer targets from the snapshot schema and reject
    * fields whose values are owned by Milvus rather than by backfill output.
    * This shared boundary protects both primary-key and physical-key mapping
    * paths before source reads or writes begin.
    */
  private[backfill] def resolveBackfillTargetFields(
      schema: CollectionSchemaJson,
      targetFieldNames: Seq[String]
  ): Either[BackfillError, Map[String, FieldJson]] = {
    val fieldsByName = schema.fields.map(field => field.name -> field).toMap
    val missing = targetFieldNames.filterNot(fieldsByName.contains)
    if (missing.nonEmpty) {
      Left(
        SchemaValidationError(
          s"Fields not found in snapshot schema: ${missing.mkString(", ")}. " +
            s"Available fields: ${fieldsByName.keys.mkString(", ")}"
        )
      )
    } else {
      val resolved = targetFieldNames.map(name => name -> fieldsByName(name))
      val unsupported = resolved.flatMap { case (name, field) =>
        val isSystemField =
          SystemFieldIdsByName.get(name).contains(field.getFieldIDAsLong)
        Seq(
          if (field.isPrimaryKey.contains(true)) Some("primary key") else None,
          if (field.isPartitionKey.contains(true)) Some("partition key")
          else None,
          if (field.isDynamic.contains(true)) Some("dynamic field") else None,
          if (field.isFunctionOutput.contains(true)) Some("function output")
          else None,
          if (isSystemField) Some("system field") else None
        ).flatten.map(role => s"'$name' ($role)")
      }

      if (unsupported.nonEmpty) {
        Left(
          SchemaValidationError(
            "Backfill targets must be ordinary writable collection fields; " +
              s"unsupported targets: ${unsupported.mkString(", ")}"
          )
        )
      } else {
        Right(resolved.toMap)
      }
    }
  }

  /** Suffix appended to backfill-side target columns while both sides of a
    * target field are live on the joined frame (coalesce + overwrite).
    */
  private[backfill] val BackfillSideSuffix = "__bf__"

  /** Every column name the join/merge pipeline synthesizes on the joined frame
    * for the given targets and resolved join key. Kept in one place so the
    * pre-read reservation check below cannot drift from the generators.
    */
  private[backfill] def reservedInternalColumnNames(
      newFieldNames: Seq[String],
      joinKey: ResolvedJoinKey
  ): Seq[String] =
    (Seq(MatchFlagCol) ++ joinKey.internalColumns ++ newFieldNames.flatMap {
      n => Seq(usedSnapshotCol(n), usedBfCol(n), n + BackfillSideSuffix)
    }).distinct

  /** Reject target names that resolve (under Spark's configured resolver,
    * case-insensitive by default) to any internal column generated later. A
    * colliding target would either be silently overwritten by marker data or
    * make the merge projection ambiguous, so fail before any source read.
    */
  private[backfill] def validateTargetNamesAgainstInternalColumns(
      newFieldNames: Seq[String],
      joinKey: ResolvedJoinKey,
      resolver: (String, String) => Boolean
  ): Either[BackfillError, Unit] = {
    val reserved = reservedInternalColumnNames(newFieldNames, joinKey)
    val collisions = newFieldNames.flatMap { name =>
      reserved.collect {
        case internal if resolver(name, internal) =>
          s"'$name' (reserved as '$internal')"
      }
    }
    if (collisions.nonEmpty) {
      Left(
        SchemaValidationError(
          "Backfill target columns collide with names reserved for internal " +
            s"use by the backfill join: ${collisions.mkString(", ")}. " +
            "Rename the column (or use --column-mapping) and retry."
        )
      )
    } else {
      Right(())
    }
  }

  /** Backfill new fields into a Milvus collection
    *
    * @param spark
    *   SparkSession
    * @param backfillDataPath
    *   Path to Parquet file containing the configured join key and new field
    *   data
    * @param snapshotPath
    *   Path to Milvus snapshot metadata JSON file
    * @param config
    *   Backfill configuration
    * @return
    *   Either error or successful result
    */
  def run(
      spark: SparkSession,
      backfillDataPath: String,
      snapshotPath: String,
      initialConfig: BackfillConfig,
      snapshotAdapter: BackfillSnapshotAdapter = BackfillSnapshotAdapter.V3Only
  ): Either[BackfillError, BackfillResult] = {
    val validatedConfig = validateConfig(spark, initialConfig) match {
      case Left(error)   => return Left(error)
      case Right(config) => config
    }
    if (Option(snapshotPath).forall(_.trim.isEmpty)) {
      return Left(
        SchemaValidationError(
          "ADDFIELD backfill requires collection schema from a snapshot"
        )
      )
    }
    resolveValidatedConfig(spark, validatedConfig).flatMap { config =>
      runInternal(
        spark,
        PathInput(backfillDataPath, initialConfig),
        snapshotPath,
        config,
        collectionFieldNames = false,
        expectedSnapshot = None,
        beforeWrite = () => (),
        snapshotAdapter = snapshotAdapter
      )
    }
  }

  /** Borrow an existing DataFrame and leave its session, source and caches
    * owned by the caller. Column names are resolved against the snapshot
    * schema.
    */
  def run(
      input: DataFrame,
      snapshotPath: String,
      initialConfig: BackfillConfig
  ): Either[BackfillError, BackfillResult] = {
    if (input == null || input.isStreaming)
      return Left(
        SchemaValidationError("Backfill requires a non-streaming DataFrame")
      )
    if (Option(snapshotPath).forall(_.trim.isEmpty))
      return Left(
        SchemaValidationError("Backfill requires an existing snapshot path")
      )
    bindConfig(input.sparkSession, initialConfig).flatMap { config =>
      runInternal(
        input.sparkSession,
        FrameInput(input),
        snapshotPath,
        config,
        collectionFieldNames = true,
        expectedSnapshot = None,
        beforeWrite = () => (),
        snapshotAdapter = BackfillSnapshotAdapter.V3Only
      )
    }
  }

  /** Bind the target once while retaining the caller's independent input
    * identity.
    */
  def bindConfig(
      spark: SparkSession,
      initialConfig: BackfillConfig
  ): Either[BackfillError, BackfillConfig] =
    validateConfig(spark, initialConfig, includeInput = false).flatMap(
      resolveValidatedConfig(spark, _)
    )

  private def validateConfig(
      spark: SparkSession,
      initialConfig: BackfillConfig,
      includeInput: Boolean = true
  ): Either[BackfillError, BackfillConfig] = {
    val validatedConfig =
      try {
        initialConfig.withHadoopStorageAssumeRole(
          spark.sparkContext.hadoopConfiguration,
          spark.sparkContext.applicationId
        )
      } catch {
        case e: IllegalArgumentException =>
          return Left(
            SchemaValidationError(
              s"Invalid Hadoop storage AssumeRole configuration: ${e.getMessage}"
            )
          )
      }

    // Validate S3/writer configuration (always required)
    val validation =
      if (includeInput) validatedConfig.validate()
      else validatedConfig.validateTarget()
    validation match {
      case Left(error) =>
        Left(SchemaValidationError(s"Invalid configuration: $error"))
      case Right(_) => Right(validatedConfig)
    }
  }

  private def resolveValidatedConfig(
      spark: SparkSession,
      validatedConfig: BackfillConfig
  ): Either[BackfillError, BackfillConfig] = {
    try
      Right(
        validatedConfig.copy(storageProperties =
          validatedConfig.resolveStorage(spark.sessionState.newHadoopConf())
        )
      )
    catch {
      case e: IllegalArgumentException =>
        Left(
          SchemaValidationError(
            s"Invalid storage configuration: ${e.getMessage}"
          )
        )
    }
  }

  /** Uses an already resolved storage identity and validates the expected
    * collection/snapshot before writing. Input columns use collection field
    * names. The callback runs after validation, immediately before writing.
    */
  def runPrepared(
      spark: SparkSession,
      backfillDataPath: String,
      snapshotPath: String,
      inputConfig: BackfillConfig,
      resolvedConfig: BackfillConfig,
      expectedCollectionId: Long,
      expectedSnapshotName: String,
      beforeWrite: () => Unit,
      snapshotAdapter: BackfillSnapshotAdapter
  ): Either[BackfillError, BackfillResult] =
    runInternal(
      spark,
      PathInput(backfillDataPath, inputConfig),
      snapshotPath,
      resolvedConfig,
      collectionFieldNames = true,
      expectedSnapshot = Some(expectedCollectionId -> expectedSnapshotName),
      beforeWrite = beforeWrite,
      snapshotAdapter = snapshotAdapter
    )

  /** Borrow a caller's resolved Spark plan without reopening or closing its
    * source.
    */
  def runPrepared(
      input: DataFrame,
      snapshotPath: String,
      resolvedConfig: BackfillConfig,
      expectedCollectionId: Long,
      expectedSnapshotName: String,
      beforeWrite: () => Unit,
      snapshotAdapter: BackfillSnapshotAdapter
  ): Either[BackfillError, BackfillResult] = {
    if (input == null || input.isStreaming)
      return Left(
        SchemaValidationError("Backfill requires a non-streaming DataFrame")
      )
    runInternal(
      input.sparkSession,
      FrameInput(input),
      snapshotPath,
      resolvedConfig,
      collectionFieldNames = true,
      expectedSnapshot = Some(expectedCollectionId -> expectedSnapshotName),
      beforeWrite = beforeWrite,
      snapshotAdapter = snapshotAdapter
    )
  }

  private def runInternal(
      spark: SparkSession,
      input: BackfillInput,
      snapshotPath: String,
      config: BackfillConfig,
      collectionFieldNames: Boolean,
      expectedSnapshot: Option[(Long, String)],
      beforeWrite: () => Unit,
      snapshotAdapter: BackfillSnapshotAdapter
  ): Either[BackfillError, BackfillResult] = {
    val startTime = System.currentTimeMillis()
    val runConfig = config

    var inputSnapshotId: Option[Long] = None
    var cachedBackfillDF: DataFrame = null
    var inputSource: InputSource = null

    logger.info(s"Backfill mode: ${config.mode}")

    val snapshotMetadata = loadSnapshotMetadata(snapshotPath, config) match {
      case Left(error)     => return Left(error)
      case Right(metadata) => metadata
    }

    expectedSnapshot.foreach { case (collectionId, snapshotName) =>
      if (
        collectionId <= 0L || snapshotMetadata.snapshotInfo.collectionId != collectionId
      )
        return Left(
          SchemaValidationError(
            "Snapshot metadata collection ID does not match the requested collection"
          )
        )
      if (
        Option(snapshotName).forall(
          _.trim.isEmpty
        ) || snapshotMetadata.snapshotInfo.name != snapshotName
      )
        return Left(
          SchemaValidationError(
            "Snapshot metadata name does not match the job-owned snapshot"
          )
        )
    }

    // Resolve optional additional formats once, sharing authoritative physical
    // layouts between the read and write paths.
    val loaded =
      loadSnapshotSegments(snapshotMetadata, config, snapshotAdapter) match {
        case Left(error)     => return Left(error)
        case Right(segments) => segments
      }
    val (additionalSegments, authoritativePartitions, deleteOnlySegments) =
      loaded
    val physicalSnapshot = snapshotMetadata.copy(storageV2ManifestList =
      snapshotMetadata.storageV2ManifestList.map(
        _.filterNot(item => deleteOnlySegments(item.segmentID))
      )
    )

    def execute(): Either[BackfillError, BackfillResult] = {
      val baseJoinKey =
        resolveJoinKey(
          snapshotMetadata.collection.schema,
          config.joinKey
        ) match {
          case Left(error) => return Left(error)
          case Right(key)  => key
        }
      logger.info(
        s"Backfill join key: kind=${baseJoinKey.kind}, " +
          s"fields=${baseJoinKey.fieldNames.mkString(",")}, " +
          s"fieldIds=${baseJoinKey.fieldIds.mkString(",")}"
      )

      val rawBackfillDF = input match {
        case FrameInput(frame) => frame
        case PathInput(path, inputConfig) =>
          readBackfillData(spark, path, inputConfig) match {
            case Left(error) => return Left(error)
            case Right(source) =>
              inputSource = source
              inputSnapshotId = source.icebergSnapshotId
              source.dataFrame
          }
      }

      // Reproject via the selected join-key mapping contract, then separate
      // the join component from fields that will be written. The join column is
      // normalized to an internal alias so downstream execution no longer
      // depends on the collection field name.
      val preparedBackfill = prepareBackfillData(
        rawBackfillDF,
        baseJoinKey,
        config.joinKey,
        managedColumnMapping(
          rawBackfillDF.columns.toSeq,
          config.columnMapping,
          collectionFieldNames
        ),
        snapshotMetadata.collection.schema.fields.map(_.name)
      ) match {
        case Left(error) => return Left(error)
        case Right(data) => data
      }
      val mappedBackfillDF = preparedBackfill.dataFrame
      val joinKey = preparedBackfill.joinKey

      val newFieldNames = preparedBackfill.targetFieldNames

      // Reserve every internal column the join/merge step will synthesize
      // (match flag, join aliases, per-field provenance flags, backfill-side
      // suffix) under Spark's configured resolver. A colliding target would be
      // silently overwritten by marker data (overwrite mode) or blow up with
      // AnalysisException on the coalesce rename/self-reference.
      validateTargetNamesAgainstInternalColumns(
        newFieldNames,
        joinKey,
        spark.sessionState.conf.resolver
      ) match {
        case Left(error) => return Left(error)
        case Right(_)    => // Continue
      }

      val targetFieldsByName = resolveBackfillTargetFields(
        snapshotMetadata.collection.schema,
        newFieldNames
      ) match {
        case Left(error)  => return Left(error)
        case Right(value) => value
      }
      // The writer takes field ids and dimensions from the collection schema.
      val collectionSchema = CollectionSchema.parseFrom(
        snapshotMetadata.collection.schema.toProtobufBytes
      )

      // User parquet uses normal ingestion-friendly vector shapes (numeric
      // arrays, byte arrays, sparse maps/structs/JSON). Encode vector
      // fields to Milvus's physical per-row bytes before caching or joining so
      // Spark never widens or rewrites half/int8/sparse representations.
      val backfillDF = BackfillVectorColumns.encode(
        mappedBackfillDF,
        targetFieldsByName
      ) match {
        case Left(error) => return Left(error)
        case Right(df)   => df
      }

      val targetVectorFields = targetFieldsByName.collect {
        case (name, field) if BackfillVectorColumns.isVectorField(field) =>
          name -> BackfillVectorColumns.canonicalStructField(field)
      }

      val targetWriteFields = newFieldNames.map { n =>
        val field = targetFieldsByName(n)
        val structField = targetVectorFields.getOrElse(
          n,
          SnapshotSparkSchema.fieldToStructField(field)
        )
        (n, field.getFieldIDAsLong, structField)
      }

      // Every mode writes parquet-side values with their current Spark types.
      // Replace has no implicit cast, so validate before any writer runs.
      validateBackfillTargetFieldTypes(
        backfillDF.schema,
        targetWriteFields,
        config.mode
      ) match {
        case Left(error) => return Left(error)
        case Right(_)    => // Continue
      }

      // Input plans retain their captured filesystem identity when recomputed.
      // Spark resolves storageLevel by normalized query, so an equivalent
      // caller-owned cache is borrowed too. Only release a cache we created.
      if (backfillDF.storageLevel == StorageLevel.NONE) {
        backfillDF.cache()
        cachedBackfillDF = backfillDF
      }

      val joinKeyStats = validateJoinKeyCardinality(
        backfillDF,
        joinKey.internalColumns,
        joinKey.fieldNames,
        side = "Backfill parquet"
      ) match {
        case Left(error)  => return Left(error)
        case Right(stats) => stats
      }
      val backfillRowCount = joinKeyStats.rowCount
      logger.info(
        s"Backfill data file rows: $backfillRowCount " +
          s"(distinct join keys: ${joinKeyStats.distinctValidKeyCount})"
      )

      // In coalesce / overwrite modes, also read each target field from the snapshot
      // so the merge step (coalesce(snapshot,bf) or when(matched, bf).otherwise(snapshot))
      // can compare snapshot and parquet values per row. Requires a snapshot —
      // we need the field's Spark type.
      val readsSnapshotFields = config.readsSnapshotFields
      val extraReadFields
          : Seq[(String, Long, org.apache.spark.sql.types.StructField)] =
        if (readsSnapshotFields) targetWriteFields else Seq.empty

      // Read snapshot rows with segment metadata
      val snapshotDF = readCollectionWithMetadata(
        spark,
        config,
        joinKey,
        physicalSnapshot,
        additionalSegments,
        authoritativePartitions,
        extraReadFields
      ) match {
        case Left(error) => return Left(error)
        case Right(df)   => df
      }

      // This check only inspects schemas. Run it before any snapshot-side action
      // so a mismatched parquet key fails without scanning the collection.
      validateJoinKeyCompatibility(
        snapshotDF,
        backfillDF,
        joinKey
      ) match {
        case Left(error) => return Left(error)
        case Right(_)    => // Continue
      }

      // Merge snapshot and backfill DataFrames according to mode.
      val joinedDF = performJoin(
        snapshotDF,
        backfillDF,
        joinKey.internalColumns,
        newFieldNames,
        config.mode
      )

      val (collectionID, segmentToPartitionMap, segmentBasePathMap) =
        extractMetadataFromSnapshot(
          physicalSnapshot,
          additionalSegments,
          config.targetStorageOptions
            .getOrElse(StorageProperties.BucketName, ""),
          config.targetStorageOptions.getOrElse(StorageProperties.Address, ""),
          authoritativePartitions
        )

      val segmentSourceVersionMap = snapshotMetadata.storageV2ManifestList
        .getOrElse(Seq.empty)
        .map { item =>
          item.segmentID -> ManifestContentJson
            .parse(item.manifest)
            .fold(error => throw error, identity)
            .ver
            .toLong
        }
        .toMap

      // V2 uses packed parquet. V3 keeps the exact snapshot revision for
      // legacy CAS; delta output leaves the current revision to DataCoord.

      beforeWrite()
      val segmentResults = BackfillSegmentWrite.write(
        spark,
        joinedDF,
        collectionID,
        segmentToPartitionMap,
        segmentBasePathMap,
        segmentSourceVersionMap,
        additionalSegments,
        runConfig,
        newFieldNames,
        collectionSchema,
        targetVectorFields
      ) match {
        case Left(error)    => return Left(error)
        case Right(results) => results
      }

      // Build final result
      val executionTime = System.currentTimeMillis() - startTime
      val partitionIDs = segmentToPartitionMap.values.toSet

      val result = BackfillResult.success(
        segmentResults = segmentResults,
        executionTimeMs = executionTime,
        collectionId = collectionID,
        partitionId = resolveResultPartitionId(partitionIDs),
        schemaVersion = snapshotMetadata.collection.schema.version,
        newFieldNames = newFieldNames,
        totalBackfillDataRows = backfillRowCount
      )

      Right(result.copy(inputSnapshotId = inputSnapshotId))

    }
    withCleanup(
      () => { if (cachedBackfillDF != null) cachedBackfillDF.unpersist(); () },
      () => { if (inputSource != null) inputSource.close() }
    )(execute())
  }

  def managedColumnMapping(
      inputColumns: Seq[String],
      explicit: Option[Map[String, String]],
      collectionFieldNames: Boolean
  ): Option[Map[String, String]] =
    if (collectionFieldNames && explicit.isEmpty)
      Some(inputColumns.map(name => name -> name).toMap)
    else explicit

  /** Preserve a typed business failure while releasing every owned resource. */
  private[backfill] def withCleanup[A](releases: (() => Unit)*)(
      operation: => Either[BackfillError, A]
  ): Either[BackfillError, A] = {
    var outcome: Either[BackfillError, A] = null
    var primary: Throwable = null
    var suppressedBefore = 0
    try {
      try {
        outcome = operation
        outcome.left.foreach { error =>
          primary =
            error.cause.getOrElse(new IllegalStateException(error.message))
          suppressedBefore = primary.getSuppressed.length
        }
      } catch {
        case error: Throwable => primary = error; throw error
      } finally {
        WriterResources.closeAll(primary, releases: _*)
        if (
          outcome != null && outcome.isLeft && primary.getSuppressed.length > suppressedBefore
        )
          outcome =
            outcome.left.map(error => BackfillError.withCause(error, primary))
      }
      outcome
    } catch {
      case error: Exception => Left(BackfillError.fromException(error))
    }
  }

  /** Bind input identity on the driver before creating its lazy scan. */
  private def readBackfillData(
      spark: SparkSession,
      rawPath: String,
      config: BackfillConfig
  ): Either[BackfillError, InputSource] = {
    try {
      val path = BackfillStorage.normalizeObjectStorageScheme(rawPath, config)
      val binding = BackfillStorage.inputBinding(spark, path, config)
      Right(
        InputAdapter.read(
          spark,
          path,
          InputOptions(config.inputFormat.trim, config.icebergSnapshotId),
          binding
        )
      )
    } catch {
      case e: Exception =>
        Left(
          DataReadError(
            rawPath,
            s"Failed to read backfill input: ${e.getMessage}",
            Some(e)
          )
        )
    }
  }

  /** Resolve the default mapping for the selected join key, validate its
    * targets, and rename in one projection so chains and swaps stay intact.
    */
  private[backfill] def applyColumnMapping(
      df: DataFrame,
      joinFieldName: String,
      joinSpec: BackfillJoinKey,
      userMapping: Option[Map[String, String]]
  ): Either[BackfillError, DataFrame] = {
    val columns = df.columns.toSeq
    val columnSet = columns.toSet
    val primaryKey = joinSpec == BackfillJoinKey.PrimaryKey
    val mapping = userMapping.getOrElse {
      if (primaryKey) {
        if (!columnSet.contains("pk")) {
          return Left(
            SchemaValidationError(
              "Backfill parquet must contain a 'pk' column (or supply --column-mapping to rename the PK column)"
            )
          )
        }
        if (joinFieldName != "pk" && columnSet.contains(joinFieldName)) {
          return Left(
            SchemaValidationError(
              s"Backfill parquet contains both a 'pk' column and a column named '$joinFieldName' " +
                "(the collection's primary-key field). Remove one, or supply --column-mapping to disambiguate."
            )
          )
        }
        columns
          .map(name => name -> (if (name == "pk") joinFieldName else name))
          .toMap
      } else {
        if (!columnSet.contains(joinFieldName)) {
          return Left(
            SchemaValidationError(
              s"Backfill parquet must contain the physical join-key column " +
                s"'$joinFieldName' (or supply --column-mapping to rename the input key)"
            )
          )
        }
        columns.map(name => name -> name).toMap
      }
    }

    val missingSources = mapping.keySet.diff(columnSet)
    if (missingSources.nonEmpty) {
      return Left(
        SchemaValidationError(
          s"column mapping references parquet columns that don't exist: " +
            s"${missingSources.mkString(", ")}. Available: ${columns.mkString(", ")}"
        )
      )
    }
    val duplicateTargets = mapping.values.groupBy(identity).collect {
      case (name, values) if values.size > 1 => name
    }
    if (duplicateTargets.nonEmpty) {
      return Left(
        SchemaValidationError(
          s"column mapping has duplicate targets: ${duplicateTargets.mkString(", ")}"
        )
      )
    }
    val targets = mapping.values.toSet
    if (!targets.contains(joinFieldName)) {
      val key =
        if (primaryKey) "the primary key field" else "physical join-key field"
      return Left(
        SchemaValidationError(
          s"column mapping must include $key '$joinFieldName' as a target"
        )
      )
    }
    if ((targets - joinFieldName).isEmpty) {
      val target = if (primaryKey) "non-PK" else "non-join-key"
      return Left(
        SchemaValidationError(
          s"column mapping must include at least one $target field to backfill"
        )
      )
    }
    Right(
      df.select(
        columns
          .filter(mapping.contains)
          .map(name => df.col(name).as(mapping(name))): _*
      )
    )
  }

  private[backfill] def prepareBackfillData(
      df: DataFrame,
      joinKey: ResolvedJoinKey,
      joinSpec: BackfillJoinKey,
      userMapping: Option[Map[String, String]],
      collectionFieldNames: Seq[String]
  ): Either[BackfillError, PreparedBackfillData] = {
    val keyColumn = joinKey.fieldNames.head
    val mappedResult = applyColumnMapping(df, keyColumn, joinSpec, userMapping)

    mappedResult.flatMap { mapped =>
      val mappedColumns = mapped.columns.toSeq
      val missingKeys = joinKey.fieldNames.filterNot(mappedColumns.contains)
      if (missingKeys.nonEmpty) {
        Left(
          SchemaValidationError(
            s"Backfill data is missing join-key columns after column mapping: " +
              missingKeys.mkString(", ")
          )
        )
      } else {
        val caseSensitive = df.sparkSession.conf
          .get("spark.sql.caseSensitive", "false")
          .toBoolean
        val resolver: (String, String) => Boolean =
          if (caseSensitive) (left, right) => left == right
          else (left, right) => left.equalsIgnoreCase(right)
        val allocatedJoinKey = joinKey.withCollisionFreeInternalColumns(
          mappedColumns ++ collectionFieldNames,
          resolver
        )
        val keyAliases =
          allocatedJoinKey.components.map(c => c.fieldName -> c).toMap
        val normalized = mapped.select(
          mappedColumns.map { name =>
            keyAliases.get(name) match {
              case Some(component) =>
                mapped.col(name).as(component.internalColumn)
              case None => mapped.col(name)
            }
          }: _*
        )
        val targetFieldNames =
          mappedColumns.filterNot(allocatedJoinKey.fieldNames.contains)
        Right(
          PreparedBackfillData(
            dataFrame = normalized,
            joinKey = allocatedJoinKey,
            targetFieldNames = targetFieldNames
          )
        )
      }
    }
  }

  /** Build the ordered field-ID/schema projection used by snapshot reads.
    * Deduplication happens on the pair before either side is emitted so the
    * supplied Spark schema always stays aligned with `ReaderFieldIDs`.
    */
  private[backfill] def buildSnapshotReadProjection(
      joinKey: ResolvedJoinKey,
      extraReadFields: Seq[
        (String, Long, org.apache.spark.sql.types.StructField)
      ]
  ): Either[BackfillError, SnapshotReadProjection] = {
    val missingSchemas = joinKey.components.filter(_.sparkField.isEmpty)
    if (missingSchemas.nonEmpty) {
      Left(
        SchemaValidationError(
          s"Missing snapshot schema for join-key fields: " +
            missingSchemas.map(_.fieldName).mkString(", ")
        )
      )
    } else {
      val requested =
        joinKey.components.map(c => (c.fieldId, c.sparkField.get)) ++
          extraReadFields.map { case (_, fieldId, field) => (fieldId, field) }
      val deduped = requested
        .foldLeft(
          (
            Vector.empty[(Long, org.apache.spark.sql.types.StructField)],
            Set.empty[Long]
          )
        ) { case ((fields, seen), item @ (fieldId, _)) =>
          if (seen.contains(fieldId)) (fields, seen)
          else (fields :+ item, seen + fieldId)
        }
        ._1

      Right(
        SnapshotReadProjection(
          fieldIds = deduped.map(_._1),
          schema = org.apache.spark.sql.types.StructType(deduped.map(_._2))
        )
      )
    }
  }

  /** Normalize snapshot-side join columns in one projection. A sequential
    * `withColumnRenamed` chain can corrupt swaps or rename chains when a
    * snapshot field happens to use another component's internal name.
    */
  private[backfill] def normalizeSnapshotJoinColumns(
      df: DataFrame,
      joinKey: ResolvedJoinKey
  ): DataFrame = {
    val snapshotAliases =
      joinKey.components.map(c => c.fieldName -> c.internalColumn).toMap
    df.select(
      df.columns.toSeq.map { name =>
        snapshotAliases.get(name) match {
          case Some(alias) => df.col(name).as(alias)
          case None        => df.col(name)
        }
      }: _*
    )
  }

  /** Read collection data with _segment_id and _row_offset metadata. They are
    * used to match with the original sequence of rows for each segment
    *
    * @param joinKey
    *   Resolved snapshot fields used to match backfill input rows
    * @param snapshotMetadata
    *   Snapshot metadata that pins the collection schema and segments
    */
  private def readCollectionWithMetadata(
      spark: SparkSession,
      config: BackfillConfig,
      joinKey: ResolvedJoinKey,
      snapshotMetadata: SnapshotJson,
      additionalSegments: BackfillAdditionalSegments,
      authoritativePartitions: Map[Long, Long],
      extraReadFields: Seq[
        (String, Long, org.apache.spark.sql.types.StructField)
      ]
  ): Either[BackfillError, DataFrame] = {
    try {
      var options = config.getMilvusReadOptions
      // Backfill writes one value per physical snapshot row. Applying deletes
      // would remove rows and invalidate _row_offset-based field alignment.
      options = options + (MilvusOption.ReadApplyDeletes ->
        ApplyDeletesToSnapshotRows.toString)

      // Add snapshot collection ID
      options =
        options + (MilvusOption.SnapshotCollectionId -> snapshotMetadata.snapshotInfo.collectionId.toString)

      // Add snapshot partition IDs
      options =
        options + (MilvusOption.SnapshotPartitionIds -> snapshotMetadata.snapshotInfo.partitionIds
          .mkString(","))

      // Convert snapshot schema to protobuf bytes and pass as Base64
      val schemaBytes =
        snapshotMetadata.collection.schema.toProtobufBytes
      val schemaBytesBase64 =
        java.util.Base64.getEncoder.encodeToString(schemaBytes)
      options =
        options + (MilvusOption.SnapshotSchemaBytes -> schemaBytesBase64)

      // The supplied schema and ReaderFieldIDs must stay in the same order.
      // The component sequence also supports future logical/composite key
      // resolvers.
      val projection =
        buildSnapshotReadProjection(joinKey, extraReadFields) match {
          case Left(error)  => return Left(error)
          case Right(value) => value
        }
      options = options + (MilvusOption.ReaderFieldIDs -> projection.fieldIds
        .mkString(","))

      logger.info(
        s"Reading with schema: ${projection.schema.fieldNames.mkString(", ")}"
      )

      val v3Manifests =
        snapshotMetadata.storageV2ManifestList.getOrElse(Seq.empty)
      val sources = Seq(
        Option.when(v3Manifests.nonEmpty) {
          spark.read
            .schema(projection.schema)
            .format(classOf[BackfillSnapshotDataSource].getName)
            .options(
              options ++ Map(
                MilvusOption.SnapshotManifests -> SegmentListJson
                  .encodeManifestItems(v3Manifests),
                BackfillSnapshotDataSource.Partitions -> BackfillSnapshotDataSource
                  .partitionOption(
                    extractMetadataFromSnapshot(
                      snapshotMetadata,
                      additionalSegments,
                      config.targetStorageOptions
                        .getOrElse(StorageProperties.BucketName, ""),
                      config.targetStorageOptions
                        .getOrElse(StorageProperties.Address, ""),
                      authoritativePartitions
                    )._2.filter { case (id, _) =>
                      v3Manifests.exists(_.segmentID == id)
                    }
                  )
              )
            )
            .load()
        },
        additionalSegments.read(spark, projection.schema, options)
      ).flatten
      val df = sources.reduceOption(_.unionByName(_)).getOrElse {
        spark.createDataFrame(
          spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
          projection.schema
        )
      }

      val missingSnapshotJoinColumns =
        joinKey.fieldNames.filterNot(df.columns.contains)
      if (missingSnapshotJoinColumns.nonEmpty) {
        return Left(
          ConnectionError(
            message = s"Failed to read collection join-key columns: " +
              missingSnapshotJoinColumns.mkString(", ")
          )
        )
      }

      if (
        !df.columns.contains(SegmentIdCol) || !df.columns.contains(RowOffsetCol)
      ) {
        return Left(
          ConnectionError(
            message =
              s"Failed to read collection data with $SegmentIdCol and $RowOffsetCol. " +
                "Ensure milvus.extra.columns is set correctly."
          )
        )
      }

      Right(normalizeSnapshotJoinColumns(df, joinKey))
    } catch {
      case e: Exception =>
        logger.error(
          s"Failed to read Milvus collection ${config.collectionName}",
          e
        )
        Left(
          ConnectionError(
            message =
              s"Failed to read Milvus collection ${config.collectionName}: ${e.getMessage}",
            cause = Some(e)
          )
        )
    }
  }

  /** Every backfill mode requires parquet target-column types to match the
    * snapshot field types exactly. Replace writes parquet columns verbatim;
    * coalesce and overwrite additionally synthesize a per-row choice between
    * snapshot and parquet values. Allowing either an input mismatch or Spark
    * widening would make the writer emit binlogs whose Arrow type no longer
    * matches the Milvus field.
    */
  private[backfill] def validateBackfillTargetFieldTypes(
      backfillSchema: org.apache.spark.sql.types.StructType,
      targetFields: Seq[
        (String, Long, org.apache.spark.sql.types.StructField)
      ],
      mode: String
  ): Either[BackfillError, Unit] = {
    val backfillTypes =
      backfillSchema.fields.map(f => f.name -> f.dataType).toMap
    val mismatches = targetFields.collect {
      case (name, _, snapshotField)
          if backfillTypes
            .get(name)
            .exists(_ != snapshotField.dataType) =>
        s"$name (snapshot=${snapshotField.dataType.simpleString}, " +
          s"parquet=${backfillTypes(name).simpleString})"
    }
    if (mismatches.nonEmpty) {
      Left(
        SchemaValidationError(
          s"--mode=$mode requires backfill parquet target-column types to match " +
            s"snapshot field types exactly; cast them in the input ETL before running backfill. " +
            s"Mismatched: ${mismatches.mkString(", ")}"
        )
      )
    } else {
      Right(())
    }
  }

  /** Validate that backfill input contains a non-null, unique join key.
    *
    * A duplicate on the backfill side would fan one snapshot row out into
    * multiple joined rows and corrupt per-segment row counts. Nulls are
    * reported separately because Spark equi-joins never match null keys.
    */
  private[backfill] def validateJoinKeyCardinality(
      df: DataFrame,
      joinColumns: Seq[String],
      displayColumns: Seq[String],
      side: String
  ): Either[BackfillError, JoinKeyStats] = {
    if (joinColumns.isEmpty) {
      return Left(SchemaValidationError("Join key must not be empty"))
    }

    val missing = joinColumns.filterNot(df.columns.contains)
    if (missing.nonEmpty) {
      return Left(
        SchemaValidationError(
          s"$side is missing internal join-key columns: ${missing.mkString(", ")}"
        )
      )
    }

    try {
      val hasNull = joinColumns.map(name => col(name).isNull).reduce(_ || _)
      val keyValue = struct(joinColumns.map(col): _*)
      val counts = df
        .agg(
          count(lit(1)).as("__bf_key_rows__"),
          coalesce(
            sum(when(hasNull, lit(1L)).otherwise(lit(0L))),
            lit(0L)
          ).as("__bf_null_key_rows__"),
          countDistinct(when(!hasNull, keyValue)).as(
            "__bf_distinct_valid_keys__"
          )
        )
        .head()

      val stats = JoinKeyStats(
        rowCount = counts.getAs[Long]("__bf_key_rows__"),
        nullKeyRowCount = counts.getAs[Long]("__bf_null_key_rows__"),
        distinctValidKeyCount = counts.getAs[Long](
          "__bf_distinct_valid_keys__"
        )
      )
      val display = displayColumns.mkString("(", ", ", ")")

      if (stats.nullKeyRowCount > 0) {
        Left(
          SchemaValidationError(
            s"$side contains ${stats.nullKeyRowCount} row(s) with null join key " +
              s"$display. Join keys must be non-null."
          )
        )
      } else if (stats.distinctValidKeyCount != stats.rowCount) {
        Left(
          SchemaValidationError(
            s"$side contains duplicate join-key values " +
              s"(columns=$display, rows=${stats.rowCount}, " +
              s"distinct=${stats.distinctValidKeyCount}). " +
              "Deduplicate the input on the join key and retry."
          )
        )
      } else {
        Right(stats)
      }
    } catch {
      case e: Exception =>
        Left(
          SchemaValidationError(
            s"Failed to validate $side join key: ${e.getMessage}",
            Some(e)
          )
        )
    }
  }

  /** Validate join-key schema compatibility between snapshot and backfill data.
    */
  private[backfill] def validateJoinKeyCompatibility(
      snapshotDF: DataFrame,
      backfillDF: DataFrame,
      joinKey: ResolvedJoinKey
  ): Either[BackfillError, Unit] = {
    try {
      joinKey.components.foreach { component =>
        val snapshotField = snapshotDF.schema.fields
          .find(_.name == component.internalColumn)
          .getOrElse {
            return Left(
              SchemaValidationError(
                s"Original collection data must have join-key field " +
                  s"'${component.fieldName}'"
              )
            )
          }

        val backfillField = backfillDF.schema.fields
          .find(_.name == component.internalColumn)
          .getOrElse {
            return Left(
              SchemaValidationError(
                s"Backfill data must have join-key field " +
                  s"'${component.fieldName}' after column mapping"
              )
            )
          }

        if (snapshotField.dataType != backfillField.dataType) {
          return Left(
            SchemaValidationError(
              s"Join-key type mismatch for '${component.fieldName}': " +
                s"original=${snapshotField.dataType}, new=${backfillField.dataType}"
            )
          )
        }
      }

      Right(())

    } catch {
      case e: Exception =>
        logger.error("Failed to validate join-key schema compatibility", e)
        Left(
          SchemaValidationError(
            s"Failed to validate join-key schema compatibility: ${e.getMessage}"
          )
        )
    }
  }

  /** Merge snapshot rows with backfill rows per `mode`.
    *
    *   - replace: left join on the resolved key; backfill value replaces the
    *     snapshot value (the snapshot only contributes join + segment tracking
    *     columns). Unmatched snapshot rows end up with null target columns.
    *   - coalesce: the snapshot carries the target fields; after the left join,
    *     compute `coalesce(snapshot, backfill)` per field (snapshot wins when
    *     non-null, otherwise use backfill). Unmatched snapshot rows keep their
    *     original target values.
    *   - overwrite: the snapshot carries the target fields; after the left
    *     join, compute `when(matched, backfill).otherwise(snapshot)` per field
    *     (file wins when the join key matched, even if the file value is null).
    *     Unmatched snapshot rows keep their existing target values.
    */
  private[backfill] def performJoin(
      snapshotDF: DataFrame,
      backfillDF: DataFrame,
      joinColumns: Seq[String],
      newFieldNames: Seq[String],
      mode: String
  ): DataFrame = {
    val backfillWithFlag =
      backfillDF.withColumn(MatchFlagCol, lit(true))
    mode match {
      case BackfillConfig.ModeCoalesce =>
        // Rename backfill-side target columns to avoid name collisions with
        // snapshot-side columns now present on snapshotDF.
        val suffix = BackfillSideSuffix
        val renamedBackfill = newFieldNames.foldLeft(backfillWithFlag) {
          (df, n) =>
            df.withColumnRenamed(n, n + suffix)
        }
        val joined = snapshotDF.join(renamedBackfill, joinColumns, "left")
        // Attach per-field provenance flags BEFORE the coalesce rewrites `n`,
        // so the flags bind to the unresolved snapshot-side `n` attribute. This
        // keeps the flags accurate even though the later coalesce shadows the
        // original `n` column in the output schema.
        val withFlags = newFieldNames.foldLeft(joined) { (df, n) =>
          df.withColumn(usedSnapshotCol(n), df.col(n).isNotNull)
            .withColumn(
              usedBfCol(n),
              df.col(n).isNull && df.col(n + suffix).isNotNull
            )
        }
        newFieldNames.foldLeft(withFlags) { (df, n) =>
          df.withColumn(n, coalesce(df.col(n), df.col(n + suffix)))
            .drop(n + suffix)
        }

      case BackfillConfig.ModeOverwrite =>
        // Same suffix-rename scaffolding as coalesce — both modes need both
        // sides' target columns live on the joined frame.
        val suffix = BackfillSideSuffix
        val renamedBackfill = newFieldNames.foldLeft(backfillWithFlag) {
          (df, n) =>
            df.withColumnRenamed(n, n + suffix)
        }
        val joined = snapshotDF.join(renamedBackfill, joinColumns, "left")
        // Match flag is null for unmatched left rows, non-null for matched.
        // Using it (rather than `bf.isNotNull`) preserves overwrite's "file
        // wins, null included" semantics when the file explicitly stores null.
        val matched = col(MatchFlagCol).isNotNull
        val withFlags = newFieldNames.foldLeft(joined) { (df, n) =>
          df.withColumn(usedSnapshotCol(n), !matched)
            .withColumn(usedBfCol(n), matched)
        }
        newFieldNames.foldLeft(withFlags) { (df, n) =>
          df.withColumn(
            n,
            when(matched, df.col(n + suffix)).otherwise(df.col(n))
          ).drop(n + suffix)
        }

      case BackfillConfig.ModeReplace =>
        // Use the using-column join form: both sides share the internal join
        // columns, so Spark collapses them into one,
        // avoiding ambiguous-reference errors downstream.
        snapshotDF.join(backfillWithFlag, joinColumns, "left")

      case other =>
        // Defensive: validate() rejects anything else, but keep a clear error
        // in case a new mode constant is added without extending this match.
        throw new IllegalArgumentException(
          s"Unknown backfill mode '$other'"
        )
    }
  }

  /** Read authoritative segment identities from the pinned snapshot. */
  private def loadSnapshotSegments(
      metadata: SnapshotJson,
      config: BackfillConfig,
      snapshotAdapter: BackfillSnapshotAdapter
  ): Either[
    BackfillError,
    (BackfillAdditionalSegments, Map[Long, Long], Set[Long])
  ] = {
    try {
      val formatVersion = snapshotFormatVersion(metadata)
      if (metadata.manifestList.isEmpty) {
        require(
          metadata.storageV2ManifestList.forall(_.isEmpty) &&
            metadata.segmentIds.forall(
              _.isEmpty
            ) && metadata.allSegments.isEmpty,
          "Backfill requires complete snapshot segment Avro records"
        )
        return Right((BackfillAdditionalSegments.Empty, Map.empty, Set.empty))
      }
      // The Avro manifests and parquet footers are read through the native
      // store the fs.* options describe (the main bucket, IAM or static
      // keys), the same store every other driver-side read uses.
      val store = com.zilliz.milvus.storage.io.NativeObjectStore
        .Factory(
          com.zilliz.milvus.storage.credential.StorageProperties
            .from(config.getMilvusReadOptions)
        )
        .open()
      val bucket =
        config.targetStorageOptions.getOrElse(StorageProperties.BucketName, "")
      val endpoint =
        config.targetStorageOptions.getOrElse(StorageProperties.Address, "")
      val (loaded, partitions, deleteOnly) =
        WriterResources.closing(store.close()) {
          val entries = metadata.manifestList.map { path =>
            val located = StoragePath.parseMilvus(path, bucket, endpoint)
            require(
              located.bucket == bucket,
              s"Snapshot manifest crosses target bucket: $path"
            )
            SnapshotSegmentReader
              .parse(store.readAll(located.key), formatVersion)
              .fold(error => throw error, identity)
          }
          require(
            entries.map(_.segmentId).distinct.size == entries.size,
            "Snapshot repeats a segment Avro entry"
          )
          metadata.segmentIds.filter(_.nonEmpty).foreach { expected =>
            require(
              expected.distinct.size == expected.size && expected.toSet == entries
                .map(_.segmentId)
                .toSet,
              "Snapshot segment_ids must match its segment Avro records"
            )
          }
          val listedV3 = metadata.storageV2ManifestList
            .getOrElse(Seq.empty)
            .map(_.segmentID)
            .toSet
          require(
            entries
              .filter(e => e.segmentLevel != 1L && e.storageVersion == 3L)
              .forall(e => listedV3(e.segmentId)),
            "Snapshot Avro lists a V3 data segment without a segment manifest"
          )
          require(
            listedV3.subsetOf(
              entries.filter(_.storageVersion == 3L).map(_.segmentId).toSet
            ),
            "Snapshot V3 manifests must refer to Storage V3 segment Avro records"
          )
          val resolved = snapshotAdapter.resolve(entries, store, config)
          (
            resolved,
            entries.map(e => e.segmentId -> e.partitionId).toMap,
            entries.filter(_.segmentLevel == 1L).map(_.segmentId).toSet
          )
        }
      Right((loaded, partitions, deleteOnly))
    } catch {
      case e: Exception =>
        Left(
          SchemaValidationError(
            s"Failed to load snapshot segments: ${e.getMessage}"
          )
        )
    }
  }

  /** Load the required snapshot, reporting read and parse failures. */
  private def snapshotFormatVersion(metadata: SnapshotJson): Int = {
    val version = metadata.formatVersion.getOrElse(
      throw new IllegalArgumentException("snapshot is missing format_version")
    )
    require(
      SnapshotSegmentReader.supportsFormatVersion(version),
      s"Unsupported Milvus snapshot format version $version"
    )
    version
  }

  private def loadSnapshotMetadata(
      snapshotPath: String,
      config: BackfillConfig
  ): Either[BackfillError, SnapshotJson] = {
    BackfillStorage.readSnapshotJson(snapshotPath, config) match {
      case Right(json) if json.nonEmpty =>
        SnapshotJson.parse(json) match {
          case Right(metadata) =>
            try {
              snapshotFormatVersion(metadata)
              Right(metadata)
            } catch {
              case e: IllegalArgumentException =>
                Left(
                  SchemaValidationError(
                    s"Invalid snapshot metadata: ${e.getMessage}"
                  )
                )
            }
          case Left(e) =>
            Left(
              SchemaValidationError(
                s"Failed to parse snapshot metadata: ${e.getMessage}"
              )
            )
        }
      case Right(_) =>
        Left(SchemaValidationError(s"Snapshot file is empty: $snapshotPath"))
      case Left(e) =>
        Left(
          SchemaValidationError(s"Failed to read snapshot file: ${e.message}")
        )
    }
  }

  /** Extract collection metadata from snapshot: collectionID,
    * segment-to-partition mapping, segment base paths. Partition IDs come from
    * Avro records or an unambiguous snapshot partition list
    */
  private[backfill] def extractMetadataFromSnapshot(
      metadata: SnapshotJson,
      additionalSegments: BackfillAdditionalSegments =
        BackfillAdditionalSegments.Empty,
      bucket: String = "",
      endpoint: String = "",
      authoritativePartitions: Map[Long, Long] = Map.empty
  ): (Long, Map[Long, Long], Map[Long, String]) = {
    val collectionID = metadata.snapshotInfo.collectionId

    val manifestList = metadata.storageV2ManifestList.getOrElse(Seq.empty)
    var segmentToPartitionMap = Map.empty[Long, Long]
    var segmentBasePathMap = Map.empty[Long, String]

    require(
      manifestList.map(_.segmentID).distinct.size == manifestList.size,
      "Snapshot repeats a V3 manifest segment"
    )
    for (item <- manifestList) {
      val segId = item.segmentID
      val mc = ManifestContentJson
        .parse(item.manifest)
        .fold(error => throw error, identity)
      require(
        mc.ver > 0,
        s"Segment $segId requires a committed manifest version"
      )
      val located = StoragePath.parseMilvus(mc.basePath, bucket, endpoint)
      require(
        located.bucket == bucket,
        s"segment $segId manifest is in bucket '${located.bucket}' but backfill is bound to '$bucket': ${mc.basePath}"
      )
      val partitionId = authoritativePartitions
        .get(segId)
        .orElse {
          Option.when(metadata.snapshotInfo.partitionIds.distinct.size == 1)(
            metadata.snapshotInfo.partitionIds.head
          )
        }
        .getOrElse(
          throw new IllegalArgumentException(
            s"Segment $segId needs authoritative per-segment partition metadata for a multi-partition snapshot"
          )
        )
      require(
        metadata.snapshotInfo.partitionIds.contains(partitionId),
        s"Segment $segId partition $partitionId is absent from snapshot partitionIds"
      )
      segmentBasePathMap += segId -> located.key
      segmentToPartitionMap += segId -> partitionId
    }

    for ((segmentId, partitionId) <- additionalSegments.partitions) {
      require(
        !segmentToPartitionMap.contains(segmentId),
        s"Segment $segmentId is listed in multiple storage formats"
      )
      require(
        metadata.snapshotInfo.partitionIds.contains(partitionId),
        s"Segment $segmentId partition $partitionId is absent from snapshot partitionIds"
      )
      segmentToPartitionMap += (segmentId -> partitionId)
    }

    logger.info(
      s"Extracted from snapshot: collectionID=$collectionID, " +
        s"segments=${segmentToPartitionMap.keys.mkString(",")} " +
        s"(v3=${segmentBasePathMap.size}, additional=${additionalSegments.partitions.size})"
    )

    (collectionID, segmentToPartitionMap, segmentBasePathMap)
  }

}
