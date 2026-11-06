package com.zilliz.spark.connector.backfill

import java.nio.file.{Files, Path, Paths}
import java.util.{Base64, UUID}
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import com.zilliz.milvus.storage.io.NativeObjectStore
import com.zilliz.milvus.storage.io.NativeWritableObjectStore
import com.zilliz.milvus.storage.manifest.{
  BinlogEntry,
  FieldBinlogEntry,
  SnapshotSegmentEntry,
  V3ManifestReader
}
import com.zilliz.milvus.storage.snapshot.json.{
  ManifestItemJson,
  SegmentListJson
}
import com.zilliz.spark.connector.input.IcebergInput
import com.zilliz.spark.connector.write.WriteOptions
import com.zilliz.spark.connector.write.WriteSchema
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}
import io.milvus.storage.{
  MilvusStorageColumnGroups,
  MilvusStorageManifest,
  MilvusStorageProperties,
  MilvusStorageTransaction
}

/** Offline backfill jobs over real V3 files, including input identity binding.
  */
final class BackfillNativeTest extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private val mapper = new ObjectMapper()
  private val collection = CollectionSchema(
    name = "rows",
    fields = Seq(
      FieldSchema(
        fieldID = 100L,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      ),
      FieldSchema(
        fieldID = 101L,
        name = "n",
        dataType = DataType.Int64,
        nullable = true
      )
    )
  )
  private val schemaBytes =
    Base64.getEncoder.encodeToString(collection.toByteArray)
  private val rowSchema = StructType(
    Seq(StructField("id", LongType, false), StructField("n", LongType, true))
  )
  private val physicalIds = Seq(Seq(3L, 1L, 4L), Seq(6L, 2L, 5L))
  private def original(id: Long): java.lang.Long =
    if (id == 2L || id == 3L) null else Long.box(id * 10L)
  private val inputValues = Map[Long, java.lang.Long](
    1L -> null,
    2L -> 200L,
    3L -> 300L,
    5L -> 500L,
    99L -> 9900L
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    NativeStorageLibrary.load()
    spark = SparkSession
      .builder()
      .master("local[2,2]")
      .appName("backfill-native")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.hadoopConfiguration.set(
      s"fs.s3a.bucket.${required("BACKFILL_TEST_S3_BUCKET")}.bucket.probe",
      "0"
    )
    // Catalog inputs select their endpoint globally after clearing bucket overrides.
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.bucket.probe", "0")
  }
  override def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  private def withDirectory(body: Path => Unit): Unit = {
    val root = Files.createTempDirectory("backfill-native-")
    try body(root)
    finally {
      val paths = Files.walk(root)
      try
        paths
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(Files.deleteIfExists(_))
      finally paths.close()
    }
  }
  private def localStorage(root: Path): Map[String, String] =
    Map("fs.storage_type" -> "local", "fs.root_path" -> root.toString)
  private def inputFrame: DataFrame = spark.createDataFrame(
    spark.sparkContext.parallelize(
      inputValues.toSeq.reverse.map { case (id, n) => Row(id, n) },
      2
    ),
    StructType(
      Seq(StructField("pk", LongType, false), StructField("n", LongType, true))
    )
  )

  private final case class Fixture(
      snapshot: Path,
      manifests: Seq[ManifestItemJson]
  )

  private def fixture(
      root: Path,
      storage: Map[String, String],
      includeTargetColumn: Boolean = true
  ): Fixture = {
    val store = new NativeWritableObjectStore(
      NativeObjectStore.Factory(storage).open()
    )
    try {
      val schema =
        WriteSchema.resolve(
          if (includeTargetColumn) rowSchema
          else StructType(rowSchema.fields.filter(_.name == "id")),
          collection,
          WriteSchema.Mode.Columns
        )
      val prefix = BackfillNativeFixture.rootFor(storage)
      val manifests = Vector.newBuilder[ManifestItemJson]
      val avros = physicalIds.zipWithIndex.map { case (ids, index) =>
        val segmentId = 31L + index
        val partitionId = 21L + index
        val base = Seq(prefix, "fixture", s"segment-$segmentId")
          .filter(_.nonEmpty)
          .mkString("/")
        val version = BackfillNativeFixture.write(
          base,
          schema,
          storage,
          ids.map(id =>
            if (includeTargetColumn) InternalRow(id, original(id))
            else InternalRow(id)
          )
        )
        manifests += ManifestItemJson(
          segmentId,
          mapper
            .createObjectNode()
            .put("ver", version)
            .put("base_path", base)
            .toString
        )
        val avro = Seq(prefix, "fixture", s"$segmentId.avro")
          .filter(_.nonEmpty)
          .mkString("/")
        store.write(
          avro,
          BackfillNativeFixture.encode(
            SnapshotSegmentEntry(
              segmentId,
              partitionId,
              2L,
              ids.size.toLong,
              3L,
              Seq.empty,
              Seq.empty
            )
          )
        )
        avro
      }
      val items = manifests.result()
      val document = mapper
        .readTree("""{
        "snapshot_info":{"name":"source","id":"1","collection_id":"10","partition_ids":["21","22"],"create_ts":"100"},
        "collection":{"schema":{"name":"rows","version":1,"fields":[
          {"fieldID":100,"name":"id","data_type":"Int64","is_primary_key":true},
          {"fieldID":101,"name":"n","data_type":"Int64","nullable":true}]}},
        "format_version":4,"segment_ids":["31","32"]
      }""")
        .asInstanceOf[ObjectNode]
      val list = document.putArray("manifest_list")
      avros.foreach(list.add)
      document.set[JsonNode](
        "storagev2_manifest_list",
        mapper.readTree(SegmentListJson.encodeManifestItems(items))
      )
      val path = root.resolve("snapshot.json")
      Files.writeString(path, document.toString)
      Fixture(path, items)
    } finally store.close()
  }

  private def read(
      storage: Map[String, String],
      source: Fixture,
      result: Option[BackfillResult]
  ): DataFrame = {
    val options = storage ++ Map(
      "milvus.snapshot.schema.bytes" -> schemaBytes,
      "milvus.snapshot.collection.id" -> "10",
      "milvus.snapshot.partition.ids" -> "21,22",
      "milvus.extra.columns" -> "_segment_id,_row_offset",
      "milvus.read.apply.deletes" -> "false"
    )
    if (source.manifests.nonEmpty) {
      val items = result
        .map(
          _.segmentResults.values.toSeq.map(value =>
            ManifestItemJson(
              value.segmentId,
              mapper
                .createObjectNode()
                .put("ver", value.committedVersion)
                .put("base_path", value.outputPath)
                .toString
            )
          )
        )
        .getOrElse(source.manifests)
      spark.read
        .format(classOf[BackfillSnapshotDataSource].getName)
        .options(
          options ++ Map(
            "milvus.snapshot.mode" -> "true",
            BackfillSnapshotDataSource.Partitions -> "31:21,32:22",
            "milvus.snapshot.manifests" -> SegmentListJson.encodeManifestItems(
              items
            )
          )
        )
        .load()
    } else throw new IllegalArgumentException("V3 fixture has no manifests")
  }

  private def assertRows(frame: DataFrame, mode: Option[String]): Unit = {
    val rows = frame
      .select("id", "n", "_segment_id", "_row_offset")
      .orderBy("_segment_id", "_row_offset")
      .collect()
    assert(rows.map(_.getLong(0)).toSeq == physicalIds.flatten)
    rows.foreach { row =>
      val id = row.getLong(0)
      val expected: java.lang.Long = mode match {
        case Some("replace")   => inputValues.getOrElse(id, null)
        case Some("overwrite") => inputValues.getOrElse(id, original(id))
        case Some("coalesce") =>
          Option(original(id)).getOrElse(inputValues.getOrElse(id, null))
        case None => original(id)
      }
      assert(
        row.get(1) == expected,
        s"$mode id=$id expected=$expected actual=${row.get(1)}"
      )
    }
  }

  private def manifestBytes(root: Path): Map[String, Vector[Byte]] = {
    val paths = Files.walk(root)
    try
      paths
        .iterator()
        .asScala
        .filter(path =>
          Files.isRegularFile(
            path
          ) && path.getParent.getFileName.toString == "_metadata"
        )
        .map(path => path.toString -> Files.readAllBytes(path).toVector)
        .toMap
    finally paths.close()
  }

  /** Models only the native application of returned ops, not DataCoord's
    * service validation, locking, index handling or retry semantics.
    */
  private def replayDelta(
      storage: Map[String, String],
      result: BackfillResult
  ): BackfillResult = {
    val segments = result.segmentResults.map { case (id, segment) =>
      val properties = new MilvusStorageProperties()
      val transaction = new MilvusStorageTransaction()
      try {
        properties.create(storage)
        transaction.begin(segment.outputPath, properties.getPtr, -1L, 0, 1)
        segment.manifestOps.foreach { op =>
          assert(
            op.files.forall(file =>
              file.startIndex == 0L && file.properties.isEmpty
            )
          )
          val groups = MilvusStorageColumnGroups.createFromGroups(
            Array(op.columns.toArray),
            Array(op.files.map(_.path).toArray),
            Array(op.files.map(file => file.endIndex - file.startIndex).toArray)
          )
          try {
            if (op.opType == "replace")
              op.columns.foreach(transaction.dropColumn)
            transaction.addColumnGroups(groups)
          } finally MilvusStorageColumnGroups.destroy(groups)
        }
        val version = transaction.commit()
        id -> segment.copy(
          committedVersion = version,
          manifestPaths = Seq(segment.outputPath),
          manifestOps = Seq.empty
        )
      } finally {
        try transaction.destroy()
        finally properties.free()
      }
    }
    result.copy(
      segmentResults = segments,
      manifestPaths = segments.values.toSeq.flatMap(_.manifestPaths)
    )
  }

  private def addConcurrentStat(
      storage: Map[String, String],
      base: String
  ): Long = {
    val store = new NativeWritableObjectStore(
      NativeObjectStore.Factory(storage).open()
    )
    val statPath = s"$base/_stats/concurrent/probe"
    try {
      store.createDir(s"$base/_stats/concurrent", recursive = true)
      store.write(statPath, Array[Byte](1, 2, 3))
    } finally store.close()
    val properties = new MilvusStorageProperties()
    val transaction = new MilvusStorageTransaction()
    try {
      properties.create(storage)
      transaction.begin(base, properties.getPtr, -1L, 0, 1)
      transaction.updateStat(
        "concurrent-probe",
        Array(statPath),
        Map("origin" -> "concurrent")
      )
      transaction.commit()
    } finally {
      try transaction.destroy()
      finally properties.free()
    }
  }

  private def assertConcurrentStat(
      storage: Map[String, String],
      segment: SegmentBackfillResult
  ): Unit = {
    val store = NativeObjectStore.Factory(storage).open()
    try {
      val stats = V3ManifestReader
        .loadStatistics(segment.outputPath, segment.committedVersion, "", store)
        .fold(throw _, identity)
      assert(
        stats("concurrent-probe").metadata == Map("origin" -> "concurrent")
      )
    } finally store.close()
  }

  for (
    mode <- Seq("coalesce", "overwrite", "replace");
    receiver <-
      Seq(
        BackfillConfig.LegacyReceiverCapability,
        BackfillConfig.ManifestDeltaReceiverCapability
      )
  ) {
    test(
      s"V3 $receiver $mode backfill preserves physical order and nulls across two segments"
    ) {
      withDirectory { root =>
        val storage = localStorage(root)
        val source = fixture(root, storage)
        val originalManifests = manifestBytes(root)
        assert(originalManifests.nonEmpty)
        val input = root.resolve("input").toString
        inputFrame.write.parquet(input)
        val config = BackfillConfig(
          collectionName = "rows",
          storageProperties = storage,
          batchSize = 2,
          mode = mode,
          receiverCapability = receiver
        )
        val result = MilvusBackfill
          .run(spark, input, source.snapshot.toString, config)
          .fold(
            error => {
              error.cause.foreach(_.printStackTrace())
              fail(
                error.message,
                error.cause.getOrElse(new IllegalStateException(error.message))
              )
            },
            identity
          )
        assert(result.segmentsProcessed == 2 && result.totalRowsWritten == 6L)
        assert(
          result.totalSourceRows == 6L && result.totalMatchedRows == 4L && result.totalBackfillDataRows == 5L
        )
        assert(result.partitionId == 0L && result.inputSnapshotId.isEmpty)
        val isDelta = receiver == BackfillConfig.ManifestDeltaReceiverCapability
        val readable = if (isDelta) {
          assert(manifestBytes(root) == originalManifests)
          assert(result.stagingPrefix.isEmpty && result.manifestPaths.isEmpty)
          result.segmentResults.values.foreach { segment =>
            assert(
              segment.committedVersion == -1L && segment.sourceVersion.isEmpty
            )
            assert(segment.manifestOps.map(_.columns) == Seq(Seq("101")))
            assert(segment.manifestOps.forall(_.opType == "replace"))
          }
          val first = result.segmentResults(31L)
          val concurrentVersion = addConcurrentStat(storage, first.outputPath)
          val replayed = replayDelta(storage, result)
          assert(
            replayed.segmentResults(31L).committedVersion > concurrentVersion
          )
          assertConcurrentStat(storage, replayed.segmentResults(31L))
          replayed
        } else result
        assertRows(read(storage, source, Some(readable)), Some(mode))
        assertRows(read(storage, source, None), None)
        assert(result.stagingPrefix.isEmpty)
        if (!isDelta)
          assert(
            result.segmentResults.values.forall(_.sourceVersion.contains(1L))
          )
        val resultPath = root.resolve("result.json")
        assert(
          BackfillStorage.writeResultJson(
            spark,
            result,
            resultPath.toString,
            config
          ) == Right(())
        )
        assert(
          mapper
            .readTree(Files.readString(resultPath))
            .get("totalRowsWritten")
            .asLong() == 6L
        )
      }
    }
  }

  test(
    "delta replace adds a target field absent from the physical snapshot manifest"
  ) {
    withDirectory { root =>
      val storage = localStorage(root)
      val source =
        fixture(root, storage, includeTargetColumn = false)
      val before = manifestBytes(root)
      assert(before.nonEmpty)
      val properties = new MilvusStorageProperties()
      try {
        properties.create(storage)
        source.manifests.foreach { item =>
          val content = mapper.readTree(item.manifest)
          val manifest = MilvusStorageManifest.open(
            content.get("base_path").asText(),
            properties,
            content.get("ver").asLong()
          )
          try {
            val groups = manifest.columnGroupsPtr
            val physicalColumns =
              (0 until MilvusStorageColumnGroups.count(groups))
                .flatMap(index =>
                  MilvusStorageColumnGroups.columns(groups, index)
                )
                .toSet
            assert(
              physicalColumns.contains("100") && !physicalColumns.contains(
                "101"
              )
            )
          } finally manifest.close()
        }
      } finally properties.free()
      val input = root.resolve("new-field-input").toString
      inputFrame.write.parquet(input)
      val result = MilvusBackfill
        .run(
          spark,
          input,
          source.snapshot.toString,
          BackfillConfig(
            collectionName = "rows",
            storageProperties = storage,
            mode = "replace",
            batchSize = 2,
            receiverCapability = BackfillConfig.ManifestDeltaReceiverCapability
          )
        )
        .fold(
          error =>
            fail(
              error.message,
              error.cause.getOrElse(new IllegalStateException(error.message))
            ),
          identity
        )
      assert(manifestBytes(root) == before)
      assert(result.segmentResults.values.forall { segment =>
        segment.manifestOps.map(op => op.opType -> op.columns) == Seq(
          "replace" -> Seq("101")
        )
      })
      assertRows(
        read(storage, source, Some(replayDelta(storage, result))),
        Some("replace")
      )
    }
  }

  test(
    "delta parquet files retain per-file ranges and read all rows without optional size hints"
  ) {
    withDirectory { root =>
      val storage = localStorage(root)
      val source = fixture(root, storage)
      val before = manifestBytes(root)
      assert(before.nonEmpty)
      val target = WriteSchema.resolve(
        StructType(Seq(StructField("n", LongType, nullable = true))),
        collection,
        WriteSchema.Mode.Columns
      )
      val segments = source.manifests.map { item =>
        val manifest = mapper.readTree(item.manifest)
        val base = manifest.get("base_path").asText()
        val writer = new BackfillV3Writer(
          target,
          WriteOptions(batchRows = 1, fileRollingBytes = 1L),
          storage,
          base,
          manifest.get("ver").asLong(),
          BackfillConfig.ManifestDeltaReceiverCapability
        )
        val message =
          try {
            physicalIds((item.segmentID - 31L).toInt).foreach { id =>
              writer.write(InternalRow(inputValues.getOrElse(id, null)))
            }
            writer.commit()
          } finally writer.close()
        assert(message.manifestOps.size == 1)
        val files = message.manifestOps.head.files
        assert(files.size == 3)
        assert(
          files.map(file => (file.startIndex, file.endIndex)) == Seq.fill(3)(
            (0L, 1L)
          )
        )
        assert(files.forall(_.properties.isEmpty))
        item.segmentID -> SegmentBackfillResult(
          item.segmentID,
          message.rowCount,
          Seq.empty,
          base,
          0L,
          manifestOps = message.manifestOps,
          sourceRowCount = message.rowCount
        )
      }.toMap
      assert(manifestBytes(root) == before)
      val result = BackfillResult.success(segments, 0L, 10L, 0L, 1, Seq("n"))
      val json = mapper.readTree(result.toJson)
      assert(!json.get("segments").get("31").has("version"))
      val readable = replayDelta(storage, result)
      assertRows(read(storage, source, Some(readable)), Some("replace"))
      assertRows(read(storage, source, None), None)
    }
  }

  test(
    "legacy backfill rejects a manifest advanced beyond its pinned snapshot revision"
  ) {
    withDirectory { root =>
      val storage = localStorage(root)
      val source = fixture(root, storage)
      val sourceManifest = mapper.readTree(source.manifests.head.manifest)
      val base = sourceManifest.get("base_path").asText()
      assert(addConcurrentStat(storage, base) == 2L)
      val before = manifestBytes(root)
      assert(before.nonEmpty)
      val target = WriteSchema.resolve(
        StructType(Seq(StructField("n", LongType, nullable = true))),
        collection,
        WriteSchema.Mode.Columns
      )
      val writer = new BackfillV3Writer(
        target,
        WriteOptions(batchRows = 2),
        storage,
        base,
        sourceManifest.get("ver").asLong(),
        BackfillConfig.LegacyReceiverCapability
      )
      try {
        physicalIds.head.foreach(id =>
          writer.write(InternalRow(Long.box(id * 100L)))
        )
        val failure = intercept[Exception](writer.commit())
        assert(failure.getMessage.contains("concurrent transaction"))
        writer.abort()
      } finally writer.close()
      assert(manifestBytes(root) == before)
      assertConcurrentStat(
        storage,
        SegmentBackfillResult(
          31L,
          3L,
          Seq(base),
          base,
          0L,
          committedVersion = 2L
        )
      )
      assertRows(read(storage, source, None), None)
    }
  }

  test(
    "managed V3 backfill uses collection field names and checks protection immediately before writes"
  ) {
    withDirectory { root =>
      val storage = localStorage(root)
      val source = fixture(root, storage)
      val input = root.resolve("managed-input").toString
      inputFrame.withColumnRenamed("pk", "id").write.parquet(input)
      val config = BackfillConfig(
        collectionName = "rows",
        storageProperties = storage,
        batchSize = 2,
        mode = "coalesce"
      )
      val resolved = MilvusBackfill
        .bindConfig(spark, config)
        .fold(error => fail(error.message), identity)
      def files(): Set[Path] = {
        val paths = Files.walk(root)
        try paths.iterator().asScala.filter(Files.isRegularFile(_)).toSet
        finally paths.close()
      }
      val before = files()
      val expired =
        new IllegalStateException("snapshot protection budget exhausted")
      var guardCalls = 0
      var completionCalls = 0
      def complete(result: BackfillResult): BackfillResult = {
        completionCalls += 1
        result.copy(stagingPrefix = "driver-owned-receipt")
      }
      val failed = MilvusBackfill.runPrepared(
        spark,
        input,
        source.snapshot.toString,
        config,
        resolved,
        10L,
        "source",
        () => { guardCalls += 1; throw expired },
        snapshotAdapter = BackfillSnapshotAdapter.V3Only,
        onResult = complete,
        onRecovery = _ => fail("unexpected partial write")
      )
      assert(failed.isLeft)
      assert(failed.left.toOption.get.cause.contains(expired))
      assert(guardCalls == 1)
      assert(completionCalls == 0)
      assert(files() == before)
      assertRows(read(storage, source, None), None)

      val result = MilvusBackfill
        .runPrepared(
          spark,
          input,
          source.snapshot.toString,
          config,
          resolved,
          10L,
          "source",
          () => { guardCalls += 1; () },
          snapshotAdapter = BackfillSnapshotAdapter.V3Only,
          onResult = complete,
          onRecovery = _ => fail("unexpected partial write")
        )
        .fold(error => fail(error.message, error.cause.orNull), identity)
      assert(guardCalls == 2)
      assert(completionCalls == 1)
      assert(result.stagingPrefix == "driver-owned-receipt")
      assert(result.segmentsProcessed == 2 && result.totalRowsWritten == 6L)
      assert(result.newFieldNames == Seq("n"))
      assertRows(read(storage, source, Some(result)), Some("coalesce"))
      assertRows(read(storage, source, None), None)
    }
  }

  test(
    "prepared backfill rejects a foreign collection or snapshot before reading segments or input"
  ) {
    withDirectory { root =>
      val storage = localStorage(root)
      val source = fixture(root, storage)
      val document = mapper
        .readTree(Files.readString(source.snapshot))
        .asInstanceOf[ObjectNode]
      document.putArray("manifest_list").add("does-not-exist.avro")
      val wrong = root.resolve("wrong-snapshot.json")
      Files.writeString(wrong, document.toString)
      val config = BackfillConfig(
        collectionName = "rows",
        storageProperties = storage
      )
      var guardCalls = 0
      Seq((11L, "source", "collection ID"), (10L, "another-snapshot", "name"))
        .foreach { case (collectionId, snapshotName, expectedError) =>
          val result = MilvusBackfill.runPrepared(
            spark,
            "does-not-exist-input",
            wrong.toString,
            config,
            config,
            collectionId,
            snapshotName,
            () => { guardCalls += 1; () },
            snapshotAdapter = BackfillSnapshotAdapter.V3Only,
            onResult = _ => fail("foreign snapshots must not produce a result"),
            onRecovery = _ => fail("unexpected partial write")
          )
          assert(result.isLeft)
          assert(result.left.toOption.get.isInstanceOf[SchemaValidationError])
          assert(result.left.toOption.get.message.contains(expectedError))
        }
      assert(guardCalls == 0)
      assert(!Files.exists(root.resolve("staging/foreign-snapshot")))
      assertRows(read(storage, source, None), None)
    }
  }

  test(
    "borrowed DataFrame backfill preserves caller and equivalent-plan caches after failure and success"
  ) {
    withDirectory { root =>
      val storage = localStorage(root)
      val source = fixture(root, storage)
      val input = inputFrame
        .withColumnRenamed("pk", "id")
        .persist(StorageLevel.MEMORY_ONLY)
      val prepared = MilvusBackfill
        .prepareBackfillData(
          input,
          ResolvedJoinKey.primaryKey("id", 100L, Some(rowSchema("id"))),
          BackfillJoinKey.PrimaryKey,
          MilvusBackfill.managedColumnMapping(
            input.columns.toSeq,
            None,
            collectionFieldNames = true
          ),
          Seq("id", "n")
        )
        .toOption
        .get
        .dataFrame
        .persist(StorageLevel.MEMORY_ONLY)
      def cacheEntry(frame: DataFrame) = spark.sharedState.cacheManager
        .lookupCachedData(
          frame.asInstanceOf[org.apache.spark.sql.classic.Dataset[Row]]
        )
        .get
      try {
        assert(input.count() == 5L && prepared.count() == 5L)
        val originalCache = cacheEntry(input)
        val preparedCache = cacheEntry(prepared)
        def assertBorrowedResources(): Unit = {
          assert(input.storageLevel == StorageLevel.MEMORY_ONLY)
          assert(prepared.storageLevel == StorageLevel.MEMORY_ONLY)
          assert(cacheEntry(input) eq originalCache)
          assert(cacheEntry(prepared) eq preparedCache)
          assert(input.count() == 5L && prepared.count() == 5L)
          assert(!spark.sparkContext.isStopped)
        }
        // These path-only settings must never reopen the borrowed Spark plan.
        val config = BackfillConfig(
          collectionName = "rows",
          storageProperties = storage,
          batchSize = 2,
          mode = "coalesce",
          inputFormat = "unsupported",
          icebergSnapshotId = Some("invalid"),
          sourceS3UseIam = Some(false),
          sourceS3AccessKey = Some("incomplete-source-identity")
        )
        assert(config.validate().isLeft)
        val resolved = MilvusBackfill
          .bindConfig(spark, config)
          .fold(error => fail(error.message), identity)
        var guardCalls = 0
        val expired = new IllegalStateException("protection exhausted")
        val failed = MilvusBackfill.runPrepared(
          input,
          source.snapshot.toString,
          resolved,
          10L,
          "source",
          () => { guardCalls += 1; throw expired },
          snapshotAdapter = BackfillSnapshotAdapter.V3Only,
          onResult = _ => fail("failed writes must not produce a result"),
          onRecovery = _ => ()
        )
        assert(failed.left.toOption.exists(_.cause.contains(expired)))
        assert(guardCalls == 1)
        assert(!Files.exists(root.resolve("staging/borrowed-frame")))
        assertBorrowedResources()
        assertRows(read(storage, source, None), None)

        val result = MilvusBackfill
          .run(input, source.snapshot.toString, config)
          .fold(error => fail(error.message, error.cause.orNull), identity)
        assert(guardCalls == 1)
        assert(result.inputSnapshotId.isEmpty)
        assert(result.segmentsProcessed == 2 && result.totalRowsWritten == 6L)
        assertRows(read(storage, source, Some(result)), Some("coalesce"))
        assertRows(read(storage, source, None), None)
        assertBorrowedResources()
      } finally {
        prepared.unpersist()
        input.unpersist()
      }
    }
  }

  for {
    capability <- Seq("legacy", "manifest-delta-v1")
    failClose <- Seq(false, true)
  } test(
    s"CLI $capability publishes once before actual input cleanup; close failure=$failClose"
  ) {
    withDirectory { root =>
      val storage = localStorage(root)
      val source = fixture(root, storage)
      val input = root.resolve("input")
      inputFrame.write.parquet(input.toString)
      val tag = UUID.randomUUID().toString
      val state = CleanupInputFileSystems.register(tag)
      state.failClose = failClose
      val conf = spark.sparkContext.hadoopConfiguration
      conf.setClass(
        "fs.cleanup.impl",
        classOf[CleanupInputFileSystem],
        classOf[org.apache.hadoop.fs.FileSystem]
      )
      conf.setBoolean("fs.cleanup.impl.disable.cache", true)
      val config = BackfillConfig(
        storageProperties = storage,
        receiverCapability = capability,
        batchSize = 2
      )
      val resultPath = root.resolve("cli-result.json")
      var publications = 0
      try {
        val outcome = MilvusBackfill.run(
          spark,
          s"cleanup://$tag${input.toAbsolutePath}",
          source.snapshot.toString,
          config,
          onResult = result => {
            publications += 1
            val recorded = BackfillApp.publishResult(
              result,
              resultPath.toString,
              (value, path) =>
                BackfillStorage.writeResolvedResultJson(value, path, config)
            )
            state.published = true
            recorded
          },
          onRecovery =
            _ => fail("complete writes must not publish partial recovery")
        )
        assert(publications == 1)
        assert(state.publishedCloses.get() == 1)
        val receipt = mapper.readTree(Files.readAllBytes(resultPath))
        assert(receipt.get("success").asBoolean())
        assert(receipt.get("totalRowsWritten").asLong() == 6L)
        if (failClose) {
          val error = outcome.left.toOption.get
          assert(error.stage == "cleanup")
          assert(error.cleanupComplete.contains(false))
          assert(error.cause.contains(state.cleanupFailure))
          assert(BackfillApp.complete(outcome) == 1)
        } else {
          assert(outcome.isRight)
          assert(BackfillApp.complete(outcome) == 0)
        }
        assertRows(read(storage, source, None), None)
      } finally {
        conf.unset("fs.cleanup.impl")
        conf.unset("fs.cleanup.impl.disable.cache")
        CleanupInputFileSystems.remove(tag)
      }
    }
  }

  test(
    "legacy partial segment commit is retained in a separate non-submittable recovery receipt"
  ) {
    withDirectory { root =>
      val storage = localStorage(root)
      val source = fixture(root, storage)
      val config = BackfillConfig(storageProperties = storage, batchSize = 2)
      val receiptPath = root.resolve("result.json.recovery.json")
      var receipts = 0
      val outcome = MilvusBackfill.runPrepared(
        inputFrame.withColumnRenamed("pk", "id"),
        source.snapshot.toString,
        config,
        10L,
        "source",
        beforeWrite = () => {
          addConcurrentStat(storage, "fixture/segment-32"); ()
        },
        snapshotAdapter = BackfillSnapshotAdapter.V3Only,
        onResult =
          _ => fail("partial output must never reach the success callback"),
        onRecovery = recovery => {
          receipts += 1
          BackfillStorage
            .writeResolvedRecoveryJson(recovery, receiptPath.toString, config)
            .fold(error => fail(error.message, error.cause.orNull), identity)
        }
      )
      val error = outcome.left.toOption.get
      assert(error.stage == "write" && error.cleanupComplete.contains(true))
      val recovery = error.recovery.get
      assert(!recovery.result.success && recovery.failedSegmentIds == Seq(32L))
      assert(recovery.result.segmentResults.keySet == Set(31L))
      val completed = recovery.result.segmentResults(31L)
      assert(
        completed.committedVersion == 2L && completed.sourceVersion.contains(1L)
      )
      assert(completed.sourceRowCount == 3L && completed.rowCount == 3L)
      assert(receipts == 1 && Files.readString(receiptPath) == recovery.toJson)
      val json = mapper.readTree(Files.readString(receiptPath))
      assert(json.get("kind").asText() == "backfill-recovery-v1")
      assert(!json.get("complete").asBoolean())
      assert(!json.get("completedResult").get("success").asBoolean())
      assert(!Files.exists(root.resolve("result.json")))
      assertRows(read(storage, source, None), None)
      assertConcurrentStat(
        storage,
        SegmentBackfillResult(
          32L,
          3L,
          Seq("fixture/segment-32"),
          "fixture/segment-32",
          0L,
          committedVersion = 2L
        )
      )
    }
  }

  test(
    "authoritative Avro row count mismatch fails before any segment commit"
  ) {
    withDirectory { root =>
      val storage = localStorage(root)
      val source = fixture(root, storage)
      Files.write(
        root.resolve("fixture/31.avro"),
        BackfillNativeFixture.encode(
          SnapshotSegmentEntry(31L, 21L, 2L, 4L, 3L, Seq.empty, Seq.empty)
        )
      )
      val before = manifestBytes(root)
      val outcome = MilvusBackfill.run(
        inputFrame.withColumnRenamed("pk", "id"),
        source.snapshot.toString,
        BackfillConfig(storageProperties = storage)
      )
      assert(outcome.isLeft)
      assert(
        outcome.left.toOption.get.message.contains("physical") ||
          outcome.left.toOption.get.message.contains("Physical")
      )
      assert(manifestBytes(root) == before)
    }
  }

  Seq("legacy", "manifest-delta-v1").foreach { capability =>
    test(
      s"$capability keeps deleted physical rows and excludes L0 from output while visible readback stays deleted"
    ) {
      withDirectory { root =>
        val fixturePath = Paths
          .get(
            getClass
              .getResource("/read-fixtures/collections/nearest/snapshot.json")
              .toURI
          )
          .getParent
        val paths = Files.walk(fixturePath)
        try
          paths.iterator().asScala.foreach { path =>
            val target = root.resolve(fixturePath.relativize(path))
            if (Files.isDirectory(path)) Files.createDirectories(target)
            else Files.copy(path, target)
          }
        finally paths.close()
        val storage = localStorage(root)
        Files.write(
          root.resolve("l0.avro"),
          BackfillNativeFixture.encode(
            SnapshotSegmentEntry(
              99L,
              20L,
              1L,
              0L,
              3L,
              Seq.empty,
              Seq.empty,
              channelName = "backfill-fixture-channel"
            )
          )
        )
        // The L0 manifest references the same pinned deletion artifact. Its
        // column files must never become a backfill data segment.
        val l0Base = "files/insert_log/10/20/99"
        Files.createDirectories(root.resolve(s"$l0Base/_metadata"))
        Files.copy(
          root.resolve("files/insert_log/10/20/30/_metadata/manifest-2.avro"),
          root.resolve(s"$l0Base/_metadata/manifest-2.avro")
        )
        Files.createDirectories(root.resolve(s"$l0Base/_delta"))
        Files.copy(
          root.resolve("files/insert_log/10/20/30/_delta/delete.parquet"),
          root.resolve(s"$l0Base/_delta/delete.parquet")
        )
        val l0Item = ManifestItemJson(
          99L,
          mapper
            .createObjectNode()
            .put("ver", 2L)
            .put("base_path", l0Base)
            .toString
        )
        val snapshot = root.resolve("snapshot.json")
        val document = mapper
          .readTree(Files.readAllBytes(snapshot))
          .asInstanceOf[ObjectNode]
        document.withArray("manifest_list").elements().asScala.foreach { item =>
          val path = root.resolve(item.asText())
          Files.write(
            path,
            BackfillNativeFixture
              .withChannel(Files.readAllBytes(path), "backfill-fixture-channel")
          )
        }
        document.withArray("manifest_list").add("l0.avro")
        document
          .withArray("storagev2_manifest_list")
          .add(
            mapper
              .readTree(SegmentListJson.encodeManifestItems(Seq(l0Item)))
              .get(0)
          )
        Files.writeString(snapshot, document.toString)
        def readSnapshot(path: String, deletes: Boolean): DataFrame = spark.read
          .format("milvus")
          .options(
            storage ++ Map(
              "milvus.snapshot.path" -> path,
              "milvus.read.apply.deletes" -> deletes.toString
            )
          )
          .load()
          .select("id", "category")
          .orderBy("id")
        def values(frame: DataFrame): Seq[(Long, Option[Long])] = frame
          .collect()
          .toSeq
          .map(row =>
            row.getLong(0) -> Option(row.get(1)).map(_.asInstanceOf[Long])
          )
        val physicalBefore = values(readSnapshot("snapshot.json", false))
        val visibleBefore = values(readSnapshot("snapshot.json", true))
        val deleted = physicalBefore.map(_._1).toSet -- visibleBefore.map(_._1)
        assert(
          physicalBefore.size == 16 && visibleBefore.size == 14 && deleted == Set(
            3002L,
            3105L
          )
        )
        val input = spark.createDataFrame(
          spark.sparkContext.parallelize(physicalBefore.map { case (id, _) =>
            Row(id, id * 10L)
          }),
          StructType(
            Seq(
              StructField("id", LongType, false),
              StructField("category", LongType, false)
            )
          )
        )
        val result = MilvusBackfill
          .run(
            input,
            snapshot.toString,
            BackfillConfig(
              storageProperties = storage,
              mode = "overwrite",
              receiverCapability = capability,
              batchSize = 2
            )
          )
          .fold(error => fail(error.message, error.cause.orNull), identity)
        assert(
          result.totalSourceRows == 16 && result.totalRowsWritten == 16 && result.totalMatchedRows == 16
        )
        assert(result.segmentResults.keySet == Set(30L, 31L))
        val committed =
          if (capability == "manifest-delta-v1") replayDelta(storage, result)
          else result
        val items = committed.segmentResults.values.toSeq.map(segment =>
          ManifestItemJson(
            segment.segmentId,
            mapper
              .createObjectNode()
              .put("ver", segment.committedVersion)
              .put("base_path", segment.outputPath)
              .toString
          )
        )
        document.set[JsonNode](
          "storagev2_manifest_list",
          mapper.readTree(SegmentListJson.encodeManifestItems(items :+ l0Item))
        )
        Files.writeString(root.resolve("readback.json"), document.toString)
        val all = values(readSnapshot("readback.json", false))
        val visible = values(readSnapshot("readback.json", true))
        assert(all == physicalBefore.map { case (id, _) =>
          id -> Some(id * 10L)
        })
        assert(visible == visibleBefore.map { case (id, _) =>
          id -> Some(id * 10L)
        })
        assert(values(readSnapshot("snapshot.json", false)) == physicalBefore)
        assert(values(readSnapshot("snapshot.json", true)) == visibleBefore)
      }
    }
  }

  private def required(name: String): String = sys.env
    .get(name)
    .filter(_.nonEmpty)
    .getOrElse(
      throw new IllegalArgumentException(
        s"Backfill identity tests require $name"
      )
    )
  private def s3Config: BackfillConfig = BackfillConfig(
    collectionName = "rows",
    s3Endpoint = required("BACKFILL_TEST_S3_ENDPOINT"),
    s3BucketName = required("BACKFILL_TEST_S3_BUCKET"),
    s3AccessKey = required("BACKFILL_TEST_S3_TARGET_ACCESS_KEY"),
    s3SecretKey = required("BACKFILL_TEST_S3_TARGET_SECRET_KEY"),
    s3RootPath = "target/" + UUID.randomUUID().toString,
    batchSize = 2,
    mode = "overwrite"
  )
  private def hadoopOptions(
      config: BackfillConfig,
      path: String
  ): Map[String, String] =
    BackfillStorage
      .inputBinding(spark, path, config)
      .newHadoopConfiguration()
      .iterator()
      .asScala
      .map(entry => entry.getKey -> entry.getValue)
      .toMap

  test(
    "run binds legacy main S3 credentials to input when there are no source overrides"
  ) {
    withDirectory { root =>
      val config = s3Config
      val storage = config.resolveStorage(spark.sessionState.newHadoopConf())
      val source = fixture(root, storage)
      val path = s"s3a://${config.s3BucketName}/${config.s3RootPath}/input"
      inputFrame.write.options(hadoopOptions(config, path)).parquet(path)
      val before = spark.sparkContext.hadoopConfiguration
        .iterator()
        .asScala
        .map(e => e.getKey -> e.getValue)
        .toMap
      val result = MilvusBackfill
        .run(spark, path, source.snapshot.toString, config)
        .fold(
          error => {
            error.cause.foreach(_.printStackTrace())
            fail(
              error.message,
              error.cause.getOrElse(new IllegalStateException(error.message))
            )
          },
          identity
        )
      assertRows(read(storage, source, Some(result)), Some("overwrite"))
      val after = spark.sparkContext.hadoopConfiguration
        .iterator()
        .asScala
        .map(e => e.getKey -> e.getValue)
        .toMap
      assert(after == before)
    }
  }

  // Lines without an Iceberg runtime cover the refusal in IcebergInputTest.
  if (IcebergInput.available)
    test(
      "run gives explicit Iceberg source identity priority over inherited same-bucket target credentials"
    ) {
      withDirectory { root =>
        val base = s3Config
        val config = base.copy(
          storageProperties = localStorage(root),
          inputFormat = "iceberg",
          sourceS3Endpoint = Some(required("BACKFILL_TEST_S3_ENDPOINT")),
          sourceS3AccessKey =
            Some(required("BACKFILL_TEST_S3_SOURCE_ACCESS_KEY")),
          sourceS3SecretKey =
            Some(required("BACKFILL_TEST_S3_SOURCE_SECRET_KEY")),
          sourceS3UseIam = Some(false),
          sourceS3UseSSL = Some(false),
          sourceS3Region = Some("us-east-1")
        )
        val source = fixture(root, config.storageProperties)
        val catalog =
          "backfillinput" + UUID.randomUUID().toString.replace("-", "")
        val property = s"spark.sql.catalog.$catalog"
        val warehouse = s"s3a://${base.s3BucketName}/source/$catalog/warehouse"
        spark.conf.set(property, "org.apache.iceberg.spark.SparkCatalog")
        spark.conf.set(property + ".type", "hadoop")
        spark.conf.set(property + ".warehouse", warehouse)
        hadoopOptions(config, warehouse).foreach { case (key, value) =>
          spark.conf.set(s"$property.hadoop.$key", value)
        }
        spark.sql(
          s"CREATE TABLE $catalog.db.rows (pk BIGINT, n BIGINT) USING iceberg"
        )
        spark.sql(
          s"INSERT INTO $catalog.db.rows VALUES (1, NULL), (2, 200), (3, 300), (5, 500), (99, 9900)"
        )
        val conf = spark.sparkContext.hadoopConfiguration
        val prefix = s"fs.s3a.bucket.${base.s3BucketName}."
        val target = Map(
          prefix + "access.key" -> base.s3AccessKey,
          prefix + "secret.key" -> base.s3SecretKey,
          prefix + "aws.credentials.provider" -> "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        val previous =
          target.keys.map(key => key -> Option(conf.get(key))).toMap
        try {
          target.foreach { case (key, value) =>
            conf.set(key, value)
            spark.conf.set(s"$property.hadoop.$key", value)
          }
          val result = MilvusBackfill
            .run(spark, s"$catalog.db.rows", source.snapshot.toString, config)
            .fold(
              error => {
                error.cause.foreach(_.printStackTrace())
                fail(
                  error.message,
                  error.cause
                    .getOrElse(new IllegalStateException(error.message))
                )
              },
              identity
            )
          assert(result.inputSnapshotId.nonEmpty)
          assert(
            mapper
              .readTree(result.toJson)
              .get("inputSnapshotId")
              .asLong() == result.inputSnapshotId.get
          )
          assertRows(
            read(config.storageProperties, source, Some(result)),
            Some("overwrite")
          )
          target.foreach { case (key, value) => assert(conf.get(key) == value) }
        } finally
          previous.foreach { case (key, value) =>
            value match {
              case Some(text) => conf.set(key, text)
              case None       => conf.unset(key)
            }
          }
      }
    }
}
