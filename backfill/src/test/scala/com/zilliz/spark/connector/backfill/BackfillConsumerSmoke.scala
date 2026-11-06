package com.zilliz.spark.connector.backfill

import java.nio.file.{Files, Path, Paths}
import java.util.Comparator
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import com.zilliz.milvus.storage.snapshot.json.{
  ManifestItemJson,
  SegmentListJson
}
import com.zilliz.spark.connector.options.MilvusOption

/** Standalone consumer compiled only against exported JARs. The sole argument
  * is the frozen collections/nearest fixture directory; this program copies it
  * before writing and does not depend on project classes or test helpers.
  */
object BackfillConsumerSmoke {
  private val mapper = new ObjectMapper()

  private def passed(name: String): Unit =
    println(s"MILVUS_CHECK\t$name\tpassed")

  private def copyFixture(source: Path, destination: Path): Unit = {
    require(
      Files.isRegularFile(source.resolve("snapshot.json")),
      "Expected the frozen collections/nearest fixture directory"
    )
    val paths = Files.walk(source)
    try
      paths.iterator().asScala.foreach { path =>
        val target = destination.resolve(source.relativize(path))
        if (Files.isDirectory(path)) Files.createDirectories(target)
        else Files.copy(path, target)
      }
    finally paths.close()
  }

  def main(args: Array[String]): Unit = {
    require(
      args.length == 1,
      "Usage: BackfillConsumerSmoke <collections/nearest directory>"
    )
    val root = Files.createTempDirectory("backfill-consumer-")
    var spark: SparkSession = null
    try {
      copyFixture(Paths.get(args(0)).toAbsolutePath, root)
      spark = SparkSession
        .builder()
        .master(sys.props.getOrElse("milvus.smoke.master", "local[2]"))
        .appName("backfill-consumer-smoke")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", root.resolve("warehouse").toString)
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      if (spark.sparkContext.master.startsWith("local-cluster[")) {
        val driverPid = ProcessHandle.current().pid()
        val workers = spark.sparkContext
          .parallelize(Seq(1, 2), 2)
          .mapPartitions(_ =>
            Iterator(
              ProcessHandle
                .current()
                .pid() -> org.apache.spark.SparkEnv.get.executorId
            )
          )
          .collect()
        assert(
          workers.nonEmpty && workers.forall { case (pid, executor) =>
            pid != driverPid && executor.nonEmpty && executor != "driver"
          },
          "Backfill consumer did not launch an independent executor"
        )
        passed("independent-executor")
      }
      val storage =
        Map("fs.storage_type" -> "local", "fs.root_path" -> root.toString)
      def read(snapshot: String): DataFrame = spark.read
        .format("milvus")
        .options(
          storage ++ Map(
            MilvusOption.SnapshotPath -> snapshot,
            MilvusOption.ReadApplyDeletes -> "false"
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
      val original = values(read("snapshot.json"))
      require(original.size == 16, "Unexpected frozen fixture row count")
      val input = spark.createDataFrame(
        spark.sparkContext.parallelize(
          Seq(Row(3001L, 41L), Row(3000L, null), Row(9999L, 99L)),
          2
        ),
        StructType(
          Seq(
            StructField("id", LongType, false),
            StructField("category", LongType, true)
          )
        )
      )
      val config = BackfillConfig(
        storageProperties = storage,
        mode = BackfillConfig.ModeOverwrite,
        batchSize = 2
      )
      val result = MilvusBackfill
        .run(input, root.resolve("snapshot.json").toString, config)
        .fold(
          error =>
            throw new IllegalStateException(error.message, error.cause.orNull),
          identity
        )
      assert(
        result.totalRowsWritten == original.size && result.totalMatchedRows == 2L
      )
      assert(result.stagingPrefix.isEmpty)
      val updated = mapper
        .readTree(Files.readAllBytes(root.resolve("snapshot.json")))
        .asInstanceOf[ObjectNode]
      val items =
        result.segmentResults.values.toSeq.sortBy(_.segmentId).map { segment =>
          ManifestItemJson(
            segment.segmentId,
            mapper
              .createObjectNode()
              .put("ver", segment.committedVersion)
              .put("base_path", segment.outputPath)
              .toString
          )
        }
      updated.set[JsonNode](
        "storagev2_manifest_list",
        mapper.readTree(SegmentListJson.encodeManifestItems(items))
      )
      Files.writeString(root.resolve("readback.json"), updated.toString)
      val expected = original.map {
        case (3000L, _) => 3000L -> None
        case (3001L, _) => 3001L -> Some(41L)
        case row        => row
      }
      assert(values(read("readback.json")) == expected)
      assert(
        values(read("snapshot.json")) == original,
        "Pinned source snapshot changed"
      )
      passed("backfill-v3-consumer")
      val resultPath = root.resolve("result.json")
      BackfillStorage
        .writeResultJson(spark, result, resultPath.toString, config)
        .fold(
          error =>
            throw new IllegalStateException(error.message, error.cause.orNull),
          identity
        )
      assert(
        mapper
          .readTree(Files.readAllBytes(resultPath))
          .get("totalRowsWritten")
          .asLong() == 16L
      )
      passed("backfill-result-json")
      assert(!Files.exists(root.resolve("staging")))
      passed("backfill-offline-boundary")
      println("ALL PASS")
    } finally {
      if (spark != null) spark.stop()
      val paths = Files.walk(root)
      try paths.sorted(Comparator.reverseOrder[Path]()).forEach(Files.delete(_))
      finally paths.close()
    }
  }
}
