package com.zilliz.milvus.storage.expr

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.{CodedInputStream, CodedOutputStream, WireFormat}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, FieldVector, VectorSchemaRoot}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.jni.segcore.SegcoreNative
import com.zilliz.milvus.storage.expr.ComparisonOperator._
import com.zilliz.milvus.storage.expr.Literal._
import com.zilliz.milvus.storage.io.ReadFixtures
import com.zilliz.milvus.storage.snapshot.json.{
  ManifestContentJson,
  SnapshotJson
}
import io.milvus.grpc.schema.{
  CollectionSchema,
  DataType => MilvusDataType,
  FieldSchema,
  IDs
}

/** segcore against [[PredicateEvaluator]] on the same rows.
  *
  * Each fixture's sealed segments are loaded into segcore from their V3
  * manifests, and its literal input rows from `expected.json` become Arrow
  * batches for the JVM evaluator, so neither side's answer comes from the
  * other's read. For every predicate here, the rows a segcore retrieve returns
  * must be the rows the JVM evaluator keeps: TRUE, under the three-valued logic
  * Spark applies. The retrieves read at timestamp 150, after the inserts (100)
  * and before the deletes (200), so deletes take no part in the comparison.
  *
  * The suite needs the segcore build's libraries (native/segcore/build), found
  * through `milvus.segcore.lib`, and runs in a JVM of its own; without them it
  * is canceled.
  */
class SegcoreDifferentialTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {
  import SegcoreDifferentialTest.{Row, Segment}

  private val libraryDir: Option[Path] =
    sys.props.get("milvus.segcore.lib").map(Paths.get(_))

  private val library: Option[Path] = libraryDir
    .map(_.resolve("libmilvus-segcore-jni.so"))
    .filter(Files.isRegularFile(_))

  private def requireSegcore(): Unit =
    assume(
      library.nonEmpty,
      s"the segcore build's libmilvus-segcore-jni.so is not under ${libraryDir
          .getOrElse("milvus.segcore.lib")}; build it with native/segcore/build/build.py"
    )

  private val readAt = 150L
  private val pk = 100L
  private val category = 102L
  private val id = FieldRef(pk, MilvusDataType.Int64)
  private val cat = FieldRef(category, MilvusDataType.Int64)

  private lazy val work = Files.createTempDirectory("segcore-differential")
  private lazy val allocator = new RootAllocator()

  override def beforeAll(): Unit = {
    super.beforeAll()
    library.foreach { path =>
      System.load(path.toString)
      SegcoreNative.init(
        Runtime.getRuntime.availableProcessors(),
        work.resolve("segcore").toString,
        1L << 30
      )
      // The fixtures take a few KiB.
      SegcoreNative.configureCache(
        1L << 30,
        1L << 30,
        1L << 30,
        1L << 30,
        1L << 30,
        1L << 30,
        work.resolve("cache").toString
      )
      SegcoreNative.initLocalFileSystem()
    }
  }

  override def afterAll(): Unit = {
    try if (library.nonEmpty) allocator.close()
    finally super.afterAll()
  }

  private final class Fixture(val name: String) {
    private val root = work.resolve(name)
    ReadFixtures.copyTo(s"collections/$name", root)

    private val snapshot = SnapshotJson
      .parse(
        new String(Files.readAllBytes(root.resolve("snapshot.json")), UTF_8)
      )
      .fold(error => throw error, identity)

    val fields: Seq[FieldSchema] =
      CollectionSchema
        .parseFrom(snapshot.collection.schema.toProtobufBytes)
        .fields

    // RootCoord appends the system fields to every collection; a sealed
    // segment's column groups carry them, and segcore resolves each column
    // through the schema.
    val collection: Long = SegcoreNative.newCollection(
      CollectionSchema(
        name = snapshot.collection.schema.name,
        fields = fields ++ Seq(
          FieldSchema(
            fieldID = 0,
            name = "RowID",
            description = "row id",
            dataType = MilvusDataType.Int64
          ),
          FieldSchema(
            fieldID = 1,
            name = "Timestamp",
            description = "time stamp",
            dataType = MilvusDataType.Int64
          )
        )
      ).toByteArray
    )
    SegcoreNative.setLoadFields(collection, Array(pk, category))

    val segments: Seq[Segment] = {
      val manifests = snapshot.storageV2ManifestList
        .getOrElse(Seq.empty)
        .map(item => item.segmentID -> item.manifest)
        .toMap
      val partition = snapshot.snapshotInfo.partitionIds.head
      expectedSegments(root.resolve("expected.json")).map {
        case (segment, rows, deleted) =>
          val manifest = ManifestContentJson
            .parse(manifests(segment))
            .fold(error => throw error, identity)
          val handle = SegcoreNative.newSealedSegment(
            collection,
            segment,
            false,
            loadInfo(
              segment,
              partition,
              snapshot.snapshotInfo.collectionId,
              rows.size,
              absoluteManifest(manifest.ver, root.resolve(manifest.basePath))
            )
          )
          SegcoreNative.load(handle, 0L)
          Segment(segment, rows, deleted, handle)
      }
    }
  }

  private lazy val fixtures: Seq[Fixture] =
    Seq(new Fixture("api"), new Fixture("nearest"))

  /** expected.json's segments: id, literal input rows and deleted ids. */
  private def expectedSegments(
      file: Path
  ): Seq[(Long, Seq[Row], Set[Long])] = {
    val tree = new ObjectMapper().readTree(file.toFile)
    tree.get("segments").elements().asScala.toSeq.map { segment =>
      val rows = segment.get("rows").elements().asScala.toSeq.map { row =>
        val category = row.get("category")
        Row(
          row.get("id").asLong(),
          if (category == null || category.isNull) None
          else Some(category.asLong())
        )
      }
      val deleted =
        segment.get("deletedIds").elements().asScala.map(_.asLong()).toSet
      (segment.get("id").asLong(), rows, deleted)
    }
  }

  /** Milvus keys for local storage are absolute paths; the fixture's are
    * relative to its directory, as remote keys are to their bucket.
    */
  private def absoluteManifest(version: Long, basePath: Path): String = {
    val node = new ObjectMapper().createObjectNode()
    node.put("ver", version)
    node.put("base_path", basePath.toAbsolutePath.toString)
    node.toString
  }

  // segcore.SegmentLoadInfo{segmentID=1, partitionID=2, collectionID=3,
  // num_of_rows=7, storageVersion=15, manifest_path=21}
  private def loadInfo(
      segment: Long,
      partition: Long,
      collection: Long,
      rows: Long,
      manifest: String
  ): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out = CodedOutputStream.newInstance(bytes)
    out.writeInt64(1, segment)
    out.writeInt64(2, partition)
    out.writeInt64(3, collection)
    out.writeInt64(7, rows)
    out.writeInt64(15, 3L)
    out.writeString(21, manifest)
    out.flush()
    bytes.toByteArray
  }

  /** The primary keys of a segcore.RetrieveResults{ids=1}. */
  private def retrievedKeys(results: Array[Byte]): Set[Long] = {
    val in = CodedInputStream.newInstance(results)
    var keys = Set.empty[Long]
    var tag = in.readTag()
    while (tag != 0) {
      if (WireFormat.getTagFieldNumber(tag) == 1)
        keys = IDs.parseFrom(in.readBytes().toByteArray).getIntId.data.toSet
      else in.skipField(tag)
      tag = in.readTag()
    }
    keys
  }

  private def segcoreKeys(
      fixture: Fixture,
      segment: Segment,
      predicate: PredicateExpr,
      timestamp: Long
  ): Set[Long] = {
    val plan = SegcorePlan
      .retrieve(predicate, fixture.fields, Seq(pk))
      .getOrElse(fail(s"$predicate does not translate"))
    val handle =
      SegcoreNative.createRetrievePlan(fixture.collection, plan.toByteArray)
    try
      retrievedKeys(
        SegcoreNative.retrieve(segment.handle, handle, timestamp, 1L << 30)
      )
    finally SegcoreNative.deleteRetrievePlan(handle)
  }

  private def evaluatorKeys(
      segment: Segment,
      predicate: PredicateExpr
  ): Set[Long] = {
    val ids = new BigIntVector(pk.toString, allocator)
    val categories = new BigIntVector(category.toString, allocator)
    val root = new VectorSchemaRoot(
      Seq[FieldVector](ids, categories).asJava
    )
    try {
      ids.allocateNew(segment.rows.size)
      categories.allocateNew(segment.rows.size)
      segment.rows.zipWithIndex.foreach { case (row, i) =>
        ids.set(i, row.id)
        row.category match {
          case Some(value) => categories.set(i, value)
          case None        => categories.setNull(i)
        }
      }
      root.setRowCount(segment.rows.size)
      val excluded = PredicateEvaluator.evaluate(
        predicate,
        root,
        field => Some(field.toString)
      )
      segment.rows.indices
        .filterNot(excluded.isExcluded)
        .map(segment.rows(_).id)
        .toSet
    } finally root.close()
  }

  /** Comparisons over every operator and literals inside and around the data,
    * IN lists with duplicates and missing values, null tests, the negation of
    * each, and AND, OR and their negations over pairs that involve the null
    * category.
    */
  private def predicates(segment: Segment): Seq[PredicateExpr] = {
    val ids = segment.rows.map(_.id).sorted
    val middle = ids(ids.size / 2)
    val operators = Seq(
      EqualTo,
      NotEqualTo,
      LessThan,
      LessThanOrEqual,
      GreaterThan,
      GreaterThanOrEqual
    )
    val leaves: Seq[PredicateExpr] =
      (for {
        operator <- operators
        value <- Seq(ids.head - 1, ids.head, middle, ids.last, ids.last + 1)
      } yield Comparison(id, operator, IntegerValue(value))) ++
        (for {
          operator <- operators
          value <- Seq(-1L, 0L, 1L, 2L)
        } yield Comparison(cat, operator, IntegerValue(value))) ++
        Seq(
          In(
            id,
            Vector(ids.last, ids.head, ids.last, ids.last + 7).map(IntegerValue)
          ),
          In(cat, Vector(1L, 0L, 1L).map(IntegerValue)),
          In(cat, Vector(2L, 5L).map(IntegerValue)),
          IsNull(cat),
          IsNotNull(cat),
          IsNull(id),
          IsNotNull(id),
          Comparison(cat, EqualNullSafe, NullValue)
        )
    val pairs: Seq[PredicateExpr] = Seq(
      Comparison(cat, EqualTo, IntegerValue(1)),
      Comparison(id, LessThan, IntegerValue(middle)),
      IsNull(cat),
      In(cat, Vector(IntegerValue(0)))
    )
    val combined = for {
      a <- pairs
      b <- pairs
      if a != b
      expr <- Seq(And(a, b), Or(a, b), Not(And(a, b)), Not(Or(a, b)))
    } yield expr
    leaves ++ leaves.map(Not(_)) ++ combined
  }

  test("segcore keeps the rows the JVM evaluator keeps, nulls included") {
    requireSegcore()
    val mismatches = for {
      fixture <- fixtures
      segment <- fixture.segments
      predicate <- predicates(segment)
      segcore = segcoreKeys(fixture, segment, predicate, readAt)
      jvm = evaluatorKeys(segment, predicate)
      if segcore != jvm
    } yield s"${fixture.name}/${segment.id} $predicate: segcore ${segcore.toSeq.sorted}, JVM ${jvm.toSeq.sorted}"
    val compared =
      fixtures.map(f => f.segments.map(predicates(_).size).sum).sum
    withClue(
      s"${mismatches.size} of $compared predicates differ:\n" + mismatches
        .mkString("\n")
    ) {
      mismatches shouldBe empty
    }
    // The comparison covers the null row it is meant to.
    fixtures
      .flatMap(_.segments)
      .flatMap(_.rows)
      .exists(_.category.isEmpty) shouldBe true
  }

  test("NOT keeps a null comparison null, as Spark does") {
    requireSegcore()
    val nearest = fixtures.find(_.name == "nearest").get
    val segment = nearest.segments.find(_.id == 30).get
    segment.rows.find(_.id == 3001).get.category shouldBe None
    // category is 0 for 3000, null for 3001 and 1 for the rest: under two-
    // valued logic 3001 would pass the negation.
    val notOne = Not(Comparison(cat, EqualTo, IntegerValue(1)))
    segcoreKeys(nearest, segment, notOne, readAt) shouldBe Set(3000L)
    evaluatorKeys(segment, notOne) shouldBe Set(3000L)
  }

  test(
    "a load from the manifest leaves the deletes it lists to the connector"
  ) {
    requireSegcore()
    // A segment loaded from its manifest path alone does not apply the delete
    // files the manifest lists, even past the deletes' timestamp: segcore
    // answers which rows a predicate admits, and the connector's own delete
    // exclusions stay in force.
    val nearest = fixtures.find(_.name == "nearest").get
    nearest.segments.foreach { segment =>
      segment.deleted should not be empty
      val all = segment.rows.map(_.id).toSet
      segcoreKeys(nearest, segment, IsNotNull(id), 1000L) shouldBe all
    }
  }
}

private object SegcoreDifferentialTest {

  /** A fixture's input row: the primary key and a nullable category. */
  final case class Row(id: Long, category: Option[Long])

  /** A fixture segment as both sides see it. */
  final case class Segment(
      id: Long,
      rows: Seq[Row],
      deleted: Set[Long],
      handle: Long
  )
}
