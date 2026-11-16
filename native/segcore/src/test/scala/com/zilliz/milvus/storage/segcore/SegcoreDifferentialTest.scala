package com.zilliz.milvus.storage.segcore

import java.nio.file.Files
import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, FieldVector, VectorSchemaRoot}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.jni.segcore.SegcoreNative
import com.zilliz.milvus.storage.expr.{
  And,
  Comparison,
  FieldRef,
  In,
  IsNotNull,
  IsNull,
  Not,
  Or,
  PredicateEvaluator,
  PredicateExpr
}
import com.zilliz.milvus.storage.expr.ComparisonOperator._
import com.zilliz.milvus.storage.expr.Literal._
import com.zilliz.milvus.storage.snapshot.SegmentIndexes
import io.milvus.grpc.schema.{DataType => MilvusDataType, IDs}

/** segcore against [[com.zilliz.milvus.storage.expr.PredicateEvaluator]] on the
  * same rows.
  *
  * The `api` and `basic` fixtures' sealed segments are loaded into segcore from
  * their V3 manifests (see [[SegcoreFixture]]); their literal input rows become
  * Arrow batches for the JVM evaluator, so neither side's answer comes from the
  * other's read. For every predicate here, the rows a segcore retrieve admits
  * must be the rows the JVM evaluator keeps: TRUE, under the three-valued logic
  * Spark applies. segcore names them by segment offset, which is the position
  * of the row in the fixture's literal input, as a test below checks. The
  * retrieves read at timestamp 150, after the inserts (100) and before the
  * deletes (200), so deletes take no part in the comparison.
  *
  * Milvus v3.0.6 gives no execution-path signal, per plan or per filter, so the
  * comparison is on results alone, and segcore has to agree whether a segment
  * is loaded with indexes or without. A segment loads with the indexes its
  * snapshot lists for the fields the predicates read; neither public fixture
  * lists one, so every comparison here is without.
  */
class SegcoreDifferentialTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {
  import SegcoreDifferentialTest.Loaded
  import SegcoreFixture.Row

  private val readAt = 150L
  private val pk = 100L
  private val category = 102L
  private val id = FieldRef(pk, MilvusDataType.Int64)
  private val cat = FieldRef(category, MilvusDataType.Int64)

  private val allocator = new RootAllocator()
  private var fixtures = Seq.empty[SegcoreFixture]
  private var loaded = Seq.empty[Loaded]

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (SegcoreRuntime.available) {
      val work = Files.createTempDirectory(SegcoreRuntime.work, "fixtures")
      Seq("api", "basic").foreach { name =>
        fixtures :+= new SegcoreFixture(
          name,
          work.resolve(name),
          Seq(pk, category)
        )
      }
      for {
        fixture <- fixtures
        expected <- fixture.expected
      } {
        fixture.segment(expected.id).rows shouldBe Some(
          expected.rows.size.toLong
        )
        val handle = fixture.newSegment(expected.id)
        loaded :+= Loaded(
          fixture,
          expected.id,
          expected.rows,
          expected.deleted,
          handle
        )
        SegcoreNative.load(handle, 0L)
        SegcoreNative.rowCount(handle) shouldBe expected.rows.size.toLong
      }
    }
  }

  override def afterAll(): Unit = {
    try {
      loaded.foreach(segment => SegcoreNative.deleteSegment(segment.handle))
      fixtures.foreach(_.close())
      allocator.close()
    } finally super.afterAll()
  }

  /** The segment offsets of the rows segcore admits. */
  private def segcoreRows(
      segment: Loaded,
      predicate: PredicateExpr,
      timestamp: Long
  ): Set[Int] = {
    val plan = SegcorePlan
      .retrieve(predicate, segment.fixture.fields)
      .getOrElse(fail(s"$predicate does not translate"))
    SegcoreRuntime.rows(
      SegcoreRuntime.retrieve(
        segment.fixture.collection,
        segment.handle,
        plan,
        timestamp
      )
    )
  }

  /** The positions of the literal rows the JVM evaluator keeps. */
  private def evaluatorRows(
      segment: Loaded,
      predicate: PredicateExpr
  ): Set[Int] = {
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
      segment.rows.indices.filterNot(excluded.isExcluded).toSet
    } finally root.close()
  }

  /** The primary keys of `rows`, for messages. */
  private def keys(segment: Loaded, rows: Set[Int]): Seq[Long] =
    rows.toSeq.sorted.map(segment.rows(_).id)

  /** Comparisons over every operator and literals inside and around the data,
    * IN lists with duplicates and missing values, null tests, the negation of
    * each, and AND, OR and their negations over pairs that involve the null
    * category.
    */
  private def predicates(segment: Loaded): Seq[PredicateExpr] = {
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
    SegcoreRuntime.assumeAvailable()
    val compared = loaded.map(predicates(_).size).sum
    val mismatches = for {
      segment <- loaded
      predicate <- predicates(segment)
      segcore = segcoreRows(segment, predicate, readAt)
      jvm = evaluatorRows(segment, predicate)
      if segcore != jvm
    } yield s"${segment.fixture.name}/${segment.id} $predicate: segcore ${keys(segment, segcore)}, JVM ${keys(segment, jvm)}"
    info(
      s"$compared predicates over ${loaded.size} segments of " +
        s"${fixtures.map(_.name).mkString(" and ")}; ${mismatches.size} differ"
    )
    withClue(
      s"${mismatches.size} of $compared predicates differ:\n" + mismatches
        .mkString("\n")
    ) {
      mismatches shouldBe empty
    }
    // The comparison covers the null row it is meant to.
    loaded.flatMap(_.rows).exists(_.category.isEmpty) shouldBe true
  }

  test("a plan without output fields names the admitted rows by offset") {
    SegcoreRuntime.assumeAvailable()
    loaded.foreach { segment =>
      val all = SegcorePlan.retrieve(IsNotNull(id), segment.fixture.fields).get
      val bare = SegcoreRuntime.retrieve(
        segment.fixture.collection,
        segment.handle,
        all,
        readAt
      )
      bare.offset.sorted shouldBe segment.rows.indices.map(_.toLong)
      bare.getIds shouldBe IDs.defaultInstance
      bare.fieldsData shouldBe empty
      // Asked for the primary key as well, segcore pairs each offset with the
      // key of the fixture's literal row at that position.
      val keyed = SegcoreRuntime.retrieve(
        segment.fixture.collection,
        segment.handle,
        all.withOutputFieldIds(Seq(pk)),
        readAt
      )
      keyed.offset.map(offset => segment.rows(offset.toInt).id) shouldBe
        keyed.getIds.getIntId.data
      keyed.offset.size shouldBe segment.rows.size
    }
  }

  test("no field the predicates read carries an index in these fixtures") {
    SegcoreRuntime.assumeAvailable()
    loaded.foreach { segment =>
      segment.fixture.segment(segment.id).indexes match {
        case SegmentIndexes.Available(all) =>
          all.map(_.fieldId).toSet.intersect(Set(pk, category)) shouldBe empty
        case _ =>
      }
    }
  }

  test("NOT keeps a null comparison null, as Spark does") {
    SegcoreRuntime.assumeAvailable()
    val segment =
      loaded.find(s => s.fixture.name == "basic" && s.id == 30).get
    segment.rows.find(_.id == 3001).get.category shouldBe None
    // category is 0 for 3000, null for 3001 and 1 for the rest: under two-
    // valued logic 3001 would pass the negation.
    val notOne = Not(Comparison(cat, EqualTo, IntegerValue(1)))
    keys(segment, segcoreRows(segment, notOne, readAt)) shouldBe Seq(3000L)
    keys(segment, evaluatorRows(segment, notOne)) shouldBe Seq(3000L)
  }

  test(
    "a load from the manifest leaves the deletes it lists to the connector"
  ) {
    SegcoreRuntime.assumeAvailable()
    // A segment loaded from its manifest path alone does not apply the delete
    // files the manifest lists, even past the deletes' timestamp: segcore
    // answers which rows a predicate admits, and the connector's own delete
    // exclusions stay in force.
    val basic = loaded.filter(_.fixture.name == "basic")
    basic should not be empty
    basic.foreach { segment =>
      segment.deleted should not be empty
      segcoreRows(segment, IsNotNull(id), 1000L) shouldBe
        segment.rows.indices.toSet
    }
  }
}

private object SegcoreDifferentialTest {

  /** A fixture segment loaded into segcore, with its literal input rows. */
  final case class Loaded(
      fixture: SegcoreFixture,
      id: Long,
      rows: Seq[SegcoreFixture.Row],
      deleted: Set[Long],
      handle: Long
  )
}
