package com.zilliz.milvus.storage.segcore

import java.nio.file.{Files, Path}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.jni.segcore.{SegcoreException, SegcoreNative}
import com.zilliz.milvus.storage.expr.{
  And,
  Comparison,
  EndsWith,
  FieldRef,
  In,
  Not,
  Or,
  PredicateExpr,
  StartsWith
}
import com.zilliz.milvus.storage.expr.ComparisonOperator._
import com.zilliz.milvus.storage.expr.Literal._
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{
  CollectionSchema,
  DataType => MilvusDataType,
  FieldData,
  FieldSchema,
  LongArray,
  ScalarField,
  StringArray
}

import milvus.proto.plan.plan.{
  ColumnInfo,
  Expr,
  GenericValue,
  OpType,
  PlanNode,
  QueryPlanNode,
  UnaryRangeExpr
}
import milvus.proto.segcore.segcore.InsertRecord

/** The segcore JNI in one JVM: its settings, a growing segment filled by
  * inserts, a sealed load that is cancelled, and where its libraries come from.
  * The growing segment covers string fields, which no public fixture has; it
  * goes once a sealed fixture covers them.
  */
class SegcoreNativeTest extends AnyFunSuite with Matchers {

  private val pk = FieldSchema(
    fieldID = 100,
    name = "pk",
    isPrimaryKey = true,
    dataType = MilvusDataType.Int64
  )
  private val age =
    FieldSchema(fieldID = 101, name = "age", dataType = MilvusDataType.Int64)
  private val name = FieldSchema(
    fieldID = 102,
    name = "name",
    dataType = MilvusDataType.VarChar,
    typeParams = Seq(KeyValuePair(key = "max_length", value = "64"))
  )
  private val fields = Seq(pk, age, name)

  private val pks = Seq(1L, 2L, 3L, 4L, 5L, 6L)
  private val ages = Seq(10L, 25L, 30L, 45L, 50L, 65L)
  private val names = Seq("alice", "bob", "carol", "dave", "erin", "frank")

  private def ref(field: FieldSchema): FieldRef =
    FieldRef(field.fieldID, field.dataType)

  /** The primary keys of the rows `predicate` admits: a growing segment's
    * offsets follow the inserts, so offset i is the i-th inserted row.
    */
  private def retrieved(
      collection: Long,
      segment: Long,
      predicate: PredicateExpr
  ): Set[Long] =
    SegcoreRuntime
      .rows(
        SegcoreRuntime.retrieve(
          collection,
          segment,
          SegcorePlan.retrieve(predicate, fields).get,
          1000L
        )
      )
      .map(pks)

  test("the log level takes Milvus's names only") {
    SegcoreRuntime.assumeAvailable()
    SegcoreRuntime.work
    SegcoreNative.setLogLevel("error")
    SegcoreNative.setLogLevel("warn")
    an[IllegalArgumentException] should be thrownBy SegcoreNative.setLogLevel(
      "warning"
    )
  }

  test("a growing segment answers plans over integer and string fields") {
    SegcoreRuntime.assumeAvailable()
    SegcoreRuntime.work
    val collection = SegcoreNative.newCollection(
      CollectionSchema(name = "growing", fields = fields).toByteArray
    )
    val segment = SegcoreNative.newGrowingSegment(collection, 1L)
    try {
      val record = InsertRecord(
        fieldsData = Seq(
          longs(pk, pks),
          longs(age, ages),
          FieldData(
            `type` = name.dataType,
            fieldName = name.name,
            fieldId = name.fieldID,
            field = FieldData.Field.Scalars(
              ScalarField(data =
                ScalarField.Data.StringData(StringArray(data = names))
              )
            )
          )
        ),
        numRows = pks.size.toLong
      )
      SegcoreNative.insert(
        segment,
        pks.toArray,
        Array.fill(pks.size)(100L),
        record.toByteArray
      )
      SegcoreNative.rowCount(segment) shouldBe pks.size.toLong
      // Asked for the primary key, segcore pairs each offset with the key
      // inserted at that position.
      val everyRow = SegcorePlan
        .retrieve(Comparison(ref(pk), GreaterThan, IntegerValue(0)), fields)
        .get
      val keyed = SegcoreRuntime.retrieve(
        collection,
        segment,
        everyRow.withOutputFieldIds(Seq(pk.fieldID)),
        1000L
      )
      keyed.offset.map(offset => pks(offset.toInt)) shouldBe
        keyed.getIds.getIntId.data
      keyed.offset.size shouldBe pks.size

      val older = Comparison(ref(age), GreaterThan, IntegerValue(28))
      val younger = Comparison(ref(age), LessThan, IntegerValue(60))
      def named(value: String) =
        Comparison(ref(name), EqualTo, StringValue(value))
      Seq[(PredicateExpr, Set[Long])](
        And(older, younger) -> Set(3L, 4L, 5L),
        StartsWith(ref(name), "c") -> Set(3L),
        EndsWith(ref(name), "ol") -> Set(3L),
        In(ref(pk), Vector(6L, 2L, 6L).map(IntegerValue)) -> Set(2L, 6L),
        In(ref(name), Vector("frank", "bob").map(StringValue)) -> Set(2L, 6L),
        And(older, Not(named("dave"))) -> Set(3L, 5L, 6L),
        Or(
          Comparison(ref(age), LessThan, IntegerValue(15)),
          named("frank")
        ) -> Set(1L, 6L),
        Comparison(ref(name), GreaterThanOrEqual, StringValue("dave")) ->
          Set(4L, 5L, 6L),
        Comparison(ref(age), LessThan, IntegerValue(0)) -> Set.empty[Long]
      ).foreach { case (predicate, want) =>
        withClue(predicate.toString) {
          retrieved(collection, segment, predicate) shouldBe want
        }
      }
    } finally {
      SegcoreNative.deleteSegment(segment)
      SegcoreNative.deleteCollection(collection)
    }
  }

  test("segcore rejects a plan over a field the collection does not have") {
    SegcoreRuntime.assumeAvailable()
    SegcoreRuntime.work
    val collection = SegcoreNative.newCollection(
      CollectionSchema(name = "unknown", fields = fields).toByteArray
    )
    try {
      val plan = PlanNode(
        node = PlanNode.Node.Query(
          QueryPlanNode(predicates =
            Some(
              Expr(
                Expr.Expr.UnaryRangeExpr(
                  UnaryRangeExpr(
                    columnInfo = Some(
                      ColumnInfo(fieldId = 999, dataType = MilvusDataType.Int64)
                    ),
                    op = OpType.Equal,
                    value = Some(GenericValue(GenericValue.Val.Int64Val(1L)))
                  )
                )
              )
            )
          )
        )
      )
      val failure = the[SegcoreException] thrownBy SegcoreNative
        .createRetrievePlan(collection, plan.toByteArray)
      failure.code should be > 0
      info(s"segcore code ${failure.code}: ${failure.getMessage}")
    } finally SegcoreNative.deleteCollection(collection)
  }

  test("a cancelled load stops with an error and leaves the segment loadable") {
    SegcoreRuntime.assumeAvailable()
    val root = Files.createTempDirectory(SegcoreRuntime.work, "cancel")
    val fixture =
      new SegcoreFixture("api", root.resolve("api"), Seq(100L, 102L))
    try {
      val expected = fixture.expected.find(_.id == 31L).get
      val segment = fixture.newSegment(31L)
      try {
        val cancellation = SegcoreNative.newLoadCancellation()
        try {
          SegcoreNative.cancelLoad(cancellation)
          val failure = the[SegcoreException] thrownBy SegcoreNative.load(
            segment,
            cancellation
          )
          info(s"segcore code ${failure.code}: ${failure.getMessage}")
        } finally SegcoreNative.releaseLoadCancellation(cancellation)
        SegcoreNative.load(segment, 0L)
        val plan = SegcorePlan
          .retrieve(
            Comparison(
              FieldRef(102L, MilvusDataType.Int64),
              EqualTo,
              IntegerValue(0)
            ),
            fixture.fields
          )
          .get
        SegcoreRuntime
          .rows(
            SegcoreRuntime.retrieve(fixture.collection, segment, plan, 150L)
          )
          .map(expected.rows(_).id) shouldBe
          expected.rows.filter(_.category.contains(0L)).map(_.id).toSet
      } finally SegcoreNative.deleteSegment(segment)
    } finally fixture.close()
  }

  test("segcore's libraries come from its own build, never the public bundle") {
    SegcoreRuntime.assumeAvailable()
    SegcoreRuntime.work
    val built: Path = SegcoreRuntime.libraryDir.get.toRealPath()
    val shipped =
      Files.list(built).toArray.map(_.asInstanceOf[Path].getFileName)
    val mapped = SegcoreRuntime.mappedLibraries().map(_.toRealPath())
    mapped.filter(_.getParent == built).map(_.getFileName.toString) should
      contain allOf ("libmilvus-segcore-jni.so", "libmilvus_core.so",
      "libmilvus-storage.so")
    // A library the segcore build ships is mapped from there and nowhere
    // else, and nothing comes from an extracted native bundle.
    mapped
      .filter(path =>
        shipped.contains(path.getFileName) && path.getParent != built
      ) shouldBe empty
    mapped.filter(_.toString.contains("/milvus-native-")) shouldBe empty
    // The build keeps the public bundle off this JVM's class path.
    getClass.getClassLoader.getResource(
      "native/milvus/2/linux-x86_64/manifest.properties"
    ) shouldBe null
  }

  private def longs(field: FieldSchema, values: Seq[Long]): FieldData =
    FieldData(
      `type` = field.dataType,
      fieldName = field.name,
      fieldId = field.fieldID,
      field = FieldData.Field.Scalars(
        ScalarField(data = ScalarField.Data.LongData(LongArray(data = values)))
      )
    )
}
