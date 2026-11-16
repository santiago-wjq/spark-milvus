package com.zilliz.milvus.storage.segcore

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.expr.{
  And,
  Comparison,
  EndsWith,
  FieldRef,
  In,
  IsNotNull,
  IsNull,
  Not,
  Or,
  PredicateExpr,
  StartsWith
}
import com.zilliz.milvus.storage.expr.ComparisonOperator._
import com.zilliz.milvus.storage.expr.Literal._
import io.milvus.grpc.schema.{DataType => MilvusDataType, FieldSchema}

import milvus.proto.plan.plan.{
  BinaryExpr,
  ColumnInfo,
  Expr,
  GenericValue,
  NullExpr,
  OpType,
  PlanNode,
  UnaryExpr
}

class SegcorePlanTest extends AnyFunSuite with Matchers {

  private val pk = FieldSchema(
    fieldID = 100,
    name = "id",
    isPrimaryKey = true,
    dataType = MilvusDataType.Int64
  )
  private val age = FieldSchema(
    fieldID = 101,
    name = "age",
    dataType = MilvusDataType.Int16,
    nullable = true,
    isPartitionKey = true
  )
  private val name = FieldSchema(
    fieldID = 102,
    name = "name",
    dataType = MilvusDataType.VarChar,
    isClusteringKey = true
  )
  private val flag =
    FieldSchema(fieldID = 103, name = "flag", dataType = MilvusDataType.Bool)
  private val score =
    FieldSchema(fieldID = 104, name = "score", dataType = MilvusDataType.Double)
  private val fields = Seq(pk, age, name, flag, score)

  private def ref(field: FieldSchema): FieldRef =
    FieldRef(field.fieldID, field.dataType)

  private def translate(predicate: PredicateExpr): Option[Expr] =
    SegcorePlan.expr(predicate, fields)

  private def int64(value: Long): GenericValue =
    GenericValue(GenericValue.Val.Int64Val(value))

  private def string(value: String): GenericValue =
    GenericValue(GenericValue.Val.StringVal(value))

  private val older = Comparison(ref(age), GreaterThan, IntegerValue(28))
  private val dave = Comparison(ref(name), EqualTo, StringValue("dave"))
  private val floating = Comparison(ref(score), GreaterThan, DoubleValue(0.5))

  test("a comparison is a unary range over the column the parser would name") {
    val range = translate(older).get.getUnaryRangeExpr
    range.op shouldBe OpType.GreaterThan
    range.getValue shouldBe int64(28)
    range.getColumnInfo shouldBe ColumnInfo(
      fieldId = 101,
      dataType = MilvusDataType.Int16,
      isPartitionKey = true,
      nullable = true
    )
    translate(
      Comparison(ref(pk), EqualTo, IntegerValue(7))
    ).get.getUnaryRangeExpr.getColumnInfo.isPrimaryKey shouldBe true
    translate(
      Comparison(ref(name), EqualTo, StringValue("a"))
    ).get.getUnaryRangeExpr.getColumnInfo.isClusteringKey shouldBe true
  }

  test("each comparison operator has its OpType") {
    Seq(
      EqualTo -> OpType.Equal,
      NotEqualTo -> OpType.NotEqual,
      LessThan -> OpType.LessThan,
      LessThanOrEqual -> OpType.LessEqual,
      GreaterThan -> OpType.GreaterThan,
      GreaterThanOrEqual -> OpType.GreaterEqual
    ).foreach { case (operator, op) =>
      translate(
        Comparison(ref(pk), operator, IntegerValue(1))
      ).get.getUnaryRangeExpr.op shouldBe op
      translate(
        Comparison(ref(name), operator, StringValue("m"))
      ).get.getUnaryRangeExpr.op shouldBe op
    }
  }

  test("booleans compare for equality and do not order") {
    val equal = translate(Comparison(ref(flag), EqualTo, BooleanValue(true)))
    equal.get.getUnaryRangeExpr.getValue shouldBe GenericValue(
      GenericValue.Val.BoolVal(true)
    )
    translate(
      Comparison(ref(flag), NotEqualTo, BooleanValue(false))
    ).get.getUnaryRangeExpr.op shouldBe OpType.NotEqual
    translate(Comparison(ref(flag), LessThan, BooleanValue(true))) shouldBe None
  }

  test("IN lists are sorted and distinct, strings by their UTF-8 bytes") {
    val ints = translate(
      In(ref(pk), Vector(IntegerValue(6), IntegerValue(2), IntegerValue(6)))
    ).get.getTermExpr
    ints.values shouldBe Seq(int64(2), int64(6))
    ints.getColumnInfo.fieldId shouldBe 100

    // U+FFFF sorts after U+1F600 in UTF-16 code units and before it in
    // UTF-8 bytes (EF BF BF < F0 9F 98 80).
    val bmp = "\uFFFF"
    val supplementary = "\uD83D\uDE00"
    val strings = translate(
      In(
        ref(name),
        Vector(StringValue(supplementary), StringValue(bmp), StringValue("b"))
      )
    ).get.getTermExpr
    strings.values shouldBe Seq(string("b"), string(bmp), string(supplementary))
  }

  test("null tests are null expressions, null-safe equality with null too") {
    translate(
      IsNull(ref(age))
    ).get.getNullExpr.op shouldBe NullExpr.NullOp.IsNull
    translate(
      IsNotNull(ref(age))
    ).get.getNullExpr.op shouldBe NullExpr.NullOp.IsNotNull
    val nullSafe = translate(Comparison(ref(age), EqualNullSafe, NullValue))
    nullSafe.get.getNullExpr.op shouldBe NullExpr.NullOp.IsNull
    nullSafe.get.getNullExpr.getColumnInfo.nullable shouldBe true
  }

  test("prefix and suffix are pattern matches on string fields only") {
    val prefix = translate(StartsWith(ref(name), "ca")).get.getUnaryRangeExpr
    prefix.op shouldBe OpType.PrefixMatch
    prefix.getValue shouldBe string("ca")
    translate(
      EndsWith(ref(name), "ol")
    ).get.getUnaryRangeExpr.op shouldBe OpType.PostfixMatch
    translate(StartsWith(ref(pk), "1")) shouldBe None
  }

  test("AND, OR and NOT keep their tree, and translate only whole") {
    val tree = translate(Or(And(older, Not(dave)), IsNull(ref(age)))).get

    val or = tree.getBinaryExpr
    or.op shouldBe BinaryExpr.BinaryOp.LogicalOr
    or.getRight.getNullExpr.op shouldBe NullExpr.NullOp.IsNull
    val and = or.getLeft.getBinaryExpr
    and.op shouldBe BinaryExpr.BinaryOp.LogicalAnd
    and.getLeft.getUnaryRangeExpr.getValue shouldBe int64(28)
    val not = and.getRight.getUnaryExpr
    not.op shouldBe UnaryExpr.UnaryOp.Not
    not.getChild.getUnaryRangeExpr.getValue shouldBe string("dave")

    translate(And(older, floating)) shouldBe None
    translate(Or(floating, older)) shouldBe None
    translate(Not(floating)) shouldBe None
  }

  test("the top-level conjuncts are the ANDs' leaves, left to right") {
    val nameIsNull = IsNull(ref(name))
    SegcorePlan.conjuncts(
      And(
        And(older, Or(dave, floating)),
        And(Not(And(dave, older)), nameIsNull)
      )
    ) shouldBe Vector(
      older,
      Or(dave, floating),
      Not(And(dave, older)),
      nameIsNull
    )
    SegcorePlan.conjuncts(Or(older, dave)) shouldBe Vector(Or(older, dave))
    SegcorePlan.conjuncts(older) shouldBe Vector(older)
  }

  test("a conjunct translates on its own when the whole tree does not") {
    val whole = And(older, And(floating, Not(dave)))
    SegcorePlan.retrieve(whole, fields) shouldBe None
    val plans = SegcorePlan
      .conjuncts(whole)
      .map(SegcorePlan.retrieve(_, fields))
    plans.map(_.isDefined) shouldBe Vector(true, false, true)
    plans.head.get.getQuery.getPredicates shouldBe translate(older).get
    plans.last.get.getQuery.getPredicates.getUnaryExpr.op shouldBe
      UnaryExpr.UnaryOp.Not
  }

  test("forms whose semantics are not settled do not translate") {
    // NaN ordering differs between Spark and Milvus.
    translate(Comparison(ref(score), LessThan, DoubleValue(1.0))) shouldBe None
    translate(In(ref(score), Vector(DoubleValue(1.0)))) shouldBe None
    // A null literal makes the comparison null; IN with null is null unless a
    // value matches.
    translate(Comparison(ref(age), EqualTo, NullValue)) shouldBe None
    translate(In(ref(age), Vector(IntegerValue(1), NullValue))) shouldBe None
    // Null-safe equality with a value is not null on null rows.
    translate(
      Comparison(ref(age), EqualNullSafe, IntegerValue(1))
    ) shouldBe None
    // An Int16 field cannot hold 40000.
    translate(Comparison(ref(age), LessThan, IntegerValue(40000))) shouldBe None
    translate(
      In(ref(age), Vector(IntegerValue(1), IntegerValue(40000)))
    ) shouldBe None
    // Literals of another type than the field's.
    translate(Comparison(ref(pk), EqualTo, StringValue("7"))) shouldBe None
    translate(Comparison(ref(name), EqualTo, IntegerValue(7))) shouldBe None
    translate(Comparison(ref(flag), EqualTo, IntegerValue(1))) shouldBe None
    translate(In(ref(pk), Vector.empty)) shouldBe None
  }

  test("a field outside the schema, or read as another type, is an error") {
    an[IllegalArgumentException] should be thrownBy translate(
      IsNull(FieldRef(999, MilvusDataType.Int64))
    )
    an[IllegalArgumentException] should be thrownBy translate(
      Comparison(FieldRef(101, MilvusDataType.Int64), EqualTo, IntegerValue(1))
    )
  }

  test("a retrieve plan is a query plan with no output field and no limit") {
    val plan = SegcorePlan.retrieve(IsNotNull(ref(age)), fields).get
    plan.node shouldBe a[PlanNode.Node.Query]
    plan.getQuery.getPredicates.getNullExpr.op shouldBe NullExpr.NullOp.IsNotNull
    plan.getQuery.limit shouldBe 0L
    plan.outputFieldIds shouldBe empty
    PlanNode.parseFrom(plan.toByteArray) shouldBe plan

    SegcorePlan.retrieve(
      Comparison(ref(score), EqualTo, DoubleValue(1.0)),
      fields
    ) shouldBe None
  }
}
