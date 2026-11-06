package com.zilliz.milvus.storage.expr

import java.nio.charset.StandardCharsets.UTF_8

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
  QueryPlanNode,
  TermExpr,
  UnaryExpr,
  UnaryRangeExpr
}

/** segcore's form of a pushed predicate: the `planpb` messages segcore takes,
  * built from a [[PredicateExpr]] instead of an expression string for the Go
  * plan parser.
  *
  * The parser's obligations for an online query are met here. Each field is
  * checked against the collection schema and described as the parser describes
  * it, each literal is checked against the field's type, and `IN` lists are
  * sorted and distinct, which segcore relies on.
  *
  * Translating is not admitting. A predicate translates when segcore can state
  * it; the connector hands it to segcore only for the (predicate, type) pairs
  * that differential tests against [[PredicateEvaluator]] have admitted, since
  * a pushed predicate is correct when it matches Spark. Outside the forms below
  * a predicate does not translate and stays with the JVM evaluator:
  * floating-point fields, whose NaN ordering Spark and Milvus define
  * differently; null literals, except in null-safe equality with null, which is
  * a null test; null-safe equality with a value; ordering on booleans; integers
  * outside the field's range; and literals of another type than the field's.
  */
object SegcorePlan {

  /** A retrieve plan for the rows `predicate` admits, returning
    * `outputFieldIds`, or `None` when the predicate does not translate.
    *
    * @param fields
    *   the collection's fields, as the snapshot schema lists them
    */
  def retrieve(
      predicate: PredicateExpr,
      fields: Seq[FieldSchema],
      outputFieldIds: Seq[Long]
  ): Option[PlanNode] =
    expr(predicate, fields).map { predicates =>
      PlanNode(
        node =
          PlanNode.Node.Query(QueryPlanNode(predicates = Some(predicates))),
        outputFieldIds = outputFieldIds
      )
    }

  /** The `planpb.Expr` for `predicate`, or `None` when it does not translate.
    *
    * @throws IllegalArgumentException
    *   when the predicate reads a field `fields` does not list, or reads it as
    *   another type: the predicate was built against another schema
    */
  def expr(predicate: PredicateExpr, fields: Seq[FieldSchema]): Option[Expr] =
    translate(predicate, fields.map(field => field.fieldID -> field).toMap)

  private def translate(
      predicate: PredicateExpr,
      fields: Map[Long, FieldSchema]
  ): Option[Expr] = predicate match {
    case Comparison(field, EqualNullSafe, NullValue) =>
      Some(nullTest(column(field, fields), NullExpr.NullOp.IsNull))
    case Comparison(_, EqualNullSafe, _) => None
    case Comparison(field, operator, literal) =>
      val info = column(field, fields)
      for {
        op <- opType(operator, field.dataType)
        value <- genericValue(field.dataType, literal)
      } yield Expr(
        Expr.Expr.UnaryRangeExpr(
          UnaryRangeExpr(columnInfo = Some(info), op = op, value = Some(value))
        )
      )
    case In(field, literals) =>
      val info = column(field, fields)
      termValues(field.dataType, literals).map { values =>
        Expr(
          Expr.Expr.TermExpr(TermExpr(columnInfo = Some(info), values = values))
        )
      }
    case IsNull(field) =>
      Some(nullTest(column(field, fields), NullExpr.NullOp.IsNull))
    case IsNotNull(field) =>
      Some(nullTest(column(field, fields), NullExpr.NullOp.IsNotNull))
    case StartsWith(field, prefix) =>
      stringMatch(column(field, fields), OpType.PrefixMatch, prefix)
    case EndsWith(field, suffix) =>
      stringMatch(column(field, fields), OpType.PostfixMatch, suffix)
    case And(left, right) =>
      binary(BinaryExpr.BinaryOp.LogicalAnd, left, right, fields)
    case Or(left, right) =>
      binary(BinaryExpr.BinaryOp.LogicalOr, left, right, fields)
    case Not(child) =>
      translate(child, fields).map { inner =>
        Expr(
          Expr.Expr.UnaryExpr(
            UnaryExpr(op = UnaryExpr.UnaryOp.Not, child = Some(inner))
          )
        )
      }
  }

  /** The column as the plan parser describes a field it resolved. */
  private def column(
      field: FieldRef,
      fields: Map[Long, FieldSchema]
  ): ColumnInfo = {
    val schema = fields.getOrElse(
      field.fieldId,
      throw new IllegalArgumentException(
        s"field ${field.fieldId} is not in the collection schema"
      )
    )
    require(
      schema.dataType == field.dataType,
      s"field ${field.fieldId} is ${schema.dataType} in the collection schema, " +
        s"not ${field.dataType}"
    )
    ColumnInfo(
      fieldId = schema.fieldID,
      dataType = schema.dataType,
      isPrimaryKey = schema.isPrimaryKey,
      isAutoID = schema.autoID,
      isPartitionKey = schema.isPartitionKey,
      isClusteringKey = schema.isClusteringKey,
      elementType = schema.elementType,
      nullable = schema.nullable
    )
  }

  private def nullTest(info: ColumnInfo, op: NullExpr.NullOp): Expr =
    Expr(Expr.Expr.NullExpr(NullExpr(columnInfo = Some(info), op = op)))

  private def stringMatch(
      info: ColumnInfo,
      op: OpType,
      pattern: String
  ): Option[Expr] =
    if (!isString(info.dataType)) None
    else
      Some(
        Expr(
          Expr.Expr.UnaryRangeExpr(
            UnaryRangeExpr(
              columnInfo = Some(info),
              op = op,
              value = Some(GenericValue(GenericValue.Val.StringVal(pattern)))
            )
          )
        )
      )

  private def binary(
      op: BinaryExpr.BinaryOp,
      left: PredicateExpr,
      right: PredicateExpr,
      fields: Map[Long, FieldSchema]
  ): Option[Expr] =
    for {
      l <- translate(left, fields)
      r <- translate(right, fields)
    } yield Expr(
      Expr.Expr.BinaryExpr(BinaryExpr(op = op, left = Some(l), right = Some(r)))
    )

  /** Booleans compare for equality only; integers and strings also order. */
  private def opType(
      operator: ComparisonOperator,
      dataType: MilvusDataType
  ): Option[OpType] = {
    val ordered = integerRange(dataType).nonEmpty || isString(dataType)
    operator match {
      case EqualTo if ordered || dataType == MilvusDataType.Bool =>
        Some(OpType.Equal)
      case NotEqualTo if ordered || dataType == MilvusDataType.Bool =>
        Some(OpType.NotEqual)
      case LessThan if ordered           => Some(OpType.LessThan)
      case LessThanOrEqual if ordered    => Some(OpType.LessEqual)
      case GreaterThan if ordered        => Some(OpType.GreaterThan)
      case GreaterThanOrEqual if ordered => Some(OpType.GreaterEqual)
      case _                             => None
    }
  }

  /** The literal as a value of the field's type, or `None` when it is null, of
    * another type, or outside the range of an integer field.
    */
  private def genericValue(
      dataType: MilvusDataType,
      literal: Literal
  ): Option[GenericValue] = literal match {
    case IntegerValue(value) =>
      integerRange(dataType)
        .filter(range => value >= range._1 && value <= range._2)
        .map(_ => GenericValue(GenericValue.Val.Int64Val(value)))
    case BooleanValue(value) if dataType == MilvusDataType.Bool =>
      Some(GenericValue(GenericValue.Val.BoolVal(value)))
    case StringValue(value) if isString(dataType) =>
      Some(GenericValue(GenericValue.Val.StringVal(value)))
    case _ => None
  }

  /** An `IN` list as segcore takes it: values of the field's type, sorted and
    * distinct. Integers sort by value, strings by their UTF-8 bytes, which is
    * how Milvus orders strings; `None` when any literal does not translate or
    * the list is empty.
    */
  private def termValues(
      dataType: MilvusDataType,
      literals: Vector[Literal]
  ): Option[Seq[GenericValue]] = {
    val values = literals.map(genericValue(dataType, _))
    if (literals.isEmpty || values.exists(_.isEmpty)) None
    else
      Some(values.flatten.distinct.sortWith { (a, b) =>
        (a.`val`, b.`val`) match {
          case (GenericValue.Val.Int64Val(x), GenericValue.Val.Int64Val(y)) =>
            x < y
          case (GenericValue.Val.BoolVal(x), GenericValue.Val.BoolVal(y)) =>
            !x && y
          case (GenericValue.Val.StringVal(x), GenericValue.Val.StringVal(y)) =>
            compareUtf8(x, y) < 0
          case (x, y) =>
            throw new IllegalStateException(s"mixed IN values $x and $y")
        }
      })
  }

  private def compareUtf8(a: String, b: String): Int = {
    val x = a.getBytes(UTF_8)
    val y = b.getBytes(UTF_8)
    val n = math.min(x.length, y.length)
    var i = 0
    while (i < n) {
      val c = (x(i) & 0xff) - (y(i) & 0xff)
      if (c != 0) return c
      i += 1
    }
    x.length - y.length
  }

  private def integerRange(dataType: MilvusDataType): Option[(Long, Long)] =
    dataType match {
      case MilvusDataType.Int8 =>
        Some((Byte.MinValue.toLong, Byte.MaxValue.toLong))
      case MilvusDataType.Int16 =>
        Some((Short.MinValue.toLong, Short.MaxValue.toLong))
      case MilvusDataType.Int32 =>
        Some((Int.MinValue.toLong, Int.MaxValue.toLong))
      case MilvusDataType.Int64 => Some((Long.MinValue, Long.MaxValue))
      case _                    => None
    }

  private def isString(dataType: MilvusDataType): Boolean =
    dataType == MilvusDataType.VarChar || dataType == MilvusDataType.String
}
