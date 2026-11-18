#!/usr/bin/env python3
"""Evaluate exported search rankings and relate problems to Collection data.

Relevance judgments must list every relevant entity for each evaluated query
and explicitly label every evaluated hit. Metadata misses are reported without
changing relevance metrics. This example does not perform vector searches.
"""

import argparse
import json
import math
from pathlib import Path


DATA = Path(__file__).resolve().parents[1] / "data/retrieval-evaluation"


def _reject_rows(frame, message):
    if frame.limit(1).count():
        raise ValueError(message)


def _check_keys(frame, keys, name):
    from pyspark.sql import functions as F

    invalid = F.col("count") > 1
    for key in keys:
        invalid = invalid | F.col(key).isNull()
    _reject_rows(frame.groupBy(*keys).count().where(invalid),
                 f"{name} requires non-null, unique keys: {', '.join(keys)}")


def evaluate(queries, hits, judgments, collection, catalog, k):
    """Return per_query, by_group, hit_details and issues as Spark DataFrames.

    Precision divides by k even for short result lists. Recall divides by all
    positive judgments, assumed exhaustive. A query with no hits scores zero.
    Exported ranks must be a complete prefix 1..n before the top-k cut.
    """
    from pyspark.sql import functions as F

    if not isinstance(k, int) or isinstance(k, bool) or k <= 0:
        raise ValueError("k must be a positive integer")
    queries = queries.select("query_id", "query_text", "query_group")
    hits = hits.select("query_id", "entity_id", "rank")
    judgments = judgments.select("query_id", "entity_id", "relevant")
    collection = collection.select("entity_id", "category")
    catalog = catalog.select("entity_id", "product_name")

    _check_keys(queries, ["query_id"], "queries")
    _check_keys(hits, ["query_id", "rank"], "hits")
    _check_keys(hits, ["query_id", "entity_id"], "hits")
    _check_keys(judgments, ["query_id", "entity_id"], "judgments")
    _check_keys(collection, ["entity_id"], "collection")
    _check_keys(catalog, ["entity_id"], "catalog")
    if not queries.limit(1).count():
        raise ValueError("queries must contain at least one query")
    _reject_rows(hits.where((F.col("rank") <= 0)
                           | (F.col("rank") != F.col("rank").cast("long"))),
                 "hit ranks must be positive integers")
    _reject_rows(judgments.where(F.col("relevant").isNull()
                                | ~F.col("relevant").isin(0, 1)),
                 "judgment relevance must be 0 or 1")
    mentioned_queries = hits.select("query_id").union(judgments.select("query_id"))
    _reject_rows(mentioned_queries.join(queries.select("query_id"), "query_id", "left_anti"),
                 "hits and judgments must reference known query IDs")
    rank_ranges = hits.groupBy("query_id").agg(
        F.min("rank").alias("first_rank"), F.max("rank").alias("last_rank"),
        F.count("*").alias("hit_count"))
    _reject_rows(rank_ranges.where((F.col("first_rank") != 1)
                                  | (F.col("last_rank") != F.col("hit_count"))),
                 "hit ranks must be consecutive from 1 before selecting top k")
    positive_counts = judgments.where(F.col("relevant") == 1).groupBy("query_id").agg(
        F.count("*").alias("relevant_total"))
    _reject_rows(queries.join(positive_counts, "query_id", "left_anti"),
                 "every query requires at least one relevant judgment for recall")
    top_hits = hits.where(F.col("rank") <= k)
    _reject_rows(top_hits.join(judgments, ["query_id", "entity_id"], "left_anti"),
                 "every top-k hit requires an explicit relevance judgment")

    # Presence flags distinguish missing metadata from present rows with NULL fields.
    details = (top_hits.join(judgments, ["query_id", "entity_id"])
               .join(queries, "query_id")
               .join(collection.withColumn("_in_collection", F.lit(True)), "entity_id", "left")
               .join(catalog.withColumn("_in_catalog", F.lit(True)), "entity_id", "left")
               .withColumn("missing_collection", F.col("_in_collection").isNull())
               .withColumn("missing_catalog", F.col("_in_catalog").isNull())
               .select("query_id", "entity_id", "rank", "relevant", "query_text", "query_group",
                       "category", "product_name", "missing_collection", "missing_catalog"))
    hit_counts = details.groupBy("query_id").agg(
        F.count("*").alias("returned_hits"), F.sum("relevant").alias("relevant_hits"),
        F.min(F.when(F.col("relevant") == 1, F.col("rank"))).alias("first_relevant_rank"),
        F.sum(F.col("missing_collection").cast("long")).alias("unmatched_collection_hits"),
        F.sum(F.col("missing_catalog").cast("long")).alias("unmatched_catalog_hits"))
    per_query = (queries.join(positive_counts, "query_id")
                 .join(hit_counts, "query_id", "left")
                 .fillna(0, subset=["returned_hits", "relevant_hits",
                                    "unmatched_collection_hits", "unmatched_catalog_hits"])
                 .withColumn("precision_at_k", F.col("relevant_hits") / F.lit(k))
                 .withColumn("recall_at_k", F.col("relevant_hits") / F.col("relevant_total"))
                 .withColumn("reciprocal_rank_at_k", F.when(
                     F.col("first_relevant_rank").isNotNull(),
                     F.lit(1.0) / F.col("first_relevant_rank")).otherwise(F.lit(0.0)))
                 .select("query_id", "query_text", "query_group", "returned_hits", "relevant_hits",
                         "relevant_total", "precision_at_k", "recall_at_k", "reciprocal_rank_at_k",
                         "unmatched_collection_hits", "unmatched_catalog_hits"))
    by_group = per_query.groupBy("query_group").agg(
        F.count("*").alias("query_count"),
        F.sum(F.when(F.col("returned_hits") == 0, 1).otherwise(0)).alias("zero_hit_queries"),
        F.sum("unmatched_collection_hits").alias("unmatched_collection_hits"),
        F.sum("unmatched_catalog_hits").alias("unmatched_catalog_hits"),
        F.avg("precision_at_k").alias("precision_at_k"),
        F.avg("recall_at_k").alias("recall_at_k"),
        F.avg("reciprocal_rank_at_k").alias("reciprocal_rank_at_k"))
    issues = details.where("missing_collection").select(
        "query_id", "entity_id", "rank", F.lit("missing_collection").alias("issue")).unionByName(
            details.where("missing_catalog").select(
                "query_id", "entity_id", "rank", F.lit("missing_catalog").alias("issue")))
    issues = issues.unionByName(details.where(F.col("relevant") == 0).select(
        "query_id", "entity_id", "rank", F.lit("non_relevant_hit").alias("issue"))).unionByName(
            per_query.where(F.col("returned_hits") == 0).select(
                "query_id", F.lit(None).cast("long").alias("entity_id"),
                F.lit(None).cast("int").alias("rank"), F.lit("no_hits").alias("issue")))
    return {"per_query": per_query, "by_group": by_group, "hit_details": details, "issues": issues}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--options", type=Path, help="Collection read options JSON")
    for name in ("queries", "hits", "judgments", "catalog"):
        parser.add_argument(f"--{name}", help=f"{name.title()} JSON Lines path")
    parser.add_argument("--id-column", default="id", help="Collection entity ID field")
    parser.add_argument("--category-column", default="category", help="Collection category field")
    parser.add_argument("--k", type=int, default=3, help="Evaluate the first k hits (default: 3)")
    parser.add_argument("--output", required=True, help="Fresh output directory for JSON reports")
    parser.add_argument("--verify-example", action="store_true", help="Check the fixed example results")
    args = parser.parse_args(argv)
    inputs = [getattr(args, name) for name in ("queries", "hits", "judgments", "catalog")]
    custom = args.options is not None
    if (custom and not all(inputs)) or (not custom and any(inputs)):
        parser.error("Provide --options and all four input paths together, or use the fixed example")
    if args.k <= 0:
        parser.error("--k must be positive")
    if not args.output.strip():
        parser.error("--output must not be blank")
    if args.verify_example and (custom or args.k != 3):
        parser.error("--verify-example requires the fixed example and --k 3")
    args.fixture = not custom
    for name in ("queries", "hits", "judgments", "catalog"):
        if args.fixture:
            setattr(args, name, str(DATA / f"{name}.jsonl"))
    return args


def verify_example(per_query, expected_path):
    """Compare the small bundled example's per-query metrics with fixed expectations."""
    expected = json.loads(Path(expected_path).read_text())["per_query"]
    if not expected or len(expected) > 1000:
        raise ValueError("Example expectations must contain between 1 and 1000 queries")
    rows = per_query.limit(len(expected) + 1).collect()
    actual = {row.query_id: row.asDict() for row in rows}
    if len(rows) != len(expected) or set(actual) != {row["query_id"] for row in expected}:
        raise AssertionError("Example query IDs differ from expectations")
    for wanted in expected:
        for field, value in wanted.items():
            found = actual[wanted["query_id"]][field]
            equal = (math.isclose(found, value, rel_tol=1e-9, abs_tol=1e-9)
                     if isinstance(value, float) else found == value)
            if not equal:
                raise AssertionError(f"Unexpected {field} for query {wanted['query_id']}: {found} != {value}")


def _require_fresh_output(spark, output):
    path = spark._jvm.org.apache.hadoop.fs.Path(output)
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if filesystem.exists(path):
        raise ValueError("--output must be a new directory; the requested path already exists")


def main(argv=None):
    args = parse_args(argv)
    from common import fixture_input, read_options
    from pyspark.sql import SparkSession, functions as F, types as T

    options = fixture_input()[0] if args.fixture else read_options(args.options)
    spark = SparkSession.builder.appName("Milvus retrieval evaluation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if not spark.version.startswith("4.0."):
            raise ValueError("This example requires Spark 4.0 / Scala 2.13")
        master = spark.sparkContext.master
        if args.fixture and not (master == "local" or master.startswith("local[")):
            raise ValueError("The fixed example requires local Spark; use --options and custom inputs for a cluster")
        _require_fresh_output(spark, args.output)
        definitions = {
            "queries": [("query_id", T.StringType()), ("query_text", T.StringType()),
                        ("query_group", T.StringType())],
            "hits": [("query_id", T.StringType()), ("entity_id", T.LongType()), ("rank", T.IntegerType())],
            "judgments": [("query_id", T.StringType()), ("entity_id", T.LongType()),
                          ("relevant", T.IntegerType())],
            "catalog": [("entity_id", T.LongType()), ("product_name", T.StringType())],
        }
        frames = {}
        for name, fields in definitions.items():
            schema = T.StructType([T.StructField(field, kind, True) for field, kind in fields])
            frames[name] = spark.read.schema(schema).option("mode", "FAILFAST").json(getattr(args, name))
        source = spark.read.format("milvus").options(**options).load()
        if not isinstance(source.schema[args.id_column].dataType,
                          (T.LongType, T.IntegerType, T.ShortType, T.ByteType)):
            raise ValueError("The Collection ID field must contain integer entity IDs")
        collection = source.select(F.col(args.id_column).cast("long").alias("entity_id"),
                                   F.col(args.category_column).alias("category"))
        reports = evaluate(frames["queries"], frames["hits"], frames["judgments"],
                           collection, frames["catalog"], args.k)
        if args.verify_example:
            verify_example(reports["per_query"], DATA / "expected.json")
        _require_fresh_output(spark, args.output)
        for name, frame in reports.items():
            frame.write.mode("errorifexists").json(f"{args.output.rstrip('/')}/{name}")
        reports["by_group"].orderBy("query_group").show(20, truncate=False)
        print("Reports written: per_query, by_group, hit_details, issues")
        if args.verify_example:
            print("Fixed example verification passed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
