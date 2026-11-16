"""Exercise retrieval metrics and malformed exports with a real local Spark JVM."""

import importlib.util
import json
from pathlib import Path
import unittest

from pyspark.sql import SparkSession, functions as F


ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "retrieval_evaluation", ROOT / "demo/python/04_retrieval_evaluation.py")
demo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(demo)


class RetrievalMetricsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("retrieval-metrics-test")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        definitions = {
            "queries": "query_id STRING, query_text STRING, query_group STRING",
            "hits": "query_id STRING, entity_id LONG, rank INT",
            "judgments": "query_id STRING, entity_id LONG, relevant INT",
            "catalog": "entity_id LONG, product_name STRING",
        }
        cls.inputs = {name: cls.spark.read.schema(schema).json(str(demo.DATA / (name + ".jsonl")))
                      for name, schema in definitions.items()}
        fixture = json.loads((ROOT / "core/src/test/resources/read-fixtures/collections/basic/expected.json").read_text())
        rows = [(row["id"], row["category"]) for segment in fixture["segments"]
                for row in segment["rows"] if row["id"] not in segment["deletedIds"]]
        cls.inputs["collection"] = cls.spark.createDataFrame(rows, "entity_id LONG, category INT")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def evaluate(self, **overrides):
        return demo.evaluate(**{**self.inputs, **overrides}, k=3)

    def test_metrics_preserve_empty_queries_and_missing_metadata(self):
        reports = self.evaluate()
        demo.verify_example(reports["per_query"], demo.DATA / "expected.json")
        groups = {row.query_group: row for row in reports["by_group"].collect()}
        self.assertEqual(groups["catalog_refresh"].zero_hit_queries, 1)
        self.assertAlmostEqual(groups["catalog_refresh"].precision_at_k, 1 / 3)
        self.assertAlmostEqual(groups["discovery"].precision_at_k, .5)
        self.assertAlmostEqual(groups["discovery"].recall_at_k, .75)
        self.assertAlmostEqual(groups["discovery"].reciprocal_rank_at_k, 2 / 3)
        issues = {(row.query_id, row.entity_id, row.issue) for row in reports["issues"].collect()}
        self.assertIn(("q3", 3002, "missing_collection"), issues)
        self.assertIn(("q3", 3001, "missing_catalog"), issues)
        self.assertIn(("q4", None, "no_hits"), issues)
        self.assertIn(("q2", 3000, "non_relevant_hit"), issues)
        nullable = reports["hit_details"].where("query_id = 'q3' AND entity_id = 3001").first()
        self.assertIsNone(nullable.category)
        self.assertFalse(nullable.missing_collection)

    def test_short_result_list_still_uses_k_as_precision_denominator(self):
        hits = self.inputs["hits"].where("NOT (query_id = 'q1' AND rank = 3)")
        row = self.evaluate(hits=hits)["per_query"].where("query_id = 'q1'").first()
        self.assertEqual(row.returned_hits, 2)
        self.assertAlmostEqual(row.precision_at_k, 2 / 3)
        self.assertAlmostEqual(row.recall_at_k, 1)

    def test_invalid_exports_fail_instead_of_inflating_metrics(self):
        hits, judgments = self.inputs["hits"], self.inputs["judgments"]
        cases = [
            ({"hits": hits.unionByName(hits.limit(1))}, "unique keys"),
            ({"collection": self.inputs["collection"].unionByName(self.inputs["collection"].limit(1))}, "unique keys"),
            ({"hits": hits.withColumn("rank", F.when((F.col("query_id") == "q1") & (F.col("rank") == 3), 4).otherwise(F.col("rank")))}, "consecutive"),
            ({"judgments": judgments.where("NOT (query_id = 'q1' AND entity_id = 3000)")}, "explicit relevance"),
            ({"judgments": judgments.where("query_id != 'q4'")}, "at least one relevant"),
            ({"hits": hits.withColumn("query_id", F.when(F.col("query_id") == "q1", "unknown").otherwise(F.col("query_id")))}, "known query"),
        ]
        for overrides, message in cases:
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                self.evaluate(**overrides)


if __name__ == "__main__":
    unittest.main()
