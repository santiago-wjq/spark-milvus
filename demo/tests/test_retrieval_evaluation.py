"""Check the evaluation CLI keeps canned and real benchmark inputs separate."""

import contextlib
import importlib.util
import io
from pathlib import Path
import unittest


SPEC = importlib.util.spec_from_file_location(
    "retrieval_evaluation", Path(__file__).parents[1] / "python/04_retrieval_evaluation.py")
demo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(demo)


class RetrievalArgumentsTest(unittest.TestCase):
    def test_example_defaults_and_explicit_real_inputs(self):
        sample = demo.parse_args(["--output", "/tmp/evaluation", "--verify-example"])
        self.assertTrue(sample.fixture)
        self.assertEqual(sample.k, 3)
        self.assertTrue(Path(sample.queries).is_file())
        custom = ["--options", "read.json", "--queries", "queries", "--hits", "hits",
                  "--judgments", "judgments", "--catalog", "catalog", "--output", "results"]
        args = demo.parse_args(custom)
        self.assertFalse(args.fixture)
        self.assertEqual(args.hits, "hits")

    def test_incomplete_inputs_and_invalid_sample_verification_are_rejected(self):
        invalid = [[], ["--output", " "], ["--output", "out", "--k", "0"],
                   ["--output", "out", "--options", "read.json"],
                   ["--output", "out", "--hits", "hits.jsonl"],
                   ["--output", "out", "--verify-example", "--k", "2"],
                   ["--options", "read.json", "--queries", "queries", "--hits", "hits",
                    "--judgments", "judgments", "--catalog", "catalog", "--output", "out",
                    "--verify-example"]]
        for arguments in invalid:
            with self.subTest(arguments=arguments), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    demo.parse_args(arguments)


if __name__ == "__main__":
    unittest.main()
