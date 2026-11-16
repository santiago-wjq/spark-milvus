# Evaluate product search results

Which queries return irrelevant products? Are some returned products missing
from the current Collection, or missing metadata needed to investigate them?
This example uses Spark to answer those questions across a set of queries,
instead of inspecting search responses one at a time.

The workflow combines three sources:

- **Ranked results and relevance judgments**: exported search results and labels
  saying which products are relevant to each query.
- **Collection data**: Spark-Milvus reads product IDs and categories from a fixed
  Milvus Snapshot.
- **Product catalog**: an external dataset supplies names for the report.

Ordinary Spark joins connect these sources, calculate retrieval metrics and
produce an investigation report. Milvus or your search application supplies the
ranked results; this script does not run vector searches or modify the Collection.

## Run the included example

The included inputs are **synthetic, illustrative data**, not exported production
traffic. They reuse the repository's frozen Collection sample, which contains
14 visible rows, and add four example queries, rankings, judgments and a product
catalog under [`data/retrieval-evaluation`](data/retrieval-evaluation). The sample
vectors are not product embeddings; the supplied rankings and labels illustrate
evaluation behavior without claiming semantic search quality.

From the repository root, run on Linux x86_64 with Spark 4.0.1 for Scala 2.13
and Java 21. The command resolves Spark-Milvus from Maven Central:

```bash
export JAVA_HOME=/path/to/jdk-21
export SPARK_HOME=/path/to/spark-4.0.1
export LD_PRELOAD="$JAVA_HOME/lib/libjsig.so"
export LD_BIND_NOW=1

"$SPARK_HOME/bin/spark-submit" \
  --master 'local[2]' \
  --packages com.zilliz:spark-milvus-4.0-linux-x86_64_2.13:3.0.0 \
  --conf 'spark.driver.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED' \
  --conf 'spark.executor.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED' \
  --conf "spark.executorEnv.LD_PRELOAD=$JAVA_HOME/lib/libjsig.so" \
  --conf spark.executorEnv.LD_BIND_NOW=1 \
  --py-files demo/python/common.py \
  demo/python/04_retrieval_evaluation.py \
  --output /tmp/product-search-evaluation \
  --verify-example
```

Choose a fresh output directory for each run. `--verify-example` checks the
included example against its known results and requires the default data with
`--k 3`. It is not a quality threshold for your own search application. The
default `--k` is `3`.

If you already have a local assembly or a verified candidate, use the
[example launcher](README.md#run-a-reader-example) instead:

```bash
./demo/run.sh python 04_retrieval_evaluation.py \
  --output /tmp/product-search-evaluation \
  --verify-example
```

The committed Collection sample is local to the checkout, so this example uses
a local Spark master. For other platforms or cluster runtime settings, see
[application development](../docs/application-development.md#running-your-application)
and [cluster setup](../docs/cluster-setup.md).

## Understand the result

The output contains four Spark JSON directories. Each has `part-*.json` files,
rather than a single JSON document; read a directory with
`spark.read.json("/tmp/product-search-evaluation/per_query")`.

| Directory | What it helps you inspect |
|---|---|
| `per_query` | Each query's P@k, R@k, reciprocal rank@k, hit counts and missing metadata counts, including queries with no hits. |
| `by_group` | Mean query metrics grouped by `query_group`, so a weak group is visible in the report. |
| `hit_details` | Top-k results with ranks, relevance labels, Collection categories and catalog names. |
| `issues` | Irrelevant hits, queries with no hits, and missing Collection or catalog matches. |

Each issue includes `query_id`, `entity_id`, `rank` and an `issue` code:

| Code | What to investigate |
|---|---|
| `non_relevant_hit` | A top-k result explicitly labeled irrelevant. Inspect the query and ranked product in `hit_details`. |
| `no_hits` | A query returned no products. Its entity ID and rank are null. |
| `missing_collection` | A returned ID was not found in the selected Collection state. Check the export and Snapshot times. |
| `missing_catalog` | A returned ID was not found in the external catalog. Check catalog coverage and entity IDs. |

A result can have several issues, so repeated query/entity pairs in this report
are expected. The included example produces seven issue rows: four irrelevant
hits, two missing metadata matches and one query with no hits.

The metrics use binary relevance labels:

- **Precision@k**: relevant hits in the first k positions, divided by k. If a
  query returns fewer than k hits, the unfilled positions contribute zero.
- **Recall@k**: relevant hits in the first k positions, divided by all known
  relevant products for the query. A meaningful recall value requires judgments
  containing every relevant product in the evaluation scope.
- **MRR@k**: the mean reciprocal rank of the first relevant result within k.
  Each query contributes `1 / rank`, or zero if none is relevant. The per-query
  report contains that contribution; the group report averages it.

For the included example at k = 3:

| Query | Ranked product IDs | Relevant product IDs | P@3 | R@3 | Reciprocal rank@3 |
|---|---|---|---|---|---|
| `q1` | 3003, 3004, 3000 | 3003, 3004 | 2/3 | 1 | 1 |
| `q2` | 3000, 3003, 3100 | 3100, 3102 | 1/3 | 1/2 | 1/3 |
| `q3` | 3002, 3101, 3001 | 3002, 3101 | 2/3 | 1 | 1 |
| `q4` | No hits | 3102 | 0 | 0 | 0 |

The `discovery` group (`q1`, `q2`) has mean P@3 = 1/2, R@3 = 3/4 and MRR@3 =
2/3. The `catalog_refresh` group (`q3`, `q4`) has mean P@3 = 1/3, R@3 = 1/2 and
MRR@3 = 1/2. These are averages over queries, including the zero-hit query.

`q2` has only one relevant product in the top three, and that product is ranked
third. Its hit details give you a concrete query and result set to investigate.
`q3` has good labeled relevance but also data issues: product `3002` was deleted
from the sample Collection, and product `3001` is absent from the external
catalog. Its Collection category is also null, which you can inspect in
`hit_details`. `q4` stays in the evaluation with zero-valued metrics, so failures
to return any result are not hidden by averaging only successful queries.

Missing metadata does not remove a hit or change its relevance label. A result
can be relevant according to the judgments while absent from the Collection
state being inspected. For real data, investigate the Collection time and
export time before treating that mismatch as an application defect. Metadata
corrections alone do not demonstrate improved retrieval relevance; rerun the
search and evaluate new results to measure that effect.

## Use your own data

Prepare a fixed Snapshot of the target Collection and a JSON options file with
`milvus.snapshot.path` and its storage settings. See
[resolve an existing Snapshot path](README.md#resolve-an-existing-snapshot-path)
and the [storage reference](../docs/reference.md#22-storage-location-and-credentials).

Export the following four datasets as JSON Lines: one JSON object per line.
Use the same query and entity IDs across files. This example uses string query
IDs and 64-bit integer entity IDs; the selected Collection ID field must also
contain integers.

| Argument | Required fields | Meaning |
|---|---|---|
| `--queries` | `query_id`, `query_text`, `query_group` | Every query to evaluate, including queries that returned no hits. |
| `--hits` | `query_id`, `entity_id`, `rank` | Ranked results exported by Milvus or your search application; ranks start at 1. |
| `--judgments` | `query_id`, `entity_id`, `relevant` | Relevance labels: `1` for relevant, `0` for irrelevant. |
| `--catalog` | `entity_id`, `product_name` | External names used to explain results. |

For example, a hit and its judgment are separate records:

```json
{"query_id":"q1","entity_id":3003,"rank":1}
```

```json
{"query_id":"q1","entity_id":3003,"relevant":1}
```

Provide `--options` and all four dataset arguments together; the demo does not
mix custom inputs with the included sample. Keep the Spark submission settings
from the first command and replace its application arguments with:

```bash
  --options /private/collection-read-options.json \
  --queries /data/evaluation/queries.jsonl \
  --hits /data/evaluation/hits.jsonl \
  --judgments /data/evaluation/judgments.jsonl \
  --catalog /data/evaluation/catalog.jsonl \
  --id-column product_id \
  --category-column category_id \
  --k 3 \
  --output /data/evaluation/results/run-001
```

`--id-column` and `--category-column` map your Collection fields; they default
to `id` and `category`. JSON Lines inputs retain the field names listed above.
For a cluster, use shared input and output locations accessible to Spark and
configure storage access on the driver and executors. The JSON input and report
paths use Spark's Hadoop filesystem configuration. The Collection's `fs.*`
options configure its separate storage reader; they do not configure access to
these JSON paths.

The script validates the inputs before writing reports:

- Query IDs, Collection entity IDs and catalog entity IDs must be unique.
  Each query/entity pair in hits and judgments must be unique; ranks within a
  query must be consecutive from 1, with no ties.
- Hit and judgment records must refer to a declared query.
- Every evaluated query must have at least one positive judgment, and every
  top-k hit must have an explicit `0` or `1` judgment. Missing labels are not
  silently treated as irrelevant.

These checks catch malformed evaluations. They cannot establish whether labels
are correct or exhaustive: your evaluation dataset must define the relevant
products for each query and use a time range consistent with the Collection
state. Missing Collection and catalog matches remain visible in the reports.

## Validate changes to the example

From the repository root, select Java 21 through `JAVA_HOME`. Run the command-line
and launcher checks with Python 3, then the metric tests with a Python environment
that has PySpark 4.0.1 installed:

```bash
python3 -m unittest discover -s demo/tests -p 'test_*.py'
python3 -m unittest discover -s demo/tests/spark -p 'test_*.py'
```

The metric tests start a local Spark JVM and use in-memory Collection metadata.
Run the included example with `--verify-example` as well to exercise an actual
Collection read through Spark-Milvus and write the reports.
