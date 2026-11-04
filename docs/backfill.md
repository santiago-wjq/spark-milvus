# Offline backfill

The optional `backfill` module matches existing snapshot rows with input data,
computes replacement field values, and writes segment files and a result
manifest. It supports Spark 4.0 / Scala 2.13. The reader remains available on
all supported Spark profiles without a dependency on backfill.

The caller supplies an existing, stable Storage V3 snapshot, its storage
configuration, input rows, and the output location. The caller is responsible
for retaining the snapshot and its files for the whole job. The module does
not create or protect snapshots, connect to Milvus, submit updates, wait for
online visibility, or clean up a managed job.

## DataFrame API

```scala
import com.zilliz.spark.connector.backfill.{BackfillConfig, MilvusBackfill}

val config = BackfillConfig(
  storageProperties = storageProperties, // native fs.* properties for the snapshot and output
  customOutputPath = Some("backfill/run-001"),
  mode = BackfillConfig.ModeCoalesce
)
val result = MilvusBackfill.run(input, "snapshots/existing.json", config)
  .fold(error => throw new IllegalStateException(error.message, error.cause.orNull), identity)
println(result.toJson)
```

`input` is a Spark DataFrame whose columns use the collection's field names.
The default join key is the collection primary key. A configured physical
join field must be a non-nullable scalar with stable equality semantics.
Duplicate or invalid input identities fail validation before output is written.
The caller owns the input DataFrame and its cache.

The path API accepts Parquet or Iceberg input. With the default primary-key
join and no column mapping, path input retains the literal `pk` column
convention. `columnMapping` maps input columns to collection field names.
Iceberg input can select an explicit snapshot ID. Input storage and target
storage may use different credentials; the
[storage reference](reference.md#22-storage-location-and-credentials) describes the
connector's native storage configuration.

## Merge modes and output

| Mode | Matching row | Snapshot row without an input match |
|---|---|---|
| `coalesce` | Keep a non-null snapshot value; otherwise use input | Keep snapshot value |
| `overwrite` | Use input, including null | Keep snapshot value |
| `replace` | Use input, including null | Write null |

The computation preserves physical segment row order and row count, including
rows hidden by delete records. Delete state remains owned by the snapshot;
backfill must not compact or renumber its rows.

A successful `BackfillResult` contains collection/schema identity, per-segment
row counts, field mappings, output file locations and merge statistics.
`BackfillStorage.writeResultJson` persists this receipt. The output is stored
data plus metadata, not a Milvus update acknowledgement.

The default receiver capability writes V3 manifest versions and records the
source and committed versions. Explicit manifest-delta capability instead
returns manifest operations and file descriptors; the receiver commits those
operations. Capability selection must match the receiving service. A successful
offline job alone does not make the new values visible to Milvus queries.

## Command-line entry point

`com.zilliz.spark.connector.backfill.BackfillApp` accepts existing snapshot and
input paths, storage settings, merge mode and output options. Run it with
`--help` for the complete option list. Supply the backfill artifact, its reader
dependencies, and one matching Storage native bundle to Spark on both driver
and executors. Never put credentials in a committed options file.

See [releasing](releasing.md) for the artifact graph and
[contributing](contributing.md) for unit and real-storage tests.
