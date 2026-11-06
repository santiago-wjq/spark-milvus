# Offline backfill

The optional `backfill` module matches existing snapshot rows with input data,
computes replacement field values, and writes segment files and a result
manifest. It supports Spark 4.0 and 4.2 with Scala 2.13. The reader remains
available on all supported Spark profiles without a dependency on backfill.

The caller supplies an existing, stable Storage V3 snapshot, its storage
configuration and input rows. Replacement files are written under each source
segment's existing base path. There is no independent column-file output-path
override. The caller is responsible
for retaining the snapshot and its files for the whole job. The module does
not create or protect snapshots, connect to Milvus, submit updates, wait for
online visibility, or clean up a managed job.

## DataFrame API

```scala
import com.zilliz.spark.connector.backfill.{BackfillConfig, MilvusBackfill}

val config = BackfillConfig(
  storageProperties = storageProperties, // native fs.* properties for the snapshot and output
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
Iceberg input can select an explicit snapshot ID. Iceberg input is available on
Spark 4.0 only: Iceberg publishes no Spark 4.2 runtime, so the 4.2 build refuses
Iceberg input before reading anything. Input storage and target
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
backfill must not compact or renumber its rows. Before writing, the computation
checks segment coverage, row counts and contiguous physical offsets against the
snapshot's Avro records. L0 delete-only entries produce no replacement files.

A successful `BackfillResult` contains collection/schema identity, per-segment
row counts, field mappings, output file locations and merge statistics.
`BackfillStorage.writeResultJson` persists this receipt. The output is stored
data plus metadata, not a Milvus update acknowledgement.

Path calls can provide `onResult: BackfillResult => BackfillResult` to persist
the complete result on the driver before owned input resources close. The
prepared API requires this callback and `onRecovery: BackfillRecovery => Unit`.
When collected segment outcomes include failures, `onRecovery` receives the
known completed segment records before cleanup; the operation still returns
`Left`. Its separate `backfill-recovery-v1` JSON envelope has `complete=false`
and a failed job result. It is recovery evidence and must never be submitted
as a complete backfill. If a worker or driver is lost before returning its
commit record, storage may require reconciliation beyond these known records.

`BackfillError.stage` distinguishes computation, segment writing, complete
result publication, recovery publication and cleanup (`compute`, `write`,
`publish-result`, `publish-recovery`, `cleanup`). `cleanupComplete` is unset
before cleanup is reached and records whether all owned resources closed
successfully afterward. A failed cleanup remains a failed operation even when
a complete result was persisted. Callers must retain that result and reconcile
the failure before retrying or submitting it.

The default receiver capability writes V3 manifest versions and records the
source and committed versions. Explicit manifest-delta capability instead
returns manifest operations and file descriptors; the receiver commits those
operations. Capability selection must match the receiving service. A successful
offline job alone does not make the new values visible to Milvus queries.

## Command-line entry point

`com.zilliz.spark.connector.backfill.BackfillApp` accepts existing snapshot and
input paths, storage settings, merge mode and output options; its
[entry point](../backfill/src/main/scala/com/zilliz/spark/connector/backfill/BackfillApp.scala)
lists the accepted arguments. Supply the backfill artifact, its reader
dependencies, and one matching Storage native bundle to Spark on both driver
and executors. Never put credentials in a committed options file.

Every CLI run requires `--output-result <path>`. A complete result is written
there before input cleanup, using the target storage identity. A partial write
instead writes `<path>.recovery.json`, preserves its completed segment records,
and exits unsuccessfully; it never writes a complete result. Use a fresh result
path for each run. The receipt location is independent of the segment file
locations and may be a local path or a path in the configured object store.

See [releasing](releasing.md) for the artifact graph and
[contributing](contributing.md) for unit and real-storage tests.
