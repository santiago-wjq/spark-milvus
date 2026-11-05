# native/segcore: Milvus segcore through JNI

Status: under development. Nothing in the published modules loads this code
yet, and it is not part of the unified platform bundle that `native-runtime`
extracts. sbt compiles the Java bindings as `native-segcore`, an unpublished
module that only `core`'s tests depend on.

## What this is for

Scalar filtering, retrieval and the rest of Milvus's per-segment execution
live in segcore (`milvus/internal/core`), behind the C interface Milvus's own
Go code calls (`internal/core/src/**/*_c.h`). The connector uses that
execution layer to decide which rows of a segment its reads and searches
exclude. It brings what a JVM evaluator would otherwise have to rebuild and
then track across Milvus releases: the scalar index families and their
per-segment selection, chunk skipping, columnar and reordered evaluation, and
the JSON, array and text expressions, with the semantics of an online Milvus
query wherever Milvus's semantics are the ones that apply.

## Decisions

- **The Milvus build is the parent build.** segcore, Knowhere and
  milvus-storage are Milvus's own dependencies: Milvus compiles the two engines
  from source with `FetchContent` and `add_subdirectory` against one Conan
  graph, `internal/core/conanfile.py`. `build/build.py` runs that build
  unchanged instead of re-assembling the graph here. Versions and package
  options therefore follow the pinned Milvus; there is nothing to reconcile.
- **The Conan profile carries settings only.** Profile `[options]` override a
  recipe's `default_options`, so `build/profiles/*` names the compiler and
  standard library and leaves every package option to the Milvus conanfile.
- **Knowhere and storage JNI are meant to come from the same build, but do
  not yet.** `-DWITH_JNI=ON` is the upstream option of both engines, and
  `scripts/core_build.sh` passes `CMAKE_EXTRA_ARGS` through. milvus-storage's
  JNI target then fails after linking: its post-build step copies to
  `${CMAKE_SOURCE_DIR}/../java/`, which names a directory outside its tree
  when Milvus is the top-level project; `CMAKE_CURRENT_SOURCE_DIR` is the
  intended base. With that one change, supplied through
  `FETCHCONTENT_SOURCE_DIR_MILVUS-STORAGE` on top of the storage commit Milvus
  pins, the Milvus build with `-DWITH_JNI=ON` completes and yields both
  `libknowhere_jni` and `libmilvus-storage-jni`. milvus-storage main has the
  change, but no Milvus commit pins it yet, and Milvus cannot simply move:
  storage main also passes `Properties` to its read APIs as
  `shared_ptr<const Properties>`, which Milvus master does not compile
  against. This build therefore keeps the storage commit Milvus pins rather
  than patching it, `build.py` passes `-DWITH_JNI=OFF`, and the connector keeps
  the Knowhere and storage JNI from `native/build` until the `milvus` gitlink
  can move to a Milvus commit whose storage pin has the change.
- **The JNI is maintained here, over the C interface only.** Milvus does not
  ship a JNI. `src/main/cpp/segcore_jni.cpp` maps one Java method to one C
  call and never includes segcore's C++ headers. When the `milvus` gitlink
  moves, the diff of the installed `*_c.h` headers is the complete list of
  interface changes to review.
- **The JNI library is segcore's logging host, and its lines go to stderr.**
  segcore sends glog lines through a sink that calls `goZapLogExt`. On Linux
  `libmilvus_core` only declares it (the weak stub exists for macOS, an empty
  one under `WITHOUT_GO_LOGGING`); Milvus's Go binary exports the definition
  and writes to zap. The JNI library defines it instead, and the dynamic
  linker resolves `libmilvus_core`'s reference to it because both load in one
  `dlopen` scope. The sink is process-wide, so Knowhere's and
  milvus-storage's glog lines take the same path. Each line goes to stderr in
  one write, laid out as Spark's default console pattern with milliseconds,
  under the name of the library it comes from (`segcore`, `knowhere` or
  `storage`); the libraries' own prefixes name the thread. `setLogLevel` sets
  the level through `SetLogLevel`, as QueryNode applies its `log.level`.

  The lines do not go through the JVM to log4j, because a native thread
  cannot enter the JVM safely while it exits. Once the JVM has begun to exit,
  HotSpot parks any thread that enters it for good. Under `System.exit`, the
  way an executor exits, the native static destructors then run, and
  segcore's thread pools join their workers: a worker the JVM had attached
  for a log line never finishes detaching, and the JVM never exits (the
  probe, which ends in `System.exit`, hung there when the lines went to
  log4j). glog also holds its log mutex around every sink call, so one thread
  caught in a log call would block every later line. Writing directly also
  keeps the lines just before a native crash. In Spark's default
  configuration log4j's console appender writes to stderr too, so native
  lines reach the same executor log; appenders, files and layouts configured
  otherwise do not receive them, and log4j levels do not apply to them. glog
  lines before `SegcoreInit` installs the sink (Knowhere's version banner)
  and text segcore prints without glog keep their own format. Building with
  `WITHOUT_GO_LOGGING` would discard the logs.
- **Process initialization is explicit.** `SegcoreNative.init` runs
  `SegcoreInit`, `InitCpuNum`, the local chunk manager rooted at a directory
  the caller owns and the mmap manager under it. Segments need the mmap
  manager even with every mmap kind off, which follows Milvus's queryNode
  defaults. Sealed segments need two more process-wide settings first: the
  cache layer they load through (`configureCache`, `ConfigureTieredStorage`),
  without which a load fails to create its overhead group, and the file
  system their manifests are read through (`InitArrowFileSystem`):
  `initLocalFileSystem` for local storage, `initRemoteFileSystem` for an object
  store, configured as QueryNode configures it from its `minio` section, the
  endpoint given as `address:port`. Indexes need a third, the remote chunk
  manager over the same object store (`initRemoteChunkManager`,
  `InitRemoteChunkManagerSingleton`): segcore's index file managers are valid
  only with one, and it reads the index files written before the packed V3
  format, while packed V3 files come through the file system.
- **Sealed segments load the way QueryNode loads them.**
  `NewSegmentWithLoadInfo` takes a `segcorepb.SegmentLoadInfo` with the
  segment's V3 manifest path, storage version 3 and row count, and
  `SegmentLoad` reads the column groups the manifest lists. Four properties
  of that path shape the connector's use of it:
  - The collection schema must include the system fields `RowID` (0) and
    `Timestamp` (1), as RootCoord appends them to every collection: a column
    group carries them, and segcore resolves every column through the schema.
    Snapshot schemas list only the user's fields, so the connector adds them.
  - Only the fields the predicates read are named through
    `UpdateLoadFields`. segcore then loads the others lazily, on the first
    request that touches them, and a filter never touches the vector. Vector
    fields and indexes are also never warmed up, so a segment's vector column
    group is listed but not read.
  - Keys carry their whole storage prefix. For a remote store the file
    system is rooted at the bucket and keys start with its root path; for
    local storage segcore roots it at `/`, so local keys are absolute paths
    and `root_path` is not consulted.
  - Indexes come in the load info's `index_infos`, one `FieldIndexInfo` per
    index with its files and versions, `current_scalar_index_version` among
    them, and the params QueryCoord passes on. QueryCoord appends the load's
    priority to those params as `load_priority` (`HIGH` or `LOW`), and
    segcore reads that key for every index: without it the load fails with a
    JSON type error.

  A load takes a cancellation source. A cancelled load stops with segcore's
  cancellation error (code 2038 at the pinned Milvus), and the segment can be
  loaded again.
- **segcore's memory is the cache layer's.** Field data and indexes a sealed
  segment loads are held by the process-wide cache layer and accounted against
  its watermarks, not against the planner's per-task budgets. Eviction stays
  off, as in QueryNode's defaults. How the planner's native room and these
  watermarks divide an executor's memory is not decided yet.
- **Plans are built in the JVM; the Go plan parser is not used.** segcore
  takes a serialized `planpb.PlanNode`, not an expression string. The
  connector's input is Spark's predicate tree, so it builds the PlanNode
  directly instead of printing an expression and parsing it back. This keeps
  a Go runtime out of the executor JVM, and with it the plan parser's
  configuration loading: its first `paramtable.Get()` builds a BaseTable that
  by default creates an etcd client for `localhost:2379` and refreshes from it
  every five seconds. The obligations the parser would otherwise meet move
  here: type checking against the schema, constant conversion, and sorted,
  distinct `IN` value lists, which segcore relies on regardless of the
  rewriter's optimization switch. `SegcorePlan` in `core` meets them,
  describing each field as the parser does, and builds the messages from
  `core/src/main/protobuf/plan.proto`, an unchanged copy of the file at the
  gitlink: `build.py` fails while the two differ, so `plan.proto` changes show
  when the gitlink moves, as `*_c.h` changes do. A predicate translates when
  segcore can state it, and stays with the JVM evaluator otherwise:
  floating-point fields, null literals in comparisons and `IN` lists,
  null-safe equality with a value, ordering on booleans and out-of-range
  integers do not translate. Nor does `milvus.filter` use the Go parser: its
  expression is parsed by the connector's own front end, which today accepts a
  bounded subset of Plan.g4 and rejects the rest, so wider Milvus syntax means
  a wider front end in the JVM, not the Go parser on the driver or anywhere
  else.
- **segcore decides which rows a segment excludes; the connector keeps the
  vector search.** For each segment, segcore evaluates the predicates assigned
  to it and returns the matching row offsets, which become the exclusion
  bitmap the reads and searches already consume. Knowhere is still called by
  the connector, and segment loading for search, the resource planner, the
  merge and the take are unchanged. The planner's budgets are per task and its
  constants were measured on the connector's own Knowhere path: index copies
  held after a load, the 32 MiB exact-scan block, queries per core in one
  call, the answer buffers in JVM direct memory that Knowhere writes into
  without a copy. segcore's search keeps segment data in a process-wide cache
  with its own watermarks and eviction and returns answers in C++ vectors that
  are copied out, so moving the search there would replace those measurements
  without a gain in the distance computation, which is the same Knowhere.
- **Spark's semantics stay with the JVM evaluator.** A predicate Spark pushes
  is correct when it matches Spark, not Milvus; NaN ordering is one place they
  differ. The connector assigns each accepted predicate to an evaluator:
  segcore for (predicate, type) pairs that differential tests against the JVM
  evaluator and Spark have admitted, the JVM evaluator for the rest.
  Predicates are top-level conjuncts, so their exclusions combine by union and
  a predicate is never split between evaluators. `milvus.filter` is defined by
  Milvus, so segcore is its reference once its front end can express a form.
  `SegcoreDifferentialTest` in `core` is that differential test: it loads the
  `api` and `nearest` fixtures' sealed segments into segcore, evaluates their
  literal input rows with `PredicateEvaluator`, and requires the same rows for
  every predicate it states. So far those are comparisons, `IN` lists and null
  tests over integer fields, their negations, and AND and OR over pairs that
  involve a null category; all agree, since segcore applies three-valued logic
  as Spark does.
- **Deletes stay with the connector.** A segment loaded from its manifest path
  alone does not apply the delete files the manifest lists, even when read
  past the deletes' timestamp. segcore answers which rows a predicate admits,
  and the connector's own delete exclusions, which already apply those files,
  stay in force.
- **No iterative filtering.** Milvus can search first and test the predicate
  only on candidates (`hints=iterative_filter`), which saves work when a
  request carries a few queries. A Spark search carries thousands to millions,
  and the exclusion bitmap is computed once per segment and shared by every
  query group, while iterative filtering repeats the predicate for every query.
- **One milvus-proto for the repository.** The C++ protobuf sources are
  generated from this repository's `milvus-proto` submodule, using the
  `--cpp_out` list of Milvus's `scripts/generate_proto.sh`. `build.py` fails
  unless that gitlink is the version `milvus/go.mod` requires, so the
  submodule moves together with the `milvus` gitlink. Milvus's own
  `download_milvus_proto.sh` is not run.
- **The `milvus` submodule is opt-in.** `.gitmodules` sets `update = none`
  and CI initializes submodules by name, so only this build clones Milvus.
- **The `milvus` gitlink keeps Milvus's fix for the AWS SDK at exit.**
  Before it, segcore's chunk managers took a reference to the AWS SDK
  (`Aws::InitAPI`) and never released it, which Milvus's Go process, exiting
  without C++ static destructors, never notices. A JVM runs them under
  `System.exit`, so the SDK's static objects were destroyed while it was
  still initialized, and the thread its client bootstrap starts when
  destroyed could crash the JVM in the SDK's log redirection (SIGSEGV).
  Milvus PR 55131 releases the reference when the process exits, from master
  commit eabafc85ad on; a gitlink without that change brings the crash back.

## Build

Prerequisites are those of the Milvus C++ build (Conan 2.25.1, CMake, Ninja,
GCC 12, Rust through rustup, libaio for DiskANN) plus a JDK named by
`JAVA_HOME`.

```bash
git submodule update --init --checkout milvus
python3 native/segcore/build/build.py --work-dir /path/outside/the/tree
```

The work directory holds the Conan output and the CMake tree; `build.py`
links it as `milvus/cmake_build`, the path the Milvus scripts expect. The
libraries install under `milvus/internal/core/output/lib`. `--only-jni`
rebuilds just `libmilvus-segcore-jni` against an existing output.

The Conan output is reused only while the Milvus conanfile and the profile are
the ones it was installed from, so moving the `milvus` gitlink reinstalls the
graph instead of building new sources against old packages; `conan` on the
path must be Conan 2. Knowhere and milvus-storage build from the commits
Milvus pins, and `build.py` passes that explicitly, which also clears an
engine source an earlier configure cached. It stops while the work
directory's `lib/` holds the storage libraries a build with `-DWITH_JNI=ON`
copies there: Milvus installs that directory whole, so they would replace
the storage library this build links.

## Probe

```bash
native/segcore/build/run_probe.sh
```

`SegcoreProbe` loads the JNI and checks retrieves with plans built in the JVM,
under `-Xcheck:jni`, in two parts. The first inserts rows into a growing
segment and covers comparison, prefix, `IN`, `AND`, `OR` and `NOT` over
integer and string fields. The second loads the sealed segments of the `api`
collection fixture (`core/src/test/resources/read-fixtures/collections/api`)
from their V3 manifests with only `id` and `category`, checks comparison,
`IN`, `AND`, `OR` and `NOT` there, and cancels one load before it completes.
Its plans collect segcore's execution feature bits
(`retrievePlanFeatureBits`), and a filter on `category`, which has no index,
must report the raw-data path.
Every expected answer is computed from the fixture's literal input rows in
`expected.json`, never from a read of the stored bytes. The probe then leaves
through `System.exit`, as an executor's JVM does, and `run_probe.sh` fails a
JVM that has not exited two minutes after it started. `run_probe.sh` copies
the fixture, makes its manifest base paths absolute for local storage and
lists the rows for the probe. The probe exercises the build feasibility gate:
the libraries of one Milvus build load and run together in HotSpot. The
growing segment and insert entries exist only for this probe and go once a
sealed fixture covers string fields.

## Not yet done

- Packaging into the platform bundle and loading through `native-runtime`.
- Tests of remote storage and of scalar indexes. `initRemoteFileSystem` and
  `initRemoteChunkManager` have loaded a segment Milvus wrote to S3 together
  with its BITMAP, INVERTED, STL_SORT and TRIE indexes in packed V3 files,
  checked by hand against the rows written. Every single-field filter on an
  indexed field reported its index's path and family, except suffix matches
  on an INVERTED string field, which segcore declines to the raw data. No
  test here covers an object store, and no fixture carries a scalar index.
- The division of an executor's memory between the planner's native room and
  the cache layer's watermarks, and whether Storage V3 column groups the
  predicates do not read are created lazily
  (`SegcoreSetLazyColumnGroupEnabled`).
- The list of (predicate, type) pairs segcore evaluates, each admitted by
  `SegcoreDifferentialTest`, which does not yet cover string and boolean
  fields, and the offsets-to-bitmap step in the row exclusions.
- The `milvus` gitlink moved to a Milvus commit whose storage pin has the
  JNI copy fix, then Knowhere and storage JNI from this build.
- Knowhere and milvus-storage gitlinks moved to the Milvus pins and passed to
  the Milvus build through `FETCHCONTENT_SOURCE_DIR_*`, so no source is
  fetched twice.
- Replacing `native/build` for Knowhere and storage with this build.
- Size: debug information shipped separately, dependencies the connector does
  not reach removed, and `libmilvus_core` loaded only by reads that use it.
- The probe as a CI job.
