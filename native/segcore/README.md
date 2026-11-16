# native/segcore: Milvus segcore through JNI

Status: under development, unpublished and test-only. Nothing in the published
modules loads this code or depends on it, and it is not part of the platform
bundle that `native-runtime` extracts. sbt builds this directory as `segcore`,
a module outside the production layers that depends on `core`; the root
aggregates it only when the `milvus` submodule is initialized.

## What this is for

Scalar filtering, retrieval and the rest of Milvus's per-segment execution
live in segcore (`milvus/internal/core`), behind the C interface Milvus's own
Go code calls (`internal/core/src/**/*_c.h`). The connector uses that
execution layer to decide which rows of a segment a filter admits. It brings
what a JVM evaluator would otherwise have to rebuild and then track across
Milvus releases: the scalar index families and their per-segment selection,
chunk skipping, columnar and reordered evaluation, and the JSON, array and
text expressions, with the semantics of an online Milvus query wherever
Milvus's semantics are the ones that apply.

## Decisions

- **The pin is a Milvus 3.0 release.** The `milvus` gitlink is the release tag
  v3.0.6 (63100f8f5b): connector 3.0 reads snapshots Milvus 3.0 writes, so
  segcore comes from the same release line. 3.0 is not master: its C
  interface lacks calls master has, among them `SetRetrievePlanMode` and
  `GetRetrievePlanFeatureBits`, and its `CStorageConfig` has no Talon field.
- **segcore reports no execution path.** v3.0.6 gives no signal, per plan or
  per filter, of whether a filter ran on a scalar index, the primary-key index
  or raw data: `GetRetrievePlanFeatureBits`, `GetSearchPlanFeatureBits` and the
  feature recorder behind them exist only on master. Even there the bits are
  one set per plan, ORed over every filter it ran, so they cannot attribute a
  path to an individual conjunct. Tests therefore compare results alone, with
  and without indexes.
- **The Milvus build is the parent build.** segcore, Knowhere and
  milvus-storage are Milvus's own dependencies: Milvus compiles the two engines
  from source with `FetchContent` and `add_subdirectory` against one Conan
  graph, `internal/core/conanfile.py`. `build/build.py` runs that build
  unchanged instead of re-assembling the graph here. Versions and package
  options therefore follow the pinned Milvus; there is nothing to reconcile.
- **The Conan profile carries settings only.** Profile `[options]` override a
  recipe's `default_options`, so `build/profiles/*` names the compiler and
  standard library and leaves every package option to the Milvus conanfile.
  Packages built from source get the compiler flags
  `scripts/3rdparty_build.sh` exports for its own `conan install`, which
  `build.py` reads from that script; the Rust toolchain is the one
  `scripts/install_deps.sh` names (1.92 at v3.0.6), and `build.py` stops
  while rustup does not have it.
- **The engines' JNI stays off.** `build.py` passes `-DWITH_JNI=OFF`: the
  connector loads the engines' JNI through its own bundle. The storage
  commit v3.0.6 pins, e5321520, has fixed the step that used to fail a build
  with `-DWITH_JNI=ON` under Milvus: its JNI target now copies to
  `${CMAKE_CURRENT_SOURCE_DIR}/../java/`, inside its own tree, where
  `${CMAKE_SOURCE_DIR}` named a directory outside it. It still copies the
  storage libraries into the top-level build's `lib/`, which Milvus installs
  whole; see [Build](#build).
- **The JNI is maintained here, over the C interface only.** Milvus does not
  ship a JNI. `src/main/cpp/segcore_jni.cpp` maps one Java method to one C
  call and never includes segcore's C++ headers. When the `milvus` gitlink
  moves, the diff of the installed `*_c.h` headers and of the protos below is
  the complete list of interface changes to review.
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
  for a log line never finishes detaching, and the JVM never exits. glog also
  holds its log mutex around every sink call, so one thread caught in a log
  call would block every later line. Writing directly also keeps the lines
  just before a native crash. In Spark's default configuration log4j's
  console appender writes to stderr too, so native lines reach the same
  executor log; appenders, files and layouts configured otherwise do not
  receive them, and log4j levels do not apply to them. glog lines before
  `SegcoreInit` installs the sink (Knowhere's version banner) and text
  segcore prints without glog keep their own format. Building with
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
    index with its files and versions and the params QueryCoord passes on.
    segcore turns `current_scalar_index_version` into the scalar index's
    engine version, so the read model carries that field from the snapshot.
    QueryCoord appends the load's priority to the params as `load_priority`
    (`HIGH` or `LOW`), and segcore reads that key for every index: without it
    the load fails with a JSON type error.

  `SegcoreLoadInfo` writes this load info from a segment of the read model and
  the indexes the caller loads with it, each with `load_priority` `HIGH`, the
  priority QueryCoord gives a load a user asks for. A load takes a
  cancellation source. A cancelled load stops with segcore's cancellation
  error (code 2038 at the pin), and the segment can be loaded again.
- **segcore's memory is the cache layer's.** Field data and indexes a sealed
  segment loads are held by the process-wide cache layer and accounted against
  its watermarks. Eviction stays off, as in QueryNode's defaults. How an
  executor's memory divides between the connector's own budgets and these
  watermarks is not decided yet.
- **Plans are built in the JVM; the Go plan parser is not used.** segcore
  takes a serialized `planpb.PlanNode`, not an expression string. The
  connector's input is a predicate tree, so it builds the PlanNode directly
  instead of printing an expression and parsing it back. This keeps a Go
  runtime out of the executor JVM, and with it the plan parser's
  configuration loading: its first `paramtable.Get()` builds a BaseTable that
  by default creates an etcd client for `localhost:2379` and refreshes from it
  every five seconds. The obligations the parser would otherwise meet move
  here: type checking against the schema, constant conversion, and sorted,
  distinct `IN` value lists, which segcore relies on regardless of the
  rewriter's optimization switch. `SegcorePlan` meets them, describing each
  field as the parser does. Floating-point fields, null literals in
  comparisons and `IN` lists, null-safe equality with a value, ordering on
  booleans and out-of-range integers do not translate. A plan carries no row
  limit, and a retrieve must come back without `has_more_result`. It names no
  output field either: a retrieve always returns the segment offsets of the
  rows it admits in `RetrieveResults.offset`, which index the rows as an
  exclusion bitmap does, and fills `ids` only when the primary key is an
  output field, which it would read for every admitted row. The size check a
  retrieve runs against its limit is an average field size times the rows,
  summed over the output fields, so with none it computes 0: the limit bounds
  no memory, and the offsets take 8 bytes a row.
- **The messages are generated from the submodules, never copied or written
  by hand.** The `segcore` module generates `plan.proto`, `segcore.proto` and
  the Milvus files `segcore.proto` imports, `index_coord.proto` and through it
  `internal.proto`, from `milvus/pkg/proto` with ScalaPB, and the
  `milvus-proto` files those import besides `schema.proto` and `common.proto`
  (`milvus.proto`, `rg.proto`, `feder.proto`, `msg.proto`); the
  `schema.proto` and `common.proto` classes are core's. No service stubs are
  generated. A gitlink move therefore changes the messages in the same
  commit, and their diff is reviewed with the `*_c.h` diff.
- **Evaluation is split by where a predicate comes from.** A predicate Spark
  pushes (R6) is correct when it matches Spark, not Milvus; NaN ordering is one
  place they differ. Its top-level conjuncts are the units: Spark keeps only
  TRUE rows, so the rows several conjuncts admit are the intersection of the
  rows each admits, and an evaluator only has to name the TRUE rows. The JVM
  evaluator prunes and scans raw data for every conjunct. segcore answers only
  a conjunct that an index on that segment fully serves, and only for (index
  family, operator, type) combinations that differential tests against the
  JVM evaluator and Spark have admitted; whether an index serves it is decided
  per segment, since small and new segments have none. A combination is
  admitted only when segcore's index path and its raw-data path both match
  Spark, so a conjunct segcore evaluates on raw data after all costs a second
  read of its column, not a wrong row; the pin reports no filter path, and
  nothing checks the path at run time. NOT and NULL handling, index
  selection and resolving a HYBRID index to its family are segcore's own
  execution layer. That `SegcorePlan` can state a conjunct is therefore
  only a necessary condition for handing it to segcore. `milvus.filter` (R7) is
  defined by Milvus, so it goes to segcore whole, with Milvus semantics, once
  the connector's own front end, which accepts a bounded subset of Plan.g4
  and rejects the rest, states it as a plan. `SegcorePlan.conjuncts` splits a
  pushed filter, and `SegcorePlan.retrieve` states one conjunct or a whole
  tree.
- **Two phases.** In the first, segcore's segment-level C interface serves
  both R7 and the R6 conjuncts an index answers: a collection, a sealed
  segment and a plan per segment, with segcore's own execution layer. The
  admitted conjuncts of a segment go into one plan, and what comes back is
  the rows all of them admit; a conjunct's own rows are retrieved separately
  only in tests and diagnostics. In the second phase, once Milvus exposes a
  direct interface to its scalar indexes, that interface takes over the R6
  conjuncts one index reader call answers, a single operator on a single
  field, admitted by the same differential tests; conjuncts that combine
  fields or operators stay with segcore, so the connector never combines
  index results under three-valued logic, and R7 keeps needing segcore's
  complete expression engine.
  The R6 raw-data scan stays with the JVM evaluator in both. None of this is
  wired into a read yet: the scan path, the per-segment choice and the
  admission table are still to come.
- **Deletes stay with the connector.** A segment loaded from its manifest path
  alone does not apply the delete files the manifest lists, even when read
  past the deletes' timestamp; `SegcoreDifferentialTest` checks this on the
  `basic` fixture at the pin. segcore answers which rows a predicate admits,
  and the connector's own delete exclusions, which already apply those files,
  stay in force.
- **One milvus-proto for the repository.** segcore's C++ protobuf sources are
  generated from this repository's `milvus-proto` submodule, using the
  `--cpp_out` list of Milvus's `scripts/generate_proto.sh`, and the JVM
  messages come from the same submodule. `build.py` fails unless that gitlink
  is the version `milvus/go.mod` requires: a pseudo-version names the commit,
  and a release names the tag under the module's directory (`v3.0.6` is
  `go-api/v3.0.6`). The submodule therefore moves together with the `milvus`
  gitlink, and every module of the connector builds against it. Milvus's own
  `download_milvus_proto.sh` is not run.
- **The `milvus` submodule is opt-in.** `.gitmodules` sets `update = none` and
  CI initializes submodules by name, so only this build clones Milvus, and
  the root aggregates `segcore` only where it is checked out. No release
  script, cache key or published artifact reads it.
- **A process holds one native closure.** `libmilvus_core` comes with
  libraries of its own build that share sixteen SONAMEs with the public
  bundle but not their bytes, among them `libmilvus-storage.so` (e5321520
  against the bundle's 5694520), folly, glog, gflags, the AWS SDK and CRT,
  s2n and OpenSSL. The dynamic linker keeps one object per SONAME in a
  process, whichever closure loaded it first, and segcore's libraries find
  each other only through `LD_LIBRARY_PATH`, which also precedes the bundle's
  `$ORIGIN` run paths. Under it the storage JNI fails to load, in either
  order, on an AWS SDK symbol its Arrow needs and segcore's SDK does not
  export; without it segcore does not load. The bundle's alias, private-symbol
  and relocation checks cover one bundle, not a second closure beside it. So
  segcore's suites run in the `segcore` module's own test JVM, which the
  build keeps the bundle off and which reads fixtures without the storage JNI,
  and `SegcoreNativeTest` checks that every library the segcore build ships is
  mapped from its directory and none from an extracted bundle. Before segcore
  can share an executor with the bundle, both have to come from one build.
- **The `milvus` gitlink keeps Milvus's fix for the AWS SDK at exit.**
  segcore's chunk managers took a reference to the AWS SDK (`Aws::InitAPI`)
  and never released it, which Milvus's Go process, exiting without C++
  static destructors, never notices. A JVM runs them under `System.exit`, so
  the SDK's static objects were destroyed while it was still initialized,
  and the thread its client bootstrap starts when destroyed could crash the
  JVM in the SDK's log redirection (SIGSEGV). Milvus PR 55131 releases the
  reference when the process exits; the 3.0 line has it from commit
  72ac82aa6b on, so v3.0.6 does. A gitlink without that change brings the
  crash back.

## Build

Prerequisites are those of the Milvus C++ build (Conan 2.25.1, CMake, Ninja,
GCC 12, the Rust toolchain Milvus names, installed through rustup, libaio for
DiskANN) plus a JDK named by `JAVA_HOME`.

```bash
git submodule update --init --checkout milvus
python3 native/segcore/build/build.py --work-dir /path/outside/the/tree --jobs 64
```

The work directory holds the Conan output and the CMake tree; `build.py`
links it as `milvus/cmake_build`, the path the Milvus scripts expect. The
libraries install under `milvus/internal/core/output/lib`. `--jobs` sets
`core_build.sh`'s Ninja jobs (all processors by default); `--only-jni`
rebuilds just `libmilvus-segcore-jni` against an existing output.

The Conan output is reused only while the Milvus conanfile and the profile are
the ones it was installed from, so moving the `milvus` gitlink reinstalls the
graph instead of building new sources against old packages; `conan` on the
path must be Conan 2. Knowhere and milvus-storage build from the commits
Milvus pins, and `build.py` passes that explicitly, which also clears an
engine source an earlier configure cached. It stops while the work
directory's `lib/` holds the storage libraries a build with `-DWITH_JNI=ON`
copies there: Milvus installs that directory whole, so they would replace
the storage library this build links. The build writes only files Milvus
ignores into the submodule (`internal/core/output`, the generated
`src/pb/*.pb.*`, Conan's `CMakeUserPresets.json`) and the `cmake_build`
link, which it excludes in the submodule's `info/exclude`. The libraries it
installs find one another through `LD_LIBRARY_PATH`, as Milvus's
`scripts/setenv.sh` arranges: the Conan libraries Milvus copies beside
`libmilvus_core` carry no run path to each other.

What `build.py` reads from the Milvus scripts has tests of its own:

```bash
python3 -m unittest discover -s native/segcore/build -p 'test_*.py'
```

## Tests

```bash
sbt segcore/test
```

`SegcorePlanTest` and `SegcoreLoadInfoTest` need no native library.
`SegcoreDifferentialTest` loads the sealed segments of the `api` and `basic`
collection fixtures (`core/src/test/resources/read-fixtures/collections`)
from their V3 manifests with the load info `SegcoreLoadInfo` writes from
`SnapshotCatalog`'s model, evaluates their literal input rows in
`expected.json` with `PredicateEvaluator`, and requires the same rows for
every predicate it states: comparisons, `IN` lists and null tests over
integer fields, their negations, and AND and OR over pairs that involve the
null category, 688 in all. segcore names the rows it admits by segment
offset, from plans without output fields, and the suite checks that those
offsets are the positions of the literal rows by asking segcore once for the
primary key as well. The comparison is on results alone. A segment loads with
the indexes its snapshot lists for the predicate fields, and neither public
fixture lists one, so every comparison is without an index.
`SegcoreNativeTest` covers what no fixture has: string
predicates on a growing segment filled by inserts, an unknown field, a
cancelled load, and where the loaded libraries come from. The native suites
find the libraries through `milvus.segcore.lib`, which the build points at
`milvus/internal/core/output/lib`, and cancel without them. Their JVM checks
every JNI call (`-Xcheck:jni`), binds every symbol at load and leaves through
`System.exit`, as an executor's does.

## Not yet done

- Packaging into the platform bundle and loading through `native-runtime`,
  which needs segcore and the bundle built as one native closure (above),
  staged with relative run paths instead of `LD_LIBRARY_PATH` and the
  absolute build paths in `libmilvus_core`'s own.
- A fixture with scalar indexes, and with it differential tests per (index
  family, operator, type), string and boolean fields among them, that make
  up the R6 admission table, each combination run with and without its
  index. Reporting which path a conjunct took needs
  `GetRetrievePlanFeatureBits`, which the 3.0 line does not have, and even
  there its bits cover a whole plan rather than one conjunct.
- Tests of remote storage: `initRemoteFileSystem` and
  `initRemoteChunkManager` are not exercised against an object store at the
  pin.
- The scan path: per-segment selection of R6 conjuncts, the row offsets
  turned into the exclusion bitmap, and R7 plans from the `milvus.filter`
  front end, whose results must match those of the plans Milvus's own parser
  builds for the same expressions before R7 moves to segcore.
- The division of an executor's memory with the cache layer's watermarks, and
  whether Storage V3 column groups the predicates do not read are created
  lazily (`SegcoreSetLazyColumnGroupEnabled`).
- One build for segcore and the bundle: segcore as a component in
  `native/build`'s dependency graph, or the bundle's storage taken from this
  build; then the engines' JNI from that one build.
- Size: debug information shipped separately, dependencies the connector does
  not reach removed, and `libmilvus_core` loaded only by reads that use it.
- A CI job for the native suites.
