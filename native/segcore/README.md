# native/segcore: Milvus segcore through JNI

Status: under development. Nothing in the published modules loads this code
yet; it is not part of the unified platform bundle that `native-runtime`
extracts, and the sbt build does not compile it.

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
  intended base. Until that is fixed in milvus-storage, `build.py` passes
  `-DWITH_JNI=OFF` and the connector keeps the Knowhere and storage JNI from
  `native/build`.
- **The JNI is maintained here, over the C interface only.** Milvus does not
  ship a JNI. `src/main/cpp/segcore_jni.cpp` maps one Java method to one C
  call and never includes segcore's C++ headers. When the `milvus` gitlink
  moves, the diff of the installed `*_c.h` headers is the complete list of
  interface changes to review.
- **The JNI library is segcore's logging host, and its lines go to log4j.**
  segcore sends glog lines through a sink that calls `goZapLogExt`. On Linux
  `libmilvus_core` only declares it (the weak stub exists for macOS, an empty
  one under `WITHOUT_GO_LOGGING`); Milvus's Go binary exports the definition
  and writes to zap. The JNI library defines it instead, and the dynamic
  linker resolves `libmilvus_core`'s reference to it because both load in one
  `dlopen` scope. The sink is process-wide, so Knowhere's and
  milvus-storage's glog lines take the same path. `goZapLogExt` calls
  `SegcoreLog`, which writes through slf4j to the executor's log4j
  configuration under `com.zilliz.milvus.jni.segcore.native.segcore`,
  `.knowhere` and `.storage`; executor logs then carry native lines in the
  same files, layout and level control as the connector's own. segcore logs
  from its own pool threads: they are attached to the JVM as daemons on their
  first line and detached when they exit, and the class and method are
  resolved in `JNI_OnLoad`, because `FindClass` on an attached native thread
  sees only the system class loader. Lines below the level in force at load
  time are dropped before crossing into the JVM. FATAL lines are also written
  to stderr, since glog aborts right after them. Two kinds of output stay on
  stderr: glog lines before `SegcoreInit` installs the sink (Knowhere's
  version banner) and text segcore prints without glog. Building with
  `WITHOUT_GO_LOGGING` would discard the logs.
- **Process initialization is explicit.** `SegcoreNative.init` runs
  `SegcoreInit`, `InitCpuNum`, the local chunk manager rooted at a directory
  the caller owns and the mmap manager under it. Segments need the mmap
  manager even with every mmap kind off, which follows Milvus's queryNode
  defaults.
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
  rewriter's optimization switch. `plan.proto` changes show in the gitlink
  diff, as `*_c.h` changes do. The same holds for `milvus.filter`: its
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

## Probe

```bash
native/segcore/build/run_probe.sh
```

`SegcoreProbe` loads the JNI, inserts rows into a growing segment and checks
retrieves for comparison, prefix, `IN`, `AND`, `OR` and `NOT` plans built in
the JVM, under `-Xcheck:jni`. It exercises the build feasibility gate: the
libraries of one Milvus build load and run together in HotSpot. The growing
segment and insert entries exist only for this probe; the connector reads
sealed segments, and they go once sealed loading is in place.

## Not yet done

- sbt module, packaging into the platform bundle and loading through
  `native-runtime`.
- Sealed segments loaded from V3 manifests, remote storage initialization,
  deletes and cancellation.
- `PredicateExpr` to PlanNode translation, the offsets-to-bitmap step in the
  row exclusions, and the list of (predicate, type) pairs segcore evaluates,
  each admitted by differential tests.
- The milvus-storage JNI copy fix, then Knowhere and storage JNI from this
  build.
- Knowhere and milvus-storage gitlinks moved to the Milvus pins and passed to
  the Milvus build through `FETCHCONTENT_SOURCE_DIR_*`, so no source is
  fetched twice.
- Replacing `native/build` for Knowhere and storage with this build.
- Size: debug information shipped separately, dependencies the connector does
  not reach removed, and `libmilvus_core` loaded only by reads that use it.
- The probe as a CI job.
