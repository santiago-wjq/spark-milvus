# native/segcore: Milvus segcore through JNI

Status: under development. Nothing in the published modules loads this code
yet; it is not part of the unified platform bundle that `native-runtime`
extracts, and the sbt build does not compile it.

## What this is for

Scalar filtering, retrieval and the rest of Milvus's per-segment execution
live in segcore (`milvus/internal/core`), behind the C interface Milvus's own
Go code calls (`internal/core/src/**/*_c.h`). Using that execution layer gives
the connector the same expression semantics, index path selection and
operator pipeline as an online Milvus query, instead of a second
implementation in the JVM.

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
- **Knowhere and storage JNI come from the same build.** `-DWITH_JNI=ON` is
  the upstream option of both engines; Milvus passes it through when given in
  `CMAKE_EXTRA_ARGS`, which `scripts/core_build.sh` accepts.
- **The JNI is maintained here, over the C interface only.** Milvus does not
  ship a JNI. `src/main/cpp/segcore_jni.cpp` maps one Java method to one C
  call and never includes segcore's C++ headers. When the `milvus` gitlink
  moves, the diff of the installed `*_c.h` headers is the complete list of
  interface changes to review.
- **Expressions compile through the plan parser.** `libmilvus-planparser` is
  Milvus's Go expression compiler built as a C shared library, so filters get
  the same rewrites as online queries. Its Go runtime shares the JVM process;
  HotSpot must preload its own `libjsig`.
- **The `milvus` submodule is opt-in.** `.gitmodules` sets `update = none`
  and CI initializes submodules by name, so only this build clones Milvus.

## Build

Prerequisites are those of the Milvus C++ build (Conan 2.25.1, CMake, Ninja,
GCC 12, Rust through rustup, Go for the plan parser, libaio for DiskANN) plus
a JDK named by `JAVA_HOME`.

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

`SegcoreProbe` loads the JNI, registers a schema with the plan parser, inserts
rows into a growing segment and checks filtered retrieves, under
`-Xcheck:jni`. It exercises the two feasibility gates of the integration:
the libraries of one Milvus build load together in HotSpot, and the plan
parser's Go runtime runs inside it.

## Not yet done

- sbt module, packaging into the platform bundle and loading through
  `native-runtime`.
- Sealed segments loaded from V3 manifests, deletes, and search.
- Replacing `native/build` for Knowhere and storage with this build.
