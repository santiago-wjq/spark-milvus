# Working on the Milvus Spark Connector

Orientation for anyone opening this repository, human or agent. `CLAUDE.md` is
a symlink to this file, so every tool reads the same page; edit this one.

This page and [docs/contributing.md](docs/contributing.md) are what you need
to build, test and change the code.

## What this project is

Spark reads fixed Milvus Storage V3 snapshots directly from object storage.
The optional backfill module joins caller-supplied input with an existing snapshot
and writes replacement column files and result JSON. The caller owns snapshot
preparation, protection, service submission and online visibility. Public code has
no Milvus service client, vector search engine or index construction API.

The 1.x line stopped at tag `v1.6.0` and is no longer maintained. Current work
replaces the earlier read/search distribution. Development versions use the 2.1
line; candidate selection is explicit and must never reuse published coordinates.

## The public layers

Five production modules, with dependencies only pointing downward.

| Layer | Modules | What lives there |
|---|---|---|
| 1 | `native-runtime`, `native-storage` | Component-aware platform loading and upstream Storage APIs. |
| 2 | `core` | V3 formats, read computation, storage access and shared V3 writing primitives; no Spark. |
| 3 | `spark` | Read DataSource V2, neutral physical scan contracts and Spark/Arrow conversion. |
| 3 | `backfill` | Optional offline backfill, input bindings, result JSON and API/CLI; depends on spark/core. |

Layer-1 modules live in `native/runtime` and `native/storage`; `native/build`
produces the Storage-only platform bundle. The root aggregates production modules
and publishes no artifact. `integration` is outside the aggregate. Backfill
requires Spark 4.0 / Scala 2.13; reader sources support Spark 3.5 (Scala 2.12 or
2.13), 4.0, 4.1 and 4.2. `-DsparkVersion` and `-DscalaVersion` select a profile
at sbt startup. Restart sbt and clean module outputs when switching profiles.
Formatting and package checks inspect every retained shim root.

Initialize the `milvus-proto` and `milvus-storage` submodules before building.
Only common/schema protobufs are generated; no service protocol is generated.
Use `spark/assembly` for a reader bundle and the optional backfill artifact for
computation. Keep Iceberg out of the reader's dependency closure.

## Rules any change has to satisfy

1. **No Spark below layer 3.** A source file in `core` or `native-storage`
   that mentions `org.apache.spark` fails the build. The check strips comments
   first. Use `com.zilliz.milvus.storage.Logging` instead of Spark's.
2. **Directories mirror packages.** Scala allows a mismatch; this repository
   does not.
3. **Dependencies point downward only.** If a lower layer needs something from a
   higher one, the thing is in the wrong layer. Move it; do not add a back edge.
4. **Every decision is written down** before the code that depends on it is
   written: each `package.scala` states what its package is responsible for,
   and a change that alters a responsibility updates it in the same commit.

Two habits follow from the same idea. **No compatibility shims**: when code
moves, update the call sites, because a forwarding stub hides the move and
outlives the migration. **Fix causes, never patch the edge**: swallowing an
exception, turning a failure into an empty result or adding a flag that routes
around a defect makes a visible problem invisible while the system keeps running
wrong.

Code, comments, build scripts, `README.md` and the documents under `docs/` are
English, and the public documents are Markdown. Use imports and short type or
object names in code; follow the [import rules](docs/contributing.md#imports).

Repository documents must not cite workstation-hosted web documents. Copy any
needed document and its required assets into the appropriate repository first,
then cite that copy with repository-relative links; see
[document references](docs/contributing.md#repository-local-document-references).

## Before every commit

Apply [.agents/skills/spark-milvus-sbt/SKILL.md](.agents/skills/spark-milvus-sbt/SKILL.md).
The [formatting](docs/contributing.md#formatting) and
[unit tests](docs/contributing.md#unit-tests) sections of the contributing guide
hold the required commands: the formatter and its check with Java 21, `git diff
--check`, then the full root unit test suite. A change to a `package.scala`
keeps its responsibility statement current
([Documents](docs/contributing.md#documents)).
