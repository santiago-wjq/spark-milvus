# core protobuf sources

`plan.proto` is Milvus's `pkg/proto/plan.proto` at the commit the `milvus`
gitlink names, unchanged. segcore takes its plans as `planpb.PlanNode`
messages, and `core` builds them from `PredicateExpr` instead of handing an
expression string to the Go plan parser, so the JVM needs the message
definitions. They live in the Milvus repository rather than in `milvus-proto`,
and the `milvus` submodule is opt-in, so the build reads this copy.

The copy moves with the gitlink: `native/segcore/build/build.py` fails while it
differs from the file at the gitlink, so a change to `plan.proto` shows up when
the gitlink moves and is reviewed with the `*_c.h` changes. Copy the file again
then; never edit it here.

`schema.proto` and `common.proto`, which it imports, come from the
`milvus-proto` submodule.
