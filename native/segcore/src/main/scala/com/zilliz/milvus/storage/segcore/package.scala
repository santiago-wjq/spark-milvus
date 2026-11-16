package com.zilliz.milvus.storage

/** What the connector hands segcore, in the messages segcore's C interface
  * takes, built from core's predicates and read model. `SegcorePlan` states a
  * `PredicateExpr`, a whole tree or one of its top-level conjuncts, as the
  * `planpb.PlanNode` of a retrieve. `SegcoreLoadInfo` states a V3 snapshot
  * segment of the read model, and the indexes to load with it, as the
  * `segcorepb.SegmentLoadInfo` that `NewSegmentWithLoadInfo` takes. Both build
  * the messages generated from the `milvus` submodule's protos, never an
  * encoding of their own. Which predicates, segments and indexes segcore is
  * given, and when, is decided by the caller; this package calls nothing
  * native, and nothing in the published modules calls it.
  */
package object segcore
