package com.zilliz.milvus.storage

/** What the connector hands segcore for one segment, in the encoding segcore's
  * C interface takes. `SegcoreLoadInfo` states a V3 snapshot segment of the
  * read model, and the indexes to load with it, as the
  * `segcorepb.SegmentLoadInfo` that `NewSegmentWithLoadInfo` takes. Messages
  * are written field by field with the numbers of Milvus's
  * `pkg/proto/segcore.proto`, not generated from a copy of it. Which segments,
  * fields and indexes segcore loads, and when, is decided by the caller; this
  * package calls nothing native.
  */
package object segcore
