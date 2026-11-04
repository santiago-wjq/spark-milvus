package com.zilliz.milvus.storage

/** Storage V3 column-file writing, manifest transactions and Arrow resource
  * ownership. These storage primitives contain no Spark or service control;
  * callers own job completion and registration.
  */
package object write
