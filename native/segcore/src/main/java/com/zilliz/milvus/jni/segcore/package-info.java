/**
 * JNI over the Milvus segcore C interface.
 *
 * <p>SegcoreNative maps one static method to one C call: collections, retrieve
 * plans and segments cross as opaque handles, and schemas, plans, insert
 * records and results cross as serialized protobuf. The caller owns every
 * handle and releases it with the matching delete method. Asynchronous segcore
 * calls block the calling thread until their future resolves. Plans are
 * serialized planpb.PlanNode messages the caller builds; Milvus's Go plan
 * parser is deliberately not used, so no Go runtime enters the JVM.
 *
 * <p>Status: unpublished and test-only. The library is produced by the Milvus
 * build itself (native/segcore/build) and is not part of the platform bundle
 * that native-runtime extracts; a process that loads it must not also load
 * that bundle's libraries (native/segcore/README.md).
 */
package com.zilliz.milvus.jni.segcore;
