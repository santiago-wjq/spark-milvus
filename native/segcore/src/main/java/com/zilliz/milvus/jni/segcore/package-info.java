/**
 * JNI over the Milvus segcore C interface and the plan parser's C exports.
 *
 * <p>SegcoreNative maps one static method to one C call: collections, retrieve
 * plans and segments cross as opaque handles, and schemas, plans, insert
 * records and results cross as serialized protobuf. The caller owns every
 * handle and releases it with the matching delete method. Asynchronous segcore
 * calls block the calling thread until their future resolves.
 *
 * <p>Status: development probe on the fork. The library is produced by the
 * Milvus build itself (native/segcore/build) and is not yet part of the unified
 * platform bundle that native-runtime loads.
 */
package com.zilliz.milvus.jni.segcore;
