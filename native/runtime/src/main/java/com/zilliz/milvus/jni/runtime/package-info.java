/**
 * Verifies and extracts the shared native bundle against per-component binding contracts
 * before a JNI loader opens its entry library. This package does not load native code.
 * Only an absent bundle yields empty; an unsupported platform or an incomplete
 * bundle fails.
 */
package com.zilliz.milvus.jni.runtime;
