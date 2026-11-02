// Copyright 2026 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// JNI over the segcore C interface (internal/core/src/**/*_c.h). Every entry
// point maps to one C call; protobuf messages cross as byte arrays and handles
// cross as jlong. Nothing here reaches into segcore's C++ classes. Plans arrive
// as serialized planpb.PlanNode built by the caller; the Go plan parser is not
// linked.

#include <jni.h>

#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <vector>

#include "common/init_c.h"
#include "futures/future_c.h"
#include "segcore/collection_c.h"
#include "segcore/plan_c.h"
#include "segcore/segcore_init_c.h"
#include "segcore/segment_c.h"
#include "storage/storage_c.h"

namespace {

constexpr const char* kErrorClass = "com/zilliz/milvus/jni/segcore/SegcoreException";

void
ThrowSegcore(JNIEnv* env, int code, const std::string& msg) {
    jclass cls = env->FindClass(kErrorClass);
    if (cls == nullptr) {
        return;  // NoClassDefFoundError is already pending
    }
    jmethodID ctor = env->GetMethodID(cls, "<init>", "(ILjava/lang/String;)V");
    jstring jmsg = env->NewStringUTF(msg.c_str());
    auto ex = static_cast<jthrowable>(env->NewObject(cls, ctor, code, jmsg));
    env->Throw(ex);
}

// Consumes the status: segcore allocates error_msg with malloc.
bool
CheckStatus(JNIEnv* env, CStatus status, const char* what) {
    if (status.error_code == 0) {
        return true;
    }
    std::string msg = std::string(what) + ": " + (status.error_msg ? status.error_msg : "");
    if (status.error_msg != nullptr) {
        free(const_cast<char*>(status.error_msg));
    }
    ThrowSegcore(env, status.error_code, msg);
    return false;
}

// The C interface signals readiness through an opaque mutex and an unlock
// callback; a condition variable stands in for the Go mutex Milvus uses.
struct Waiter {
    std::mutex mu;
    std::condition_variable cv;
    bool ready = false;
};

void
UnlockWaiter(CLockedGoMutex* opaque) {
    auto* w = reinterpret_cast<Waiter*>(opaque);
    {
        std::lock_guard<std::mutex> lock(w->mu);
        w->ready = true;
    }
    w->cv.notify_all();
}

// Blocks until the future resolves, then takes ownership of its result.
bool
AwaitFuture(JNIEnv* env, CFuture* future, void** result, const char* what) {
    Waiter waiter;
    future_register_ready_callback(future, UnlockWaiter, reinterpret_cast<CLockedGoMutex*>(&waiter));
    {
        std::unique_lock<std::mutex> lock(waiter.mu);
        waiter.cv.wait(lock, [&] { return waiter.ready; });
    }
    CStatus status = future_leak_and_get(future, result);
    future_destroy(future);
    return CheckStatus(env, status, what);
}

std::vector<uint8_t>
Bytes(JNIEnv* env, jbyteArray array) {
    jsize n = env->GetArrayLength(array);
    std::vector<uint8_t> out(static_cast<size_t>(n));
    if (n > 0) {
        env->GetByteArrayRegion(array, 0, n, reinterpret_cast<jbyte*>(out.data()));
    }
    return out;
}

jbyteArray
ToArray(JNIEnv* env, const void* data, int64_t size) {
    jbyteArray out = env->NewByteArray(static_cast<jsize>(size));
    if (out != nullptr && size > 0) {
        env->SetByteArrayRegion(out, 0, static_cast<jsize>(size), static_cast<const jbyte*>(data));
    }
    return out;
}

}  // namespace

extern "C" {

// segcore routes glog through a sink that calls goZapLogExt, which on Linux
// the host process must define: Milvus's Go binary exports it. Here the JNI
// library is the host, and the dynamic linker resolves libmilvus_core's
// reference to this definition, since both load in one dlopen scope. Lines
// go to stderr, where executor logs are collected.
__attribute__((visibility("default"))) void
goZapLogExt(int severity, const char* file, int file_len, int line, const char* msg, int msg_len) {
    static const char kLevels[] = "IWEF";
    char level = severity >= 0 && severity < 4 ? kLevels[severity] : '?';
    std::fprintf(stderr, "segcore %c %.*s:%d] %.*s\n", level, file_len, file, line, msg_len, msg);
}

// The local chunk manager roots at localDir; the mmap manager, which segments
// require even with mmap off, uses localDir/mmap. Mmap settings follow
// Milvus's queryNode defaults: read-ahead "willneed", 64 MiB allocation
// blocks, every mmap kind off.
JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_init(JNIEnv* env, jclass, jint cpuNum, jstring localDir,
                                                       jlong mmapDiskLimitBytes) {
    SegcoreInit(nullptr);
    InitCpuNum(cpuNum);
    const char* chars = env->GetStringUTFChars(localDir, nullptr);
    std::string root(chars);
    env->ReleaseStringUTFChars(localDir, chars);
    if (!CheckStatus(env, InitLocalChunkManagerSingleton(root.c_str()), "InitLocalChunkManagerSingleton")) {
        return;
    }
    std::string mmap = root + "/mmap";
    CMmapConfig config{};
    config.cache_read_ahead_policy = "willneed";
    config.mmap_path = mmap.c_str();
    config.disk_limit = static_cast<uint64_t>(mmapDiskLimitBytes);
    config.fix_file_size = 64ULL << 20;
    config.json_stats_mmap_path = mmap.c_str();
    CheckStatus(env, InitMmapManager(config), "InitMmapManager");
}

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_newCollection(JNIEnv* env, jclass, jbyteArray schema) {
    auto blob = Bytes(env, schema);
    CCollection collection = nullptr;
    if (!CheckStatus(env, NewCollection(blob.data(), static_cast<int64_t>(blob.size()), &collection),
                     "NewCollection")) {
        return 0;
    }
    return reinterpret_cast<jlong>(collection);
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_deleteCollection(JNIEnv*, jclass, jlong collection) {
    DeleteCollection(reinterpret_cast<CCollection>(collection));
}

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_createRetrievePlan(JNIEnv* env, jclass, jlong collection,
                                                                     jbyteArray planNode) {
    auto blob = Bytes(env, planNode);
    CRetrievePlan plan = nullptr;
    if (!CheckStatus(env,
                     CreateRetrievePlanByExpr(reinterpret_cast<CCollection>(collection), blob.data(),
                                              static_cast<int64_t>(blob.size()), &plan),
                     "CreateRetrievePlanByExpr")) {
        return 0;
    }
    return reinterpret_cast<jlong>(plan);
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_deleteRetrievePlan(JNIEnv*, jclass, jlong plan) {
    DeleteRetrievePlan(reinterpret_cast<CRetrievePlan>(plan));
}

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_newGrowingSegment(JNIEnv* env, jclass, jlong collection,
                                                                    jlong segmentId) {
    CSegmentInterface segment = nullptr;
    if (!CheckStatus(env,
                     NewSegment(reinterpret_cast<CCollection>(collection), Growing, segmentId, &segment, false),
                     "NewSegment")) {
        return 0;
    }
    return reinterpret_cast<jlong>(segment);
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_deleteSegment(JNIEnv*, jclass, jlong segment) {
    DeleteSegment(reinterpret_cast<CSegmentInterface>(segment));
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_insert(JNIEnv* env, jclass, jlong segment, jlongArray rowIds,
                                                         jlongArray timestamps, jbyteArray insertRecord) {
    auto seg = reinterpret_cast<CSegmentInterface>(segment);
    jsize rows = env->GetArrayLength(rowIds);
    if (env->GetArrayLength(timestamps) != rows) {
        ThrowSegcore(env, -1, "insert: rowIds and timestamps differ in length");
        return;
    }
    std::vector<int64_t> ids(rows);
    std::vector<uint64_t> ts(rows);
    env->GetLongArrayRegion(rowIds, 0, rows, reinterpret_cast<jlong*>(ids.data()));
    env->GetLongArrayRegion(timestamps, 0, rows, reinterpret_cast<jlong*>(ts.data()));
    auto record = Bytes(env, insertRecord);
    int64_t offset = 0;
    if (!CheckStatus(env, PreInsert(seg, rows, &offset), "PreInsert")) {
        return;
    }
    CheckStatus(env, Insert(seg, offset, rows, ids.data(), ts.data(), record.data(), record.size()), "Insert");
}

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_rowCount(JNIEnv*, jclass, jlong segment) {
    return GetRowCount(reinterpret_cast<CSegmentInterface>(segment));
}

// Returns a serialized segcorepb.RetrieveResults.
JNIEXPORT jbyteArray JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_retrieve(JNIEnv* env, jclass, jlong segment, jlong plan,
                                                           jlong timestamp, jlong limitSize) {
    CTraceContext trace{nullptr, nullptr, 0};
    CFuture* future = AsyncRetrieve(trace, reinterpret_cast<CSegmentInterface>(segment),
                                    reinterpret_cast<CRetrievePlan>(plan), static_cast<uint64_t>(timestamp),
                                    limitSize, false, 0, 0, 0);
    void* raw = nullptr;
    if (!AwaitFuture(env, future, &raw, "AsyncRetrieve")) {
        return nullptr;
    }
    auto* result = static_cast<CRetrieveResult*>(raw);
    jbyteArray out = ToArray(env, result->proto_blob, result->proto_size);
    DeleteRetrieveResult(result);
    return out;
}

}  // extern "C"
