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
#include <pthread.h>

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <vector>

#include "common/init_c.h"
#include "futures/future_c.h"
#include "segcore/arrow_fs_c.h"
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

std::string
Utf(JNIEnv* env, jstring string) {
    const char* chars = env->GetStringUTFChars(string, nullptr);
    std::string out(chars);
    env->ReleaseStringUTFChars(string, chars);
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

// Logging host state, set by JNI_OnLoad.
constexpr jint kJniVersion = JNI_VERSION_1_8;
constexpr const char* kLogClass = "com/zilliz/milvus/jni/segcore/SegcoreLog";
constexpr int kFatal = 3;  // glog FATAL

std::atomic<JavaVM*> g_vm{nullptr};
jclass g_log_class = nullptr;
jmethodID g_log_method = nullptr;
std::atomic<int> g_log_threshold{0};
pthread_key_t g_detach_key;

void
WriteStderr(int severity, const char* file, int file_len, int line, const char* msg, int msg_len) {
    static const char kLevels[] = "IWEF";
    char level = severity >= 0 && severity < 4 ? kLevels[severity] : '?';
    std::fprintf(stderr, "segcore %c %.*s:%d] %.*s\n", level, file_len, file, line, msg_len, msg);
}

// Runs when a thread this library attached exits.
void
DetachOnExit(void*) {
    if (JavaVM* vm = g_vm.load()) {
        vm->DetachCurrentThread();
    }
}

// The calling thread's JNIEnv. segcore logs from its own pool threads, which
// are attached as daemons on their first line, under their native names, and
// detached when they exit. Returns null when there is no usable VM.
JNIEnv*
LogEnv() {
    JavaVM* vm = g_vm.load();
    if (vm == nullptr) {
        return nullptr;
    }
    JNIEnv* env = nullptr;
    jint rc = vm->GetEnv(reinterpret_cast<void**>(&env), kJniVersion);
    if (rc == JNI_OK) {
        return env;
    }
    if (rc != JNI_EDETACHED) {
        return nullptr;
    }
    char name[16] = "segcore-native";
    pthread_getname_np(pthread_self(), name, sizeof(name));
    JavaVMAttachArgs args{kJniVersion, name, nullptr};
    if (vm->AttachCurrentThreadAsDaemon(reinterpret_cast<void**>(&env), &args) != JNI_OK) {
        return nullptr;
    }
    pthread_setspecific(g_detach_key, env);
    return env;
}

// An object store as QueryNode configures it from its minio section, owning
// the strings its CStorageConfig points at. Every string field is set: the
// chunk manager copies each into a std::string. With useIam the keys are the
// environment's, and the access key and secret may be empty.
struct RemoteStorage {
    std::string address, bucket, root, type, provider, region, key, secret;
    bool use_ssl, use_iam, use_virtual_host;
    int64_t request_timeout_ms;

    CStorageConfig
    Config() const {
        CStorageConfig config{};
        config.address = address.c_str();
        config.bucket_name = bucket.c_str();
        config.access_key_id = key.c_str();
        config.access_key_value = secret.c_str();
        config.root_path = root.c_str();
        config.storage_type = type.c_str();
        config.cloud_provider = provider.c_str();
        config.iam_endpoint = "";
        config.log_level = "warn";
        config.region = region.c_str();
        config.useSSL = use_ssl;
        config.sslCACert = "";
        config.useIAM = use_iam;
        config.useVirtualHost = use_virtual_host;
        config.requestTimeoutMs = request_timeout_ms;
        config.gcp_credential_json = "";
        config.use_custom_part_upload = true;
        config.tls_min_version = "";
        config.talon_coordinator = "";
        return config;
    }
};

RemoteStorage
ReadRemoteStorage(JNIEnv* env, jstring address, jstring bucketName, jstring rootPath, jstring storageType,
                  jstring cloudProvider, jstring region, jstring accessKeyId, jstring secretAccessKey,
                  jboolean useSsl, jboolean useIam, jboolean useVirtualHost, jlong requestTimeoutMs) {
    return RemoteStorage{Utf(env, address),       Utf(env, bucketName),      Utf(env, rootPath),
                         Utf(env, storageType),   Utf(env, cloudProvider),   Utf(env, region),
                         Utf(env, accessKeyId),   Utf(env, secretAccessKey), useSsl == JNI_TRUE,
                         useIam == JNI_TRUE,      useVirtualHost == JNI_TRUE, requestTimeoutMs};
}

}  // namespace

extern "C" {

JNIEXPORT jint JNICALL
JNI_OnLoad(JavaVM* vm, void*) {
    JNIEnv* env = nullptr;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), kJniVersion) != JNI_OK) {
        return JNI_ERR;
    }
    // Resolved here, on the thread that loads the library: FindClass on a
    // native thread attached later searches the system class loader, which
    // does not see the connector's classes under Spark.
    jclass cls = env->FindClass(kLogClass);
    if (cls == nullptr) {
        return JNI_ERR;
    }
    jmethodID threshold = env->GetStaticMethodID(cls, "threshold", "()I");
    jmethodID log = env->GetStaticMethodID(cls, "log", "(I[BI[B)V");
    if (threshold == nullptr || log == nullptr) {
        return JNI_ERR;
    }
    jint min_severity = env->CallStaticIntMethod(cls, threshold);
    if (env->ExceptionCheck()) {
        return JNI_ERR;
    }
    if (pthread_key_create(&g_detach_key, DetachOnExit) != 0) {
        return JNI_ERR;
    }
    g_log_class = static_cast<jclass>(env->NewGlobalRef(cls));
    g_log_method = log;
    g_log_threshold.store(min_severity);
    g_vm.store(vm);
    return kJniVersion;
}

// segcore routes glog through a sink that calls goZapLogExt, which on Linux
// the host process must define: Milvus's Go binary exports it and writes the
// lines to zap. Here the JNI library is the host, and the dynamic linker
// resolves libmilvus_core's reference to this definition, since both load in
// one dlopen scope. The sink is process-wide, so Knowhere's and
// milvus-storage's glog lines arrive here too. They go to SegcoreLog and from
// there to the executor's log4j configuration.
__attribute__((visibility("default"))) void
goZapLogExt(int severity, const char* file, int file_len, int line, const char* msg, int msg_len) {
    if (severity < g_log_threshold.load(std::memory_order_relaxed)) {
        return;
    }
    // glog aborts after a FATAL line, before an asynchronous appender may
    // have written it; stderr keeps it.
    if (severity >= kFatal) {
        WriteStderr(severity, file, file_len, line, msg, msg_len);
    }
    JNIEnv* env = LogEnv();
    // No VM (before load or while it exits), or a Java thread with an
    // exception pending, which must not call into Java.
    if (env == nullptr || env->ExceptionCheck()) {
        if (severity < kFatal) {
            WriteStderr(severity, file, file_len, line, msg, msg_len);
        }
        return;
    }
    // An attached native thread has no Java frame to release local
    // references, so the frame is pushed and popped here.
    if (env->PushLocalFrame(2) != JNI_OK) {
        env->ExceptionClear();
        return;
    }
    jbyteArray jfile = ToArray(env, file, file_len);
    jbyteArray jmsg = ToArray(env, msg, msg_len);
    if (jfile != nullptr && jmsg != nullptr) {
        env->CallStaticVoidMethod(g_log_class, g_log_method, static_cast<jint>(severity), jfile,
                                  static_cast<jint>(line), jmsg);
    }
    if (env->ExceptionCheck()) {
        // A failing appender must not surface as an exception in an
        // unrelated Java call on this thread; the line still goes out.
        env->ExceptionClear();
        WriteStderr(severity, file, file_len, line, msg, msg_len);
    }
    env->PopLocalFrame(nullptr);
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
    std::string root = Utf(env, localDir);
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

// The cache layer sealed segments load through, which accounts their memory
// and disk against these watermarks. QueryNode's defaults apply with two
// exceptions. Vector indexes are not warmed up, as vector fields already are
// not: the connector searches with its own Knowhere calls, and segcore only
// evaluates predicates. And one prefetch thread serves, since nothing warms up
// asynchronously; scalar fields and indexes warm up during the load.
JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_configureCache(JNIEnv* env, jclass, jlong memoryLowBytes,
                                                                 jlong memoryHighBytes, jlong memoryMaxBytes,
                                                                 jlong diskLowBytes, jlong diskHighBytes,
                                                                 jlong diskMaxBytes, jstring diskPath) {
    std::string path = Utf(env, diskPath);
    ConfigureTieredStorage(CacheWarmupPolicy_Sync, CacheWarmupPolicy_Disable, CacheWarmupPolicy_Sync,
                           CacheWarmupPolicy_Disable, memoryLowBytes, memoryHighBytes, memoryMaxBytes, diskLowBytes,
                           diskHighBytes, diskMaxBytes,
                           /*storage_usage_tracking_enabled=*/false,
                           /*eviction_enabled=*/false,
                           /*cache_touch_window_ms=*/3000,
                           /*background_eviction_enabled=*/false,
                           /*eviction_interval_ms=*/1000,
                           /*cache_cell_unaccessed_survival_time=*/0,
                           /*overloaded_memory_threshold_percentage=*/0.9f,
                           /*loading_resource_factor=*/1.0f,
                           /*max_disk_usage_percentage=*/0.95f, path.c_str(),
                           /*loading_timeout_ms=*/-1,
                           /*warmup_loading_timeout_ms=*/0,
                           /*reject_remote_vector_output=*/false,
                           /*prefetch_pool_threads=*/1);
}

// The file system segcore reads V3 manifests and column groups through, here
// local storage. Milvus keys carry their whole prefix, which for local storage
// makes them absolute paths, so segcore roots the local file system at "/"
// whatever root_path says, and manifest base paths must be absolute. The
// fields left unset are the remote store's; QueryNode leaves them unset for
// local storage too.
JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_initLocalFileSystem(JNIEnv* env, jclass) {
    CStorageConfig config{};
    config.storage_type = "local";
    config.talon_coordinator = "";
    CheckStatus(env, InitArrowFileSystem(config), "InitArrowFileSystem");
}

// The same file system over an object store. Keys carry their whole prefix,
// so the file system is rooted at the bucket and manifest base paths start
// with the root path.
JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_initRemoteFileSystem(
    JNIEnv* env, jclass, jstring address, jstring bucketName, jstring rootPath, jstring storageType,
    jstring cloudProvider, jstring region, jstring accessKeyId, jstring secretAccessKey, jboolean useSsl,
    jboolean useIam, jboolean useVirtualHost, jlong requestTimeoutMs) {
    RemoteStorage storage = ReadRemoteStorage(env, address, bucketName, rootPath, storageType, cloudProvider, region,
                                              accessKeyId, secretAccessKey, useSsl, useIam, useVirtualHost,
                                              requestTimeoutMs);
    CheckStatus(env, InitArrowFileSystem(storage.Config()), "InitArrowFileSystem");
}

// The remote chunk manager over the same object store. segcore loads no index
// without it: an index's file manager context is valid only with a chunk
// manager, and index files written before the packed V3 format are read
// through it. Packed V3 index files are read through the file system, as
// column groups are.
JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_initRemoteChunkManager(
    JNIEnv* env, jclass, jstring address, jstring bucketName, jstring rootPath, jstring storageType,
    jstring cloudProvider, jstring region, jstring accessKeyId, jstring secretAccessKey, jboolean useSsl,
    jboolean useIam, jboolean useVirtualHost, jlong requestTimeoutMs) {
    RemoteStorage storage = ReadRemoteStorage(env, address, bucketName, rootPath, storageType, cloudProvider, region,
                                              accessKeyId, secretAccessKey, useSsl, useIam, useVirtualHost,
                                              requestTimeoutMs);
    CheckStatus(env, InitRemoteChunkManagerSingleton(storage.Config()), "InitRemoteChunkManagerSingleton");
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

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_setLoadFields(JNIEnv* env, jclass, jlong collection,
                                                                jlongArray fieldIds) {
    jsize n = env->GetArrayLength(fieldIds);
    std::vector<int64_t> ids(static_cast<size_t>(n));
    env->GetLongArrayRegion(fieldIds, 0, n, reinterpret_cast<jlong*>(ids.data()));
    CheckStatus(env, UpdateLoadFields(reinterpret_cast<CCollection>(collection), ids.data(), n), "UpdateLoadFields");
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

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_newSealedSegment(JNIEnv* env, jclass, jlong collection,
                                                                   jlong segmentId, jboolean sortedByPk,
                                                                   jbyteArray loadInfo) {
    auto blob = Bytes(env, loadInfo);
    CSegmentInterface segment = nullptr;
    if (!CheckStatus(env,
                     NewSegmentWithLoadInfo(reinterpret_cast<CCollection>(collection), Sealed, segmentId, &segment,
                                            sortedByPk == JNI_TRUE, blob.data(), static_cast<int64_t>(blob.size())),
                     "NewSegmentWithLoadInfo")) {
        return 0;
    }
    return reinterpret_cast<jlong>(segment);
}

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_newLoadCancellation(JNIEnv*, jclass) {
    return reinterpret_cast<jlong>(NewLoadCancellationSource());
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_cancelLoad(JNIEnv*, jclass, jlong cancellation) {
    CancelLoadCancellationSource(reinterpret_cast<CLoadCancellationSource>(cancellation));
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_releaseLoadCancellation(JNIEnv*, jclass, jlong cancellation) {
    ReleaseLoadCancellationSource(reinterpret_cast<CLoadCancellationSource>(cancellation));
}

// SegmentLoad on the calling thread; cancellation 0 loads without a source.
JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_load(JNIEnv* env, jclass, jlong segment, jlong cancellation) {
    CTraceContext trace{nullptr, nullptr, 0};
    CheckStatus(env,
                SegmentLoad(trace, reinterpret_cast<CSegmentInterface>(segment),
                            reinterpret_cast<CLoadCancellationSource>(cancellation)),
                "SegmentLoad");
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
