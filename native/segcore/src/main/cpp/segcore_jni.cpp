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
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <mutex>
#include <string>
#include <string_view>
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

// glog severities as segcore's sink passes them, by log4j's level names.
constexpr const char* kLevels[] = {"INFO", "WARN", "ERROR", "FATAL"};

// The library a line comes from, by the compile-time path of its source: the
// Milvus build fetches Knowhere and milvus-storage into directories named
// after them.
const char*
Origin(std::string_view path) {
    if (path.find("knowhere") != std::string_view::npos) {
        return "knowhere";
    }
    if (path.find("milvus-storage") != std::string_view::npos ||
        path.find("milvus_storage") != std::string_view::npos) {
        return "storage";
    }
    return "segcore";
}

// Writes every byte unless stderr itself fails, which leaves nowhere to
// report it.
void
WriteAll(iovec* iov, int count) {
    while (count > 0) {
        ssize_t written = writev(STDERR_FILENO, iov, count);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return;
        }
        auto left = static_cast<size_t>(written);
        while (count > 0 && left >= iov->iov_len) {
            left -= iov->iov_len;
            ++iov;
            --count;
        }
        if (count > 0) {
            iov->iov_base = static_cast<char*>(iov->iov_base) + left;
            iov->iov_len -= left;
        }
    }
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

// segcore routes glog through a sink that calls goZapLogExt, which on Linux
// the host process must define: Milvus's Go binary exports it and writes the
// lines to zap. Here the JNI library is the host, and the dynamic linker
// resolves libmilvus_core's reference to this definition, since both load in
// one dlopen scope. The sink is process-wide, so Knowhere's and
// milvus-storage's glog lines arrive here too. Each line goes to stderr in one
// write, laid out as Spark's default console pattern with milliseconds, and
// nothing here calls into the JVM: glog holds its log mutex around the sink,
// on whichever thread logs, and a thread that enters a JVM that has begun to
// exit is parked there for good.
__attribute__((visibility("default"))) void
goZapLogExt(int severity, const char* file, int file_len, int line, const char* msg, int msg_len) {
    int saved_errno = errno;
    std::string_view path(file, static_cast<size_t>(file_len));
    std::string_view name = path.substr(path.rfind('/') + 1);
    timespec now{};
    clock_gettime(CLOCK_REALTIME, &now);
    tm local{};
    localtime_r(&now.tv_sec, &local);
    char head[256];
    int head_len = std::snprintf(head, sizeof(head), "%02d/%02d/%02d %02d:%02d:%02d.%03ld %s %s: %.*s:%d] ",
                                 local.tm_year % 100, local.tm_mon + 1, local.tm_mday, local.tm_hour, local.tm_min,
                                 local.tm_sec, now.tv_nsec / 1000000,
                                 severity >= 0 && severity < 4 ? kLevels[severity] : "?", Origin(path),
                                 static_cast<int>(name.size()), name.data(), line);
    head_len = std::clamp(head_len, 0, static_cast<int>(sizeof(head)) - 1);
    char newline = '\n';
    iovec parts[] = {{head, static_cast<size_t>(head_len)},
                     {const_cast<char*>(msg), static_cast<size_t>(msg_len)},
                     {&newline, 1}};
    WriteAll(parts, 3);
    errno = saved_errno;
}

// SetLogLevel, as QueryNode applies its log.level: glog's minimum severity
// for segcore, Knowhere and milvus-storage, and its verbose level. SetLogLevel
// leaves the minimum unchanged for a name it does not know, so only Milvus's
// names pass.
JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_segcore_SegcoreNative_setLogLevel(JNIEnv* env, jclass, jstring level) {
    static const char* const kNames[] = {"trace", "debug", "info", "warn", "error"};
    std::string value = Utf(env, level);
    if (std::none_of(std::begin(kNames), std::end(kNames), [&](const char* name) { return value == name; })) {
        jclass cls = env->FindClass("java/lang/IllegalArgumentException");
        if (cls != nullptr) {
            env->ThrowNew(cls, ("log level " + value + " is not one of trace, debug, info, warn, error").c_str());
        }
        return;
    }
    SetLogLevel(value.c_str());
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
