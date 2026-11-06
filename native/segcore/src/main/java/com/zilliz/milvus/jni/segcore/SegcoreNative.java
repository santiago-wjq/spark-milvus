package com.zilliz.milvus.jni.segcore;

/**
 * Static JNI entries, one per segcore C call.
 *
 * <p>Load libmilvus-segcore-jni before the first call; it links libmilvus_core
 * from the same Milvus build as the Knowhere and storage JNI. HotSpot must start
 * with its own JRE's libjsig preloaded, as Knowhere requires. Once {@link #init}
 * has run, the glog lines of segcore, Knowhere and milvus-storage go to stderr,
 * at the level {@link #setLogLevel} sets.
 */
public final class SegcoreNative {
    private SegcoreNative() {}

    /**
     * SetLogLevel, as QueryNode applies its {@code log.level}: the lowest
     * severity of the native libraries' glog lines, one of {@code trace},
     * {@code debug}, {@code info}, {@code warn} and {@code error}, where
     * {@code debug} and {@code trace} add glog's verbose lines. Takes effect
     * from the next line. Throws IllegalArgumentException for any other name.
     */
    public static native void setLogLevel(String level);

    /**
     * SegcoreInit with default configuration, InitCpuNum, then the local chunk
     * manager rooted at {@code localDir} and the mmap manager under
     * {@code localDir/mmap}, limited to {@code mmapDiskLimitBytes}. Every mmap
     * kind starts disabled; segments still need the manager. Call once per JVM.
     */
    public static native void init(int cpuNum, String localDir, long mmapDiskLimitBytes);

    /**
     * ConfigureTieredStorage: the cache layer sealed segments load through,
     * with its memory and disk watermarks in bytes and its disk cache under
     * {@code diskPath}. Scalar fields and indexes load with the segment;
     * vector fields and indexes are never warmed up. Eviction is off, as in
     * QueryNode's defaults. Call once per JVM, before the first sealed load.
     */
    public static native void configureCache(long memoryLowBytes, long memoryHighBytes, long memoryMaxBytes,
        long diskLowBytes, long diskHighBytes, long diskMaxBytes, String diskPath);

    /**
     * InitArrowFileSystem for local storage: sealed segments read their V3
     * manifests and column groups through it. Milvus keys for local storage
     * are absolute paths, so segcore roots this file system at {@code /} and
     * manifest base paths must be absolute.
     */
    public static native void initLocalFileSystem();

    /**
     * InitArrowFileSystem over an object store, as QueryNode configures it
     * from its minio section: {@code address} is the endpoint,
     * {@code storageType} {@code remote} or {@code minio}. The file system is
     * rooted at the bucket, so manifest base paths start with the root path.
     * With {@code useIam} the credentials come from the environment and the
     * keys may be empty.
     */
    public static native void initRemoteFileSystem(String address, String bucketName, String rootPath,
        String storageType, String cloudProvider, String region, String accessKeyId, String secretAccessKey,
        boolean useSsl, boolean useIam, boolean useVirtualHost, long requestTimeoutMs);

    /**
     * InitRemoteChunkManagerSingleton over the same object store, with the same
     * arguments as {@link #initRemoteFileSystem}. segcore loads no index
     * without it: index files written before the packed V3 format are read
     * through it, and packed V3 index files through the file system, which
     * still needs a chunk manager beside it.
     */
    public static native void initRemoteChunkManager(String address, String bucketName, String rootPath,
        String storageType, String cloudProvider, String region, String accessKeyId, String secretAccessKey,
        boolean useSsl, boolean useIam, boolean useVirtualHost, long requestTimeoutMs);

    /** NewCollection from a serialized schemapb.CollectionSchema. */
    public static native long newCollection(byte[] schema);

    public static native void deleteCollection(long collection);

    /**
     * UpdateLoadFields: segments of this collection load only these fields;
     * an empty list loads every field.
     */
    public static native void setLoadFields(long collection, long[] fieldIds);

    /** CreateRetrievePlanByExpr from a serialized planpb.PlanNode. */
    public static native long createRetrievePlan(long collection, byte[] planNode);

    public static native void deleteRetrievePlan(long plan);

    /**
     * GetRetrievePlanFeatureBits: the execution features the plan used in
     * every retrieve it served, as milvus::FeatureBit flags
     * (common/FeatureBits.h): each filter's path, such as a scalar index or
     * raw data, and the index families that served it. 0 unless the
     * PlanNode's plan_options set collect_feature_bits.
     */
    public static native long retrievePlanFeatureBits(long plan);

    /** NewSegment of type Growing. */
    public static native long newGrowingSegment(long collection, long segmentId);

    /**
     * NewSegmentWithLoadInfo of type Sealed from a serialized
     * segcorepb.SegmentLoadInfo. Nothing is read until {@link #load}.
     */
    public static native long newSealedSegment(long collection, long segmentId, boolean sortedByPk, byte[] loadInfo);

    /** NewLoadCancellationSource; release it with {@link #releaseLoadCancellation}. */
    public static native long newLoadCancellation();

    /** CancelLoadCancellationSource: loads given this source stop with an error. */
    public static native void cancelLoad(long cancellation);

    public static native void releaseLoadCancellation(long cancellation);

    /**
     * SegmentLoad on the calling thread: the load info's fields that the
     * collection's load fields admit. {@code cancellation} is a source from
     * {@link #newLoadCancellation} or 0 for none.
     */
    public static native void load(long segment, long cancellation);

    public static native void deleteSegment(long segment);

    /** PreInsert then Insert of a serialized segcorepb.InsertRecord. */
    public static native void insert(long segment, long[] rowIds, long[] timestamps, byte[] insertRecord);

    public static native long rowCount(long segment);

    /** AsyncRetrieve, awaited; returns a serialized segcorepb.RetrieveResults. */
    public static native byte[] retrieve(long segment, long plan, long timestamp, long limitSize);
}
