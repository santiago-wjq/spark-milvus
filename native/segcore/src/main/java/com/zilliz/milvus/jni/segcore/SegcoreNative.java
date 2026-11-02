package com.zilliz.milvus.jni.segcore;

/**
 * Static JNI entries, one per segcore or plan parser C call.
 *
 * <p>Load libmilvus-segcore-jni before the first call; it links
 * libmilvus_core and libmilvus-planparser from the same Milvus build.
 * HotSpot must start with its own JRE's libjsig preloaded, because the plan
 * parser starts a Go runtime that installs signal handlers.
 */
public final class SegcoreNative {
    private SegcoreNative() {}

    /** SegcoreInit with default configuration, then InitCpuNum. Call once per JVM. */
    public static native void init(int cpuNum);

    /** NewCollection from a serialized schemapb.CollectionSchema. */
    public static native long newCollection(byte[] schema);

    public static native void deleteCollection(long collection);

    /** Plan parser RegisterSchema; the handle is independent of any collection. */
    public static native long registerSchema(byte[] schema);

    public static native void unregisterSchema(long handle);

    /** Plan parser Parse: a boolean expression to a serialized planpb.PlanNode. */
    public static native byte[] parse(long schemaHandle, String expression);

    /** CreateRetrievePlanByExpr from a serialized planpb.PlanNode. */
    public static native long createRetrievePlan(long collection, byte[] planNode);

    public static native void deleteRetrievePlan(long plan);

    /** NewSegment of type Growing. */
    public static native long newGrowingSegment(long collection, long segmentId);

    public static native void deleteSegment(long segment);

    /** PreInsert then Insert of a serialized segcorepb.InsertRecord. */
    public static native void insert(long segment, long[] rowIds, long[] timestamps, byte[] insertRecord);

    public static native long rowCount(long segment);

    /** AsyncRetrieve, awaited; returns a serialized segcorepb.RetrieveResults. */
    public static native byte[] retrieve(long segment, long plan, long timestamp, long limitSize);
}
