package com.zilliz.milvus.jni.segcore;

/** A failed segcore call, carrying the segcore error code. */
public final class SegcoreException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final int code;

    public SegcoreException(int code, String message) {
        super(message);
        this.code = code;
    }

    /** The segcore error code, or -1 for an argument rejected by the JNI layer. */
    public int code() {
        return code;
    }
}
