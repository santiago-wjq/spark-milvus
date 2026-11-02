package com.zilliz.milvus.jni.segcore;

/** A failed segcore or plan parser call, carrying the native error code. */
public final class SegcoreException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final int code;

    public SegcoreException(int code, String message) {
        super(message);
        this.code = code;
    }

    /** The segcore error code, or -1 for a plan parser failure. */
    public int code() {
        return code;
    }
}
