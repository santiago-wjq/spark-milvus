package com.zilliz.milvus.jni.segcore;

import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receives the native libraries' glog lines and writes them through slf4j, so
 * they reach the executor's log4j configuration like the connector's own logs.
 *
 * <p>The JNI library's goZapLogExt calls {@link #log} for every glog line
 * segcore's sink forwards, on whichever thread logged it; native pool threads
 * are attached to the JVM for this. Lines are split by origin into three
 * loggers, so each library's level can be set on its own:
 * {@code com.zilliz.milvus.jni.segcore.native.segcore}, {@code .knowhere} and
 * {@code .storage}.
 *
 * <p>The native side reads {@link #threshold} once, when the library loads,
 * and drops lines below it without crossing into the JVM. Raising a level
 * later takes effect here; lowering it below the level in force at load time
 * does not.
 */
final class SegcoreLog {
    private static final String PREFIX = "com.zilliz.milvus.jni.segcore.native.";
    private static final Logger SEGCORE = LoggerFactory.getLogger(PREFIX + "segcore");
    private static final Logger KNOWHERE = LoggerFactory.getLogger(PREFIX + "knowhere");
    private static final Logger STORAGE = LoggerFactory.getLogger(PREFIX + "storage");

    // glog severities
    private static final int INFO = 0;
    private static final int WARNING = 1;
    private static final int ERROR = 2;
    private static final int FATAL = 3;

    private SegcoreLog() {}

    /** The lowest glog severity any of the three loggers accepts. */
    static int threshold() {
        int threshold = FATAL;
        for (Logger logger : new Logger[] {SEGCORE, KNOWHERE, STORAGE}) {
            if (logger.isInfoEnabled()) {
                return INFO;
            }
            if (logger.isWarnEnabled()) {
                threshold = Math.min(threshold, WARNING);
            } else if (logger.isErrorEnabled()) {
                threshold = Math.min(threshold, ERROR);
            }
        }
        return threshold;
    }

    // Called from native code. Text arrives as bytes because glog messages are
    // arbitrary bytes, not the modified UTF-8 NewStringUTF requires.
    static void log(int severity, byte[] file, int line, byte[] message) {
        String path = new String(file, StandardCharsets.UTF_8);
        Logger logger = loggerFor(path);
        String text = path.substring(path.lastIndexOf('/') + 1) + ":" + line + "] "
            + new String(message, StandardCharsets.UTF_8);
        switch (severity) {
            case INFO:
                logger.info(text);
                break;
            case WARNING:
                logger.warn(text);
                break;
            default:
                // slf4j has no FATAL; glog aborts the process after a FATAL line.
                logger.error(severity == FATAL ? "FATAL " + text : text);
                break;
        }
    }

    // Source paths are the compile-time paths of the Milvus build, where
    // Knowhere and milvus-storage are fetched into directories named after them.
    private static Logger loggerFor(String path) {
        if (path.contains("knowhere")) {
            return KNOWHERE;
        }
        if (path.contains("milvus-storage") || path.contains("milvus_storage")) {
            return STORAGE;
        }
        return SEGCORE;
    }
}
