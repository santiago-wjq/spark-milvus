#!/usr/bin/env bash
# Compile and run SegcoreProbe against the Milvus output that build.py installed.
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
lib="$root/milvus/internal/core/output/lib"
: "${JAVA_HOME:?JAVA_HOME must name the JDK to run the probe with}"
jsig=$(find "$JAVA_HOME" -name libjsig.so -path '*/server/*' -o -name libjsig.so -path '*/lib/*' | head -n1)
[[ -n "$jsig" ]] || { echo "no libjsig.so under $JAVA_HOME" >&2; exit 1; }
classes=$(mktemp -d)
trap 'rm -rf "$classes"' EXIT
src="$root/native/segcore/src"
"$JAVA_HOME/bin/javac" -d "$classes" \
  "$src"/main/java/com/zilliz/milvus/jni/segcore/*.java \
  "$src"/test/java/com/zilliz/milvus/jni/segcore/SegcoreProbe.java
LD_PRELOAD="$jsig" LD_LIBRARY_PATH="$lib" \
  "$JAVA_HOME/bin/java" -Xcheck:jni -Djava.library.path="$lib" -cp "$classes" \
  com.zilliz.milvus.jni.segcore.SegcoreProbe
