#!/usr/bin/env bash
# Compile and run SegcoreProbe against the Milvus output that build.py installed.
#
# Native glog lines go through slf4j to log4j, as in an executor. The probe
# uses the slf4j and log4j versions Spark 4.0 ships, downloaded once into
# PROBE_JARS (default ~/.cache/segcore-probe).
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
lib="$root/milvus/internal/core/output/lib"
: "${JAVA_HOME:?JAVA_HOME must name the JDK to run the probe with}"
jsig=$(find "$JAVA_HOME/" -name libjsig.so -path '*/server/*' -o -name libjsig.so -path '*/lib/*' | head -n1)
[[ -n "$jsig" ]] || { echo "no libjsig.so under $JAVA_HOME" >&2; exit 1; }

jars="${PROBE_JARS:-$HOME/.cache/segcore-probe}"
mkdir -p "$jars"
central=https://repo1.maven.org/maven2
cp=""
for coord in org/slf4j:slf4j-api:2.0.16 \
             org/apache/logging/log4j:log4j-api:2.24.3 \
             org/apache/logging/log4j:log4j-core:2.24.3 \
             org/apache/logging/log4j:log4j-slf4j2-impl:2.24.3; do
  IFS=: read -r group artifact version <<<"$coord"
  jar="$jars/$artifact-$version.jar"
  [[ -s "$jar" ]] || curl -fsSL -o "$jar" "$central/$group/$artifact/$version/$artifact-$version.jar"
  cp="$cp:$jar"
done
cp=${cp#:}

classes=$(mktemp -d)
trap 'rm -rf "$classes"' EXIT
src="$root/native/segcore/src"
"$JAVA_HOME/bin/javac" -d "$classes" -cp "$cp" \
  "$src"/main/java/com/zilliz/milvus/jni/segcore/*.java \
  "$src"/test/java/com/zilliz/milvus/jni/segcore/SegcoreProbe.java
LD_PRELOAD="$jsig" LD_LIBRARY_PATH="$lib" \
  "$JAVA_HOME/bin/java" -Xcheck:jni -Djava.library.path="$lib" \
  -Dlog4j2.configurationFile="$src/test/resources/log4j2-probe.properties" \
  -cp "$classes:$cp" com.zilliz.milvus.jni.segcore.SegcoreProbe
