#!/usr/bin/env bash
# Compile and run SegcoreProbe against the Milvus output that build.py
# installed, with sealed segments from the api collection fixture.
# Native glog lines go to stderr, as in an executor.
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
lib="$root/milvus/internal/core/output/lib"
: "${JAVA_HOME:?JAVA_HOME must name the JDK to run the probe with}"
jsig=$(find "$JAVA_HOME/" -name libjsig.so -path '*/server/*' -o -name libjsig.so -path '*/lib/*' | head -n1)
[[ -n "$jsig" ]] || { echo "no libjsig.so under $JAVA_HOME" >&2; exit 1; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
classes="$work/classes"
src="$root/native/segcore/src"
"$JAVA_HOME/bin/javac" -d "$classes" \
  "$src"/main/java/com/zilliz/milvus/jni/segcore/*.java \
  "$src"/test/java/com/zilliz/milvus/jni/segcore/SegcoreProbe.java

# The sealed checks load a copy of the api collection fixture. Its literal
# input rows and manifest paths reach the probe as tab-separated lines, so the
# expected answers never come from a read of the stored bytes. The fixture's
# keys are relative to its directory, as remote keys are to their bucket;
# segcore roots local storage at / and takes the absolute key Milvus would
# have written for local storage.
fixture="$work/api"
cp -R "$root/core/src/test/resources/read-fixtures/collections/api" "$fixture"
python3 -I - "$fixture" >"$work/api.tsv" <<'EOF'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
snapshot = json.loads((root / "snapshot.json").read_text())
expected = json.loads((root / "expected.json").read_text())
for entry in snapshot["storagev2_manifest_list"]:
    manifest = json.loads(entry["manifest"])
    manifest["base_path"] = str(root / manifest["base_path"])
    print("manifest", entry["segmentID"], json.dumps(manifest, separators=(",", ":")), sep="\t")
for segment in expected["segments"]:
    for deleted in segment["deletedIds"]:
        print("delete", segment["id"], deleted, sep="\t")
    for row in segment["rows"]:
        category = "null" if row["category"] is None else row["category"]
        print("row", segment["id"], row["id"], category, sep="\t")
EOF

# The probe leaves through System.exit, as an executor does. A JVM that has
# not exited two minutes after it started fails the probe: the run itself
# takes seconds, and an exit that hangs never ends.
# libjsig is preloaded into the JVM alone.
status=0
timeout --kill-after=10s 120s \
  env LD_PRELOAD="$jsig" LD_LIBRARY_PATH="$lib" \
  "$JAVA_HOME/bin/java" -Xcheck:jni -Djava.library.path="$lib" \
  -cp "$classes" com.zilliz.milvus.jni.segcore.SegcoreProbe \
  "$work/segcore" "$work/api.tsv" || status=$?
if [[ $status -eq 124 || $status -eq 137 ]]; then
  echo "SegcoreProbe: the JVM did not exit" >&2
fi
exit "$status"
