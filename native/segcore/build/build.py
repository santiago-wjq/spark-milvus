#!/usr/bin/env python3
# Copyright 2026 Zilliz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Build libmilvus_core and the segcore JNI with Milvus's own build.

The Milvus submodule is the parent build: its conanfile selects every package
and option, and its CMake compiles Knowhere and milvus-storage from their pinned
sources. This driver only supplies what Milvus leaves to its environment:

* the Conan profile (settings only, so the recipe's default_options apply),
* a work directory outside the source tree, linked as milvus/cmake_build
  because the Milvus scripts hard-code that path,
* the C++ protobuf sources, generated from this repository's milvus-proto
  submodule, which must be the commit Milvus's go.mod names,
* a check that core's copy of plan.proto is the file at the milvus gitlink,
* -DWITH_JNI=OFF for now, and the engine sources Milvus pins (see build_core),
* the segcore JNI library, compiled against the installed Milvus output.

The Go plan parser is not built: the connector builds planpb.PlanNode itself.
"""
import argparse
import hashlib
import os
import pathlib
import re
import shlex
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[2]
MILVUS = ROOT / "milvus"
CORE = MILVUS / "internal" / "core"
OUTPUT = CORE / "output"
JNI_SOURCE = ROOT / "native" / "segcore" / "src" / "main" / "cpp"
MILVUS_PROTO = ROOT / "milvus-proto"
CORE_PLAN_PROTO = ROOT / "core" / "src" / "main" / "protobuf" / "plan.proto"
PROTO_MODULE = "github.com/milvus-io/milvus-proto/go-api/v3"

# Rust toolchain the Milvus scripts install for the storage bridge and tantivy.
MILVUS_RUST = "1.94.1"


def run(cmd, cwd=None, env=None):
    print("+", " ".join(shlex.quote(str(c)) for c in cmd), flush=True)
    subprocess.run([str(c) for c in cmd], cwd=cwd, env=env, check=True)


def link_work_dir(work):
    """Point milvus/cmake_build at the work directory."""
    link = MILVUS / "cmake_build"
    if link.is_symlink() and link.resolve() != work.resolve():
        link.unlink()
    elif link.exists() and not link.is_symlink():
        sys.exit(f"{link} exists and is not a symlink; move it away first")
    if not link.is_symlink():
        link.symlink_to(work)
    # Milvus ignores a cmake_build directory, not a link of that name; exclude
    # it locally so the submodule stays clean without changing tracked files.
    exclude = pathlib.Path(git(MILVUS, "rev-parse", "--path-format=absolute", "--git-path", "info/exclude"))
    entries = exclude.read_text().splitlines() if exclude.exists() else []
    if "/cmake_build" not in entries:
        exclude.parent.mkdir(parents=True, exist_ok=True)
        with exclude.open("a") as f:
            f.write("/cmake_build\n")


def conan_install(work, profile, env):
    """Install the Milvus conanfile's graph, reusing the output only while the
    conanfile and the profile are the ones it was installed from: moving the
    milvus gitlink changes package versions, and a reused output would build
    the new sources against the old packages."""
    output = work / "conan"
    stamp = output / "inputs.sha256"
    digest = hashlib.sha256((CORE / "conanfile.py").read_bytes() + b"\0" + profile.read_bytes()).hexdigest()
    if (output / "conan_toolchain.cmake").exists() and stamp.exists() and stamp.read_text().strip() == digest:
        print(f"conan: reusing {output}, installed from the same conanfile and profile")
        return
    # Conan 1 reads the profile and fails on its Conan 2 sections, which says
    # nothing about the version.
    version = subprocess.run(["conan", "--version"], env=env, check=True, capture_output=True,
                             text=True).stdout.strip()
    if not re.search(r"\b2\.\d+\.\d+", version):
        sys.exit(f"the Milvus conanfile needs Conan 2 (see README); `conan --version` says: {version}")
    run(["conan", "install", CORE, "--output-folder", output, "--build=missing",
         "-pr:h", profile, "-pr:b", profile], cwd=work, env=env)
    stamp.write_text(digest + "\n")


def cpp_proto_commands():
    """The C++ protoc invocations of scripts/generate_proto.sh, in its order.

    Only the --cpp_out lines are run: the Go outputs belong to the Milvus Go
    build and would rewrite tracked files in the submodule.
    """
    script = (MILVUS / "scripts" / "generate_proto.sh").read_text()
    protos = re.findall(r"^\$\{protoc_opt\} --cpp_out=\$CPP_SRC_DIR/src/pb (\S+?\.proto)", script, re.M)
    if not protos:
        sys.exit("generate_proto.sh no longer lists C++ protoc commands in the expected form")
    return protos


def git(repo, *args):
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True,
                          text=True).stdout.strip()


def check_milvus_proto():
    """The milvus-proto gitlink must be the version Milvus's go.mod requires.

    A pseudo-version ends in the commit's 12-character prefix; any other
    version is a tag of the milvus-proto repository.
    """
    gomod = (MILVUS / "go.mod").read_text()
    m = re.search(r"^\s*" + re.escape(PROTO_MODULE) + r"\s+(\S+)", gomod, re.M)
    if not m:
        sys.exit(f"{PROTO_MODULE} is not required by milvus/go.mod")
    version = m.group(1)
    head = git(MILVUS_PROTO, "rev-parse", "HEAD")
    pseudo = re.fullmatch(r"v\d+\.\d+\.\d+-(?:\S+\.)?\d{14}-([0-9a-f]{12})", version)
    if pseudo:
        wanted = pseudo.group(1)
        ok = head.startswith(wanted)
    else:
        wanted = git(MILVUS_PROTO, "rev-parse", f"{version}^{{commit}}")
        ok = head == wanted
    if not ok:
        sys.exit(f"milvus-proto is at {head[:12]}, but milvus/go.mod requires {version} ({wanted[:12]}); "
                 "move the milvus-proto gitlink with the milvus gitlink")


def check_plan_proto():
    """core's copy of plan.proto must be the file at the milvus gitlink.

    core builds segcore's plans from that copy, since the milvus submodule is
    opt-in; a gitlink move that changes plan.proto stops here until the copy
    follows, so the change is reviewed with the C interface changes.
    """
    pinned = MILVUS / "pkg" / "proto" / "plan.proto"
    if CORE_PLAN_PROTO.read_bytes() != pinned.read_bytes():
        sys.exit(f"{CORE_PLAN_PROTO.relative_to(ROOT)} differs from {pinned.relative_to(ROOT)} at the milvus "
                 f"gitlink; copy it: cp {pinned.relative_to(ROOT)} {CORE_PLAN_PROTO.relative_to(ROOT)}")


def generate_proto(work, env):
    check_milvus_proto()
    protoc = work / "bin" / "protoc"
    pb = CORE / "src" / "pb"
    for proto in cpp_proto_commands():
        run([protoc, f"--proto_path={MILVUS_PROTO / 'proto'}", "--proto_path=.", f"--cpp_out={pb}", proto],
            cwd=MILVUS / "pkg" / "proto", env=env)


def build_core(env, jobs):
    env = dict(env)
    # The engines' JNI stays off: milvus-storage's JNI post-build step copies
    # to ${CMAKE_SOURCE_DIR}/../java/, which resolves outside its own tree
    # when Milvus is the top-level project, and the build fails. The fix
    # (CMAKE_CURRENT_SOURCE_DIR) is in milvus-storage main, past the commit
    # the pinned Milvus uses; until Milvus moves to it, the connector's
    # Knowhere and storage JNI come from native/build. Explicit values also
    # clear what an earlier configure cached: JNI on, or an engine source
    # supplied through FETCHCONTENT_SOURCE_DIR_*, which an empty value returns
    # to the commit Milvus pins.
    env["CMAKE_EXTRA_ARGS"] = " ".join([
        "-DWITH_JNI=OFF",
        "-DFETCHCONTENT_SOURCE_DIR_KNOWHERE=",
        "-DFETCHCONTENT_SOURCE_DIR_MILVUS-STORAGE=",
    ])
    if jobs:
        env["CMAKE_BUILD_PARALLEL_LEVEL"] = str(jobs)
    run(["bash", MILVUS / "scripts" / "core_build.sh", "-t", "Release"], cwd=MILVUS, env=env)


def build_jni(work, env):
    build = work / "segcore-jni"
    run(["cmake", "-S", JNI_SOURCE, "-B", build, "-G", "Ninja",
         "-DCMAKE_BUILD_TYPE=Release",
         "-DCMAKE_CXX_COMPILER=g++-12",
         f"-DMILVUS_OUTPUT={OUTPUT}",
         f"-DCMAKE_INSTALL_PREFIX={OUTPUT}"], env=env)
    run(["cmake", "--build", build], env=env)
    run(["cmake", "--install", build], env=env)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--platform", default="linux-x86_64")
    parser.add_argument("--work-dir", type=pathlib.Path, required=True,
                        help="build directory outside the source tree; linked as milvus/cmake_build")
    parser.add_argument("--jobs", type=int, default=0)
    parser.add_argument("--only-jni", action="store_true",
                        help="rebuild the segcore JNI against an existing Milvus output")
    args = parser.parse_args()

    profile = HERE / "profiles" / args.platform
    if not profile.exists():
        sys.exit(f"no Conan profile for {args.platform} under {HERE / 'profiles'}")
    if not (CORE / "conanfile.py").exists():
        sys.exit("initialize the milvus submodule first: git submodule update --init milvus")
    if "JAVA_HOME" not in os.environ:
        sys.exit("JAVA_HOME must name the JDK whose headers the JNI targets compile against")
    check_plan_proto()

    work = args.work_dir.resolve()
    work.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env["RUSTUP_TOOLCHAIN"] = MILVUS_RUST
    # Cargo's built-in git client ignores git's own URL rewrites and
    # credentials; the CLI applies the same configuration git checkouts use.
    env["CARGO_NET_GIT_FETCH_WITH_CLI"] = "true"

    if not args.only_jni:
        conan_install(work, profile, env)
        link_work_dir(work)
        generate_proto(work, env)
        build_core(env, args.jobs)
    build_jni(work, env)
    print(f"installed under {OUTPUT}")


if __name__ == "__main__":
    main()
