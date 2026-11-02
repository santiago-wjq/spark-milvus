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
"""Build libmilvus_core, the plan parser and the segcore JNI with Milvus's own build.

The Milvus submodule is the parent build: its conanfile selects every package
and option, its CMake compiles Knowhere and milvus-storage from their pinned
sources, and its scripts generate the protobuf sources and the plan parser.
This driver only supplies what Milvus leaves to its environment:

* the Conan profile (settings only, so the recipe's default_options apply),
* a work directory outside the source tree, linked as milvus/cmake_build
  because the Milvus scripts hard-code that path,
* -DWITH_JNI=ON, which turns on the upstream JNI targets of Knowhere and
  milvus-storage inside the same build,
* the segcore JNI library, compiled against the installed Milvus output.
"""
import argparse
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

# Rust toolchain the Milvus scripts install for the storage bridge and tantivy.
MILVUS_RUST = "1.94.1"


def run(cmd, cwd=None, env=None):
    print("+", " ".join(shlex.quote(str(c)) for c in cmd), flush=True)
    subprocess.run([str(c) for c in cmd], cwd=cwd, env=env, check=True)


def link_work_dir(work):
    """Point milvus/cmake_build at the work directory."""
    link = MILVUS / "cmake_build"
    if link.is_symlink():
        if link.resolve() == work.resolve():
            return
        link.unlink()
    elif link.exists():
        sys.exit(f"{link} exists and is not a symlink; move it away first")
    link.symlink_to(work)


def conan_install(work, profile, env):
    toolchain = work / "conan" / "conan_toolchain.cmake"
    if toolchain.exists():
        print(f"conan: reusing {toolchain}")
        return
    run(["conan", "install", CORE, "--output-folder", work / "conan", "--build=missing",
         "-pr:h", profile, "-pr:b", profile], cwd=work, env=env)


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


def generate_proto(work, env):
    run(["bash", MILVUS / "scripts" / "download_milvus_proto.sh"], cwd=MILVUS, env=env)
    protoc = work / "bin" / "protoc"
    api = work / "thirdparty" / "milvus-proto" / "proto"
    pb = CORE / "src" / "pb"
    for proto in cpp_proto_commands():
        run([protoc, f"--proto_path={api}", "--proto_path=.", f"--cpp_out={pb}", proto],
            cwd=MILVUS / "pkg" / "proto", env=env)


def build_core(env, jobs):
    env = dict(env)
    env["CMAKE_EXTRA_ARGS"] = "-DWITH_JNI=ON"
    if jobs:
        env["CMAKE_BUILD_PARALLEL_LEVEL"] = str(jobs)
    run(["bash", MILVUS / "scripts" / "build_plan_parser.sh"], cwd=MILVUS, env=env)
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

    work = args.work_dir.resolve()
    work.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env["RUSTUP_TOOLCHAIN"] = MILVUS_RUST

    if not args.only_jni:
        conan_install(work, profile, env)
        link_work_dir(work)
        generate_proto(work, env)
        build_core(env, args.jobs)
    build_jni(work, env)
    print(f"installed under {OUTPUT}")


if __name__ == "__main__":
    main()
