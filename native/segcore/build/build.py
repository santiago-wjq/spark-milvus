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

* the Conan profile (settings only, so the recipe's default_options apply) and
  the compiler flags scripts/3rdparty_build.sh gives packages built from source,
* a work directory outside the source tree, linked as milvus/cmake_build
  because the Milvus scripts hard-code that path,
* the C++ protobuf sources, generated from this repository's milvus-proto
  submodule, which must be the version Milvus's go.mod requires,
* the Rust toolchain scripts/install_deps.sh installs, which must be present,
* -DWITH_JNI=OFF and the engine sources Milvus pins (see build_core),
* a check that lib/ holds no storage library a build with JNI on copied there,
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
PROTO_MODULE = "github.com/milvus-io/milvus-proto/go-api/v3"
# The Go module lives in the repository's go-api directory, so its release
# tags carry that directory: go.mod's v3.0.6 is the tag go-api/v3.0.6.
PROTO_TAG_PREFIX = "go-api/"

# What only a build with -DWITH_JNI=ON writes to the work directory's lib/:
# milvus-storage's JNI target copies its own library and the storage library
# there after linking (cmake/CopyDependencies.cmake, OUTPUT_DIR
# ${CMAKE_BINARY_DIR}/lib).
JNI_BUILD_COPIES = ("libmilvus-storage.so", "libmilvus-storage-jni.so")


def run(cmd, cwd=None, env=None):
    print("+", " ".join(shlex.quote(str(c)) for c in cmd), flush=True)
    subprocess.run([str(c) for c in cmd], cwd=cwd, env=env, check=True)


def git(repo, *args):
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True,
                          text=True).stdout.strip()


def script_value(script, pattern, what):
    """The first value a Milvus script sets in the expected form, so a gitlink
    move that changes the form stops here instead of building with a stale one."""
    m = re.search(pattern, (MILVUS / "scripts" / script).read_text(), re.M)
    if not m:
        sys.exit(f"scripts/{script} no longer sets {what} in the expected form")
    return m.group(1)


def milvus_rust():
    """The Rust toolchain the Milvus scripts install for tantivy and storage."""
    return script_value("install_deps.sh", r'^RUST_VERSION="([0-9.]+)"$', "RUST_VERSION")


def check_rust(version, env):
    listed = subprocess.run(["rustup", "toolchain", "list"], env=env, check=True, capture_output=True,
                            text=True).stdout.split()
    if not any(name == version or name.startswith(version + "-") for name in listed):
        sys.exit(f"Milvus builds with Rust {version}, which rustup does not have; "
                 f"install it: rustup toolchain install {version}")


def conan_env(env):
    """The environment scripts/3rdparty_build.sh gives conan install: the flags
    for packages built from source, and no preloaded or extra libraries in
    Conan's own Python process."""
    env = dict(env)
    for name in ("CXXFLAGS", "CFLAGS", "CMAKE_POLICY_VERSION_MINIMUM"):
        env[name] = script_value("3rdparty_build.sh", r'^export ' + name + r'="?([^"$\n]*)"?$', name)
    env.pop("LD_PRELOAD", None)
    env.pop("LD_LIBRARY_PATH", None)
    return env


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
         "-pr:h", profile, "-pr:b", profile], cwd=work, env=conan_env(env))
    stamp.write_text(digest + "\n")


def cpp_proto_commands():
    """The C++ protoc invocations of scripts/generate_proto.sh, in its order.

    Only the --cpp_out lines are run: the Go outputs belong to the Milvus Go
    build and would rewrite tracked files in the submodule. The list names
    milvus-proto's files as well as Milvus's own; --proto_path resolves each.
    """
    script = (MILVUS / "scripts" / "generate_proto.sh").read_text()
    protos = re.findall(r"^\$\{protoc_opt\} --cpp_out=\$CPP_SRC_DIR/src/pb (\S+?\.proto)", script, re.M)
    if not protos:
        sys.exit("generate_proto.sh no longer lists C++ protoc commands in the expected form")
    return protos


def proto_revision(version):
    """How a milvus-proto version in milvus/go.mod names its commit.

    A pseudo-version ends in the commit's 12-character prefix; a release is a
    tag of the milvus-proto repository under the module's directory.
    """
    pseudo = re.fullmatch(r"v\d+\.\d+\.\d+-(?:\S+\.)?\d{14}-([0-9a-f]{12})", version)
    if pseudo:
        return "prefix", pseudo.group(1)
    if re.fullmatch(r"v\d+\.\d+\.\d+(?:-[0-9A-Za-z.]+)?", version):
        return "tag", PROTO_TAG_PREFIX + version
    sys.exit(f"milvus/go.mod names milvus-proto {version}, which is neither a release nor a pseudo-version")


def check_milvus_proto():
    """The milvus-proto gitlink must be the version Milvus's go.mod requires."""
    gomod = (MILVUS / "go.mod").read_text()
    m = re.search(r"^\s*" + re.escape(PROTO_MODULE) + r"\s+(\S+)", gomod, re.M)
    if not m:
        sys.exit(f"{PROTO_MODULE} is not required by milvus/go.mod")
    version = m.group(1)
    head = git(MILVUS_PROTO, "rev-parse", "HEAD")
    kind, wanted = proto_revision(version)
    if kind == "prefix":
        ok = head.startswith(wanted)
    else:
        try:
            wanted = git(MILVUS_PROTO, "rev-parse", "--verify", "--quiet", f"refs/tags/{wanted}^{{commit}}")
        except subprocess.CalledProcessError:
            sys.exit(f"milvus/go.mod requires milvus-proto {version}, but the milvus-proto submodule has no "
                     f"tag {wanted}; fetch it: git -C milvus-proto fetch origin tag {wanted}")
        ok = head == wanted
    if not ok:
        sys.exit(f"milvus-proto is at {head[:12]}, but milvus/go.mod requires {version} ({wanted[:12]}); "
                 "move the milvus-proto gitlink with the milvus gitlink")


def generate_proto(work, env):
    check_milvus_proto()
    protoc = work / "bin" / "protoc"
    pb = CORE / "src" / "pb"
    for proto in cpp_proto_commands():
        run([protoc, f"--proto_path={MILVUS_PROTO / 'proto'}", "--proto_path=.", f"--cpp_out={pb}", proto],
            cwd=MILVUS / "pkg" / "proto", env=env)


def check_jni_build_copies(work):
    """Stop on storage libraries an earlier build with the engines' JNI on left.

    Milvus installs the work directory's lib/ whole, after the storage
    library this build links, so such a copy would take that library's place
    in the output and segcore would run against an older milvus-storage.
    Setting WITH_JNI=OFF again clears the cache entry, not the files.
    """
    copies = [work / "lib" / name for name in JNI_BUILD_COPIES if (work / "lib" / name).exists()]
    if copies:
        sys.exit("a build with WITH_JNI=ON left " + ", ".join(str(c) for c in copies) + ", which the "
                 "install would put in place of the milvus-storage this build links; remove them first: rm "
                 + " ".join(shlex.quote(str(c)) for c in copies))


def build_core(env, jobs):
    env = dict(env)
    # The engines' JNI stays off: the connector loads them through its own
    # bundle (see README). Explicit values also clear what an earlier
    # configure cached: JNI on, or an engine source supplied through
    # FETCHCONTENT_SOURCE_DIR_*, which an empty value returns to the commit
    # Milvus pins. The files a build with JNI on copied stay;
    # check_jni_build_copies stops on them.
    env["CMAKE_EXTRA_ARGS"] = " ".join([
        "-DWITH_JNI=OFF",
        "-DFETCHCONTENT_SOURCE_DIR_KNOWHERE=",
        "-DFETCHCONTENT_SOURCE_DIR_MILVUS-STORAGE=",
    ])
    # core_build.sh runs `ninja -j ${jobs}`, nproc when unset.
    if jobs:
        env["jobs"] = str(jobs)
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
    parser.add_argument("--jobs", type=int, default=0,
                        help="parallel compile jobs of the Milvus build (default: core_build.sh's nproc)")
    parser.add_argument("--only-jni", action="store_true",
                        help="rebuild the segcore JNI against an existing Milvus output")
    args = parser.parse_args()

    profile = HERE / "profiles" / args.platform
    if not profile.exists():
        sys.exit(f"no Conan profile for {args.platform} under {HERE / 'profiles'}")
    if not (CORE / "conanfile.py").exists():
        sys.exit("initialize the milvus submodule first: git submodule update --init --checkout milvus")
    if "JAVA_HOME" not in os.environ:
        sys.exit("JAVA_HOME must name the JDK whose headers the JNI targets compile against")

    work = args.work_dir.resolve()
    work.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    rust = milvus_rust()
    check_rust(rust, env)
    env["RUSTUP_TOOLCHAIN"] = rust
    # Cargo's built-in git client ignores git's own URL rewrites and
    # credentials; the CLI applies the same configuration git checkouts use.
    env["CARGO_NET_GIT_FETCH_WITH_CLI"] = "true"

    if not args.only_jni:
        check_jni_build_copies(work)
        conan_install(work, profile, env)
        link_work_dir(work)
        generate_proto(work, env)
        build_core(env, args.jobs)
    build_jni(work, env)
    print(f"installed under {OUTPUT}")


if __name__ == "__main__":
    main()
