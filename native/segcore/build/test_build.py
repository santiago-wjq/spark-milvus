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
"""What build.py reads from the Milvus submodule, without building anything.

The checks against the scripts need the milvus submodule and skip without it.
"""
import importlib.util
import pathlib
import unittest

HERE = pathlib.Path(__file__).resolve().parent
_SPEC = importlib.util.spec_from_file_location("segcore_build", HERE / "build.py")
build = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(build)


class ProtoRevisionTest(unittest.TestCase):
    def test_a_release_is_the_tag_under_the_module_directory(self):
        self.assertEqual(build.proto_revision("v3.0.6"), ("tag", "go-api/v3.0.6"))
        self.assertEqual(build.proto_revision("v3.1.0-rc.1"), ("tag", "go-api/v3.1.0-rc.1"))

    def test_a_pseudo_version_names_its_commit_prefix(self):
        for version in ("v3.0.0-20251103083745-84b82513689e",
                        "v3.0.2-0.20251103083745-84b82513689e",
                        "v3.1.0-rc.1.0.20251103083745-84b82513689e"):
            with self.subTest(version=version):
                self.assertEqual(build.proto_revision(version), ("prefix", "84b82513689e"))

    def test_anything_else_stops_the_build(self):
        with self.assertRaises(SystemExit):
            build.proto_revision("master")


@unittest.skipUnless((build.MILVUS / "scripts" / "generate_proto.sh").is_file(),
                     "the milvus submodule is not initialized")
class MilvusScriptsTest(unittest.TestCase):
    def test_the_rust_toolchain_is_the_one_milvus_installs(self):
        self.assertRegex(build.milvus_rust(), r"^\d+\.\d+(\.\d+)?$")

    def test_conan_gets_the_flags_milvus_gives_packages_built_from_source(self):
        env = build.conan_env({"LD_PRELOAD": "libjsig.so", "LD_LIBRARY_PATH": "/elsewhere", "PATH": "/bin"})
        self.assertIn("-Wno-error=address", env["CXXFLAGS"])
        self.assertIn("-Wno-error=address", env["CFLAGS"])
        self.assertTrue(env["CMAKE_POLICY_VERSION_MINIMUM"])
        self.assertNotIn("LD_PRELOAD", env)
        self.assertNotIn("LD_LIBRARY_PATH", env)
        self.assertEqual(env["PATH"], "/bin")

    def test_the_cpp_protos_include_the_messages_segcore_takes(self):
        protos = build.cpp_proto_commands()
        for proto in ("schema.proto", "common.proto", "plan.proto", "segcore.proto", "index_coord.proto"):
            self.assertIn(proto, protos)


if __name__ == "__main__":
    unittest.main()
