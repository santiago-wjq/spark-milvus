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

# libmilvus-segcore-jni: JNI over the installed Milvus C headers. It compiles
# only against the *_c.h interface Milvus installs and links the libraries the
# same Milvus build produced, so it never sees segcore's C++ types.
cmake_minimum_required(VERSION 3.20)
project(milvus_segcore_jni CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT MILVUS_OUTPUT)
  message(FATAL_ERROR "MILVUS_OUTPUT must name milvus/internal/core/output")
endif()

find_package(JNI REQUIRED)

find_library(MILVUS_CORE milvus_core PATHS "${MILVUS_OUTPUT}/lib" NO_DEFAULT_PATH REQUIRED)

add_library(milvus-segcore-jni SHARED segcore_jni.cpp)
target_include_directories(milvus-segcore-jni PRIVATE "${MILVUS_OUTPUT}/include" ${JNI_INCLUDE_DIRS})
target_link_libraries(milvus-segcore-jni PRIVATE "${MILVUS_CORE}")
# The library sits next to libmilvus_core and its dependency closure.
set_target_properties(milvus-segcore-jni PROPERTIES
  BUILD_RPATH "${MILVUS_OUTPUT}/lib"
  INSTALL_RPATH "$ORIGIN")
target_link_options(milvus-segcore-jni PRIVATE "-Wl,--no-undefined")

install(TARGETS milvus-segcore-jni LIBRARY DESTINATION lib)
