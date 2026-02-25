/*
 * Copyright (c) 2026 Scitix Tech PTE. LTD.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gflags/gflags.h>
#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>
#include <nanobind/stl/function.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/pair.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <cstdint>

#include "simm/simm_common.h"
#include "simm/simm_kv.h"

namespace nb = nanobind;
using namespace nb::literals;

namespace clnt = simm::clnt;

bool set_flag(const std::string &name, const std::string &value) {
  return gflags::SetCommandLineOption(name.c_str(), value.c_str()).empty() == false;
}

NB_MODULE(_kv, m) {
  m.def("set_flag", &set_flag, "name"_a, "value"_a, "Set a flag using GFLAGS API. Returns true if successful.");
  nb::class_<clnt::MrExt>(m, "MrExt");
  nb::class_<clnt::Data>(m, "Data").def(
      "AsRef",
      [](clnt::Data &this_) {
        auto span = this_.AsRef();
        return nb::ndarray<uint8_t, nb::pytorch>(span.data(), {span.size()});
      },
      nb::rv_policy::reference_internal);
  m.def("register_mr", &clnt::RegisterMr, nb::call_guard<nb::gil_scoped_release>());
  m.def("deregister_mr", &clnt::DeregisterMr, nb::call_guard<nb::gil_scoped_release>());
  nb::class_<clnt::DataView>(m, "DataView")
      .def(nb::init<clnt::Data &>())
      .def(nb::init<uintptr_t, size_t, clnt::MrExt &>())
      .def(
          "AsRef",
          [](clnt::DataView &this_) {
            auto span = this_.AsRef();
            return nb::ndarray<uint8_t, nb::pytorch>(span.data(), {span.size()});
          },
          nb::rv_policy::reference_internal);
  nb::class_<clnt::KVStore>(m, "KVStore")
      .def(nb::init<>())
      .def(
          "Allocate",
          [](clnt::KVStore &this_, size_t size) { return this_.Allocate(size); },
          nb::call_guard<nb::gil_scoped_release>())
      .def(
          "Get",
          [](clnt::KVStore &this_, const std::string &key, clnt::DataView &get_data) {
            return this_.Get(key, get_data);
          },
          nb::call_guard<nb::gil_scoped_release>())
      .def(
          "Put",
          [](clnt::KVStore &this_, const std::string &key, const clnt::DataView &put_data) {
            return this_.Put(key, put_data);
          },
          nb::call_guard<nb::gil_scoped_release>())
      .def(
          "Exists",
          [](clnt::KVStore &this_, const std::string &key) { return this_.Exists(key); },
          nb::call_guard<nb::gil_scoped_release>())
      .def(
          "Delete",
          [](clnt::KVStore &this_, const std::string &key) { return this_.Delete(key); },
          nb::call_guard<nb::gil_scoped_release>())
      .def(
          "AsyncGet",
          [](clnt::KVStore &this_, const std::string &key, clnt::DataView &get_data, clnt::AsyncCallback callback) {
            return this_.AsyncGet(key, get_data, std::move(callback));
          },
          nb::call_guard<nb::gil_scoped_release>())
      .def(
          "AsyncPut",
          [](clnt::KVStore &this_,
             const std::string &key,
             const clnt::DataView &put_data,
             clnt::AsyncCallback callback) { return this_.AsyncPut(key, put_data, std::move(callback)); },
          nb::call_guard<nb::gil_scoped_release>())
      .def(
          "AsyncDelete",
          [](clnt::KVStore &this_, const std::string &key, clnt::AsyncCallback callback) {
            return this_.AsyncDelete(key, std::move(callback));
          },
          nb::call_guard<nb::gil_scoped_release>())
      .def(
          "AsyncExists",
          [](clnt::KVStore &this_, const std::string &key, clnt::AsyncCallback callback) {
            return this_.AsyncExists(key, std::move(callback));
          },
          nb::call_guard<nb::gil_scoped_release>())
      .def(
          "MPut",
          [](clnt::KVStore &this_, const std::vector<std::string> &keys, const std::vector<clnt::DataView> &datas) {
            return this_.MPut(keys, datas);
          },
          nb::call_guard<nb::gil_scoped_release>())
      .def(
          "MGet",
          [](clnt::KVStore &this_, const std::vector<std::string> &keys, const std::vector<clnt::DataView> &datas) {
            return this_.MGet(keys, datas);
          },
          nb::call_guard<nb::gil_scoped_release>())
      .def(
          "MExists",
          [](clnt::KVStore &this_, const std::vector<std::string> &keys) {
            return this_.MExists(keys);
          },
          nb::call_guard<nb::gil_scoped_release>());
}