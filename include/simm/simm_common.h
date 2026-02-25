/*
 * Copyright (c) 2026 The Scitix Authors.
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

#pragma once

#include <memory>
#include <optional>
#include <span>

namespace simm {

namespace common {
  struct MemBlock;
}  // namespace common

namespace clnt {

class KVStore;
class DataView;

struct MrExt {
  void *mem_desc{nullptr};
};

/*
 * Register user buffer memory region into SiMM client to realize zero-copy transfer
 * @param buf pointer to user buffer
 * @param len length of user buffer
 * @return MR extension info registered in SiMM client, std::nullopt if registration failed
 */
std::optional<MrExt> RegisterMr(uintptr_t buf, size_t len);

/*
 * Deregister user buffer memory region from SiMM client
 * @param mr MR extension info already registered in SiMM client
 * @return 0 if success, non-zero error code if failed
 */
int16_t DeregisterMr(MrExt &mr);

class Data {
 public:
  Data(const Data &) = delete;
  Data &operator=(const Data &) = delete;

  Data(Data &&rhs) noexcept;
  Data &operator=(Data &&rhs) noexcept;
  ~Data();

  std::span<char> AsRef();
  const std::span<char> AsRef() const;

 private:
  explicit Data(size_t len);

  std::unique_ptr<common::MemBlock> memp_;

  friend class KVStore;
  friend class DataView;
};

class DataView {
 public:
  DataView(Data &data);
  DataView(uintptr_t buf, size_t len, const MrExt &mr);

  std::shared_ptr<common::MemBlock> GetMemp() { return memp_; }

  // TODO: replace span because it's C++20
  std::span<char> AsRef();
  const std::span<char> AsRef() const;

 private:
  std::shared_ptr<common::MemBlock> memp_;

  friend class KVStore;
};

}  // namespace clnt
}  // namespace simm
