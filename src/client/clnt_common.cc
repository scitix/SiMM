#include "simm/simm_common.h"

#include <span>
#include <utility>

#include "clnt_mempool.h"
#include "common/errcode/errcode_def.h"

namespace simm {
namespace clnt {

Data::Data(size_t len) : memp_{std::make_unique<simm::common::MemBlock>(len)} {
  MempoolBase::Instance().Allocate(memp_.get());
}

Data::Data(Data &&rhs) noexcept : memp_{std::move(rhs.memp_)} {}

Data &Data::operator=(Data &&rhs) noexcept {
  std::swap(memp_, rhs.memp_);
  return *this;
}

Data::~Data() {
  if (!memp_) {
    return;
  }

  MempoolBase::Instance().Release(memp_.get());
}

std::span<char> Data::AsRef() {
  return {memp_->buf, memp_->len};
}

const std::span<char> Data::AsRef() const {
  return {memp_->buf, memp_->len};
}

std::optional<MrExt> RegisterMr(uintptr_t buf, size_t len) {
  auto memp = simm::common::MemBlock((char *)buf, len);
  auto ret = MempoolBase::Instance().RegisterMr(&memp);
  if (ret != CommonErr::OK) {
    return std::nullopt;
  }

  return MrExt{memp.descr};
}

int16_t DeregisterMr(MrExt &mr) {
  auto ret = MempoolBase::Instance().DeregisterMr(mr.mem_desc);
  if (ret == CommonErr::OK) {
    mr.mem_desc = nullptr;
  }
  return ret;
}

DataView::DataView(Data &data)
    : DataView(reinterpret_cast<uintptr_t>(data.memp_->buf), data.memp_->len, MrExt{data.memp_->descr}) {}

DataView::DataView(uintptr_t buf, size_t len, const MrExt &mr) {
  void *descr = mr.mem_desc;
  char *buf_ptr = reinterpret_cast<char *>(buf);
  memp_ = std::make_shared<simm::common::MemBlock>(buf_ptr, len, descr);
}

std::span<char> DataView::AsRef() {
  return {memp_->buf, memp_->len};
}

const std::span<char> DataView::AsRef() const {
  return {memp_->buf, memp_->len};
}

}  // namespace clnt
}  // namespace simm
