#pragma once

#include <memory>
#include <mutex>

#include "transport/ibv_manager.h"
#include "transport/mempool.h"

#include "common/base/memory.h"
#include "common/errcode/errcode_def.h"

namespace simm {
namespace clnt {

class MempoolBase {
 public:
  virtual ~MempoolBase(){};

  virtual error_code_t Init() = 0;

  virtual error_code_t Allocate(simm::common::MemBlock *memp) = 0;

  virtual error_code_t ReAlloc(simm::common::MemBlock *memp, size_t size) = 0;

  virtual error_code_t Release(simm::common::MemBlock *memp) = 0;

  virtual std::string ToString() { return std::string(""); }

  virtual error_code_t RegisterMr(simm::common::MemBlock *memp) = 0;
  virtual error_code_t DeregisterMr(void *descr) = 0;

  static MempoolBase &Instance(const std::string &name = "", const std::string &type = "");
};

class SiclMempool : public MempoolBase {
 public:
  SiclMempool(std::string type = "");
  ~SiclMempool();

  error_code_t Init() override;
  virtual error_code_t Allocate(simm::common::MemBlock *memp) override;
  virtual error_code_t ReAlloc(simm::common::MemBlock *memp, size_t size) override;
  virtual error_code_t Release(simm::common::MemBlock *memp) override;
  virtual std::string ToString() override;
  virtual error_code_t RegisterMr(simm::common::MemBlock *memp) override;
  virtual error_code_t DeregisterMr(void *descr) override;

 protected:
  static constexpr size_t block_size_ = 1 << 20;  // default 1MB block

  std::string type_{"cpu"};
  std::once_flag init_flag_;

  sicl::transport::IbvDeviceManager *ibv_mgr_{nullptr};
  sicl::transport::Mempool *mempool_{nullptr};
};

}  // namespace clnt
}  // namespace simm
