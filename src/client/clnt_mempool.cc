#include "clnt_mempool.h"

#include <folly/concurrency/ConcurrentHashMap.h>

#include "transport/ibv_manager.h"

#include "common/base/assert.h"
#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"

DECLARE_LOG_MODULE("simm_client");

namespace simm {
namespace clnt {

static folly::ConcurrentHashMap<std::string, std::unique_ptr<MempoolBase>> mpools;

MempoolBase &MempoolBase::Instance(const std::string &name, const std::string &type) {
  std::string _name = name;
  if (name == "")
    _name = "sicl";

  std::string _type = type;
  if (type == "")
    _type = "cpu";

  std::string key = _name + "." + _type;
  if (auto it = mpools.find(key); it != mpools.end()) {
    return *it->second;
  }

  if (_name == "sicl") {
    auto [it, _] = mpools.emplace(key, std::make_unique<SiclMempool>(_type));
    it->second->Init();
    return *it->second;
  }

  SIMM_ASSERT(false, "Illegal mempool type: {} {}", name.c_str(), type.c_str());
}

SiclMempool::SiclMempool(std::string type) : type_(type) {}

SiclMempool::~SiclMempool() {}

error_code_t SiclMempool::Init() {
  std::call_once(init_flag_, [&]() {
    sicl::transport::IbvDeviceManager::instance(ibv_mgr_);
    SIMM_ASSERT(ibv_mgr_, "Failed to Init IbvDeviceManager! It May No ibv devices found");
    auto ibv_devs = ibv_mgr_->getAllDevices();
    SIMM_ASSERT(!ibv_devs.empty(), "No ibv devices found!");
    int ret = sicl::transport::Mempool::newInstance(mempool_, "ClientMempool", ibv_devs);
    SIMM_ASSERT((ret == 0 && mempool_), "sicl::transport::Mempool::newInstance fail!");
  });

  return CommonErr::OK;
}

error_code_t SiclMempool::Allocate(simm::common::MemBlock *memp) {
  Init();

  sicl::transport::MemDesc *mem_desc = nullptr;
  auto res = mempool_->alloc(mem_desc, memp->len);
  if (res != sicl::transport::Result::SICL_SUCCESS) {
    MLOG_ERROR("failed to allocate buffer: result={}", int(res));
    return ClntErr::ClntMempoolAllocateFailed;
  }
  memp->buf = static_cast<char *>(mem_desc->getAddr());
  memp->descr = mem_desc;

  return CommonErr::OK;
}

error_code_t SiclMempool::Release(simm::common::MemBlock *memp) {
  auto mem_desc = static_cast<sicl::transport::MemDesc *>(memp->descr);
  auto res = mempool_->release(mem_desc);
  if (res != sicl::transport::Result::SICL_SUCCESS) {
    MLOG_ERROR("failed to release buffer: result={}", int(res));
    return ClntErr::ClntMempoolReleaseFailed;
  }
  memp->buf = nullptr;
  memp->len = 0;
  memp->descr = nullptr;
  return CommonErr::OK;
}

error_code_t SiclMempool::ReAlloc([[maybe_unused]] simm::common::MemBlock *memp, [[maybe_unused]] size_t size) {
  return CommonErr::NotImplemented;
}

std::string SiclMempool::ToString() {
  std::stringstream ss;
  ss << "SiclMempool@ " << this << ", type=" << type_ << "]";
  ss << std::endl;
  return ss.str();
}

error_code_t SiclMempool::RegisterMr(simm::common::MemBlock *memp) {
  auto mem_desc = static_cast<sicl::transport::MemDesc *>(memp->descr);
  auto res = mempool_->regMr(mem_desc, memp->buf, memp->len);
  if (res != sicl::transport::Result::SICL_SUCCESS) {
    MLOG_ERROR("failed to register buffer: result={}", int(res));
    return ClntErr::ClntMempoolRegisterMrFailed;
  }
  memp->descr = mem_desc;
  return CommonErr::OK;
}

error_code_t SiclMempool::DeregisterMr(void *descr) {
  auto mem_desc = static_cast<sicl::transport::MemDesc *>(descr);
  auto res = mempool_->deregMr(mem_desc);
  if (res != sicl::transport::Result::SICL_SUCCESS) {
    MLOG_ERROR("failed to deregister buffer: result={}", int(res));
    return ClntErr::ClntMempoolDeregisterMrFailed;
  }
  return CommonErr::OK;
}

}  // namespace clnt
}  // namespace simm
