#pragma once

#include <string.h>
#include <sstream>
#include <string>

namespace simm {
namespace common {

struct MemBlock {
  MemBlock() {}
  MemBlock(size_t _len) : len(_len) {}
  MemBlock(char *_buf, size_t _len, void *_descr = nullptr) : buf(_buf), len(_len), descr(_descr) {}
  MemBlock(size_t _len, const char *_shmname, bool _hugepage = false) : len(_len), hugepage(_hugepage) {
    size_t shmname_sz = _shmname ? strlen(_shmname) : 0;
    if (shmname_sz) {
      shmname = _shmname;
    }
  }
  ~MemBlock() {}

  std::string ToString() {
    std::ostringstream oss;
    oss << "MemBlock{"
        << "buf: " << (void *)buf << ", "
        << "len: " << len << ", "
        << "descr: " << descr << ", "
        << "shmname: " << (shmname.empty() ? "none" : shmname) << ","
        << "}";
    return oss.str();
  }

  char *buf = nullptr;
  size_t len = 0;
  std::string shmname;
  void *descr = nullptr;
  bool hugepage = false;
};

}  // namespace common
}  // namespace simm