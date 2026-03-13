#include "common/hashkit/hashkit.h"

#include "common/base/assert.h"
#include "common/hashkit/murmurhash.h"

namespace simm {
namespace hashkit {
HashkitBase& HashkitBase::Instance(HashkitType type) {
    if (type == HashkitType::MURMUR) {
        return HashkitMurmur::Instance();
    } else {
        SIMM_ASSERT(false, "Illegal hashkit type");
    }
}

}  // namespace hashkit
}  // namespace simm
