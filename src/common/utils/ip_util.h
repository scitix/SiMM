#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace simm {
namespace utils {

std::string GetHostIp();
std::vector<std::string> GetIpbyDomain(const char *domain);
std::pair<int, std::vector<std::string>> GetIpAndPortbyDomain(const char *domain,
                                                              uint32_t port = 0,
                                                              bool nolog = false);
bool IsValidV4IPAddr(const std::string & ip4_addr_str);
bool IsValidV6IPAddr(const std::string & ip6_addr_str);
bool IsValidPortNum(int port_num);

}  // namespace utils
}  // namespace simm
