#include "common/utils/ip_util.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <utility>

#include "common/base/assert.h"
#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"

namespace simm {
namespace utils {

std::string GetHostIp() {
  char hostname[256] = {0};
  int ret = gethostname(hostname, 256);
  SIMM_ASSERT(ret == 0, "Fail to gethostname");
  auto hostip = GetIpbyDomain(hostname);
  SIMM_ASSERT(hostip.size() >= 1, "Fail to GetHostIp");
  if (hostip.size() > 1) {
    LOG_WARN("Get {} hostip, use hostip[0] = {}", hostip.size(), hostip[0]);
  }
  return hostip[0];
}

std::vector<std::string> GetIpbyDomain(const char *domain) {
  struct hostent *_hostent = nullptr;
  _hostent = gethostbyname(domain);
  std::vector<std::string> ip_vec;
  if (nullptr != _hostent) {
    int i = 0;
    while (nullptr != _hostent->h_addr_list[i]) {
      char *ipaddr = inet_ntoa(*((struct in_addr *)_hostent->h_addr_list[i]));
      if (nullptr != ipaddr) {
        std::string ipaddr_str = ipaddr;
        ip_vec.push_back(ipaddr_str);
      } else {
        LOG_ERROR("The return addr of inet_ntoa is nullptr");
      }
      i++;
    }
  }
  return ip_vec;
}

std::pair<int, std::vector<std::string>> GetIpAndPortbyDomain(const char *domain, uint32_t port, bool nolog) {
  std::vector<std::string> ip_port_vec;
  struct addrinfo hints, *res;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;      // Allow both IPv4 and IPv6
  hints.ai_socktype = SOCK_STREAM;  // TCP socket
  std::string port_str = std::to_string(port);
  int result = 0;
  result = getaddrinfo(domain, port_str.c_str(), &hints, &res);
  if (result != 0) {
    if (!nolog) {
      LOG_ERROR("getaddrinfo for {} failed: {}", domain, gai_strerror(result));
    }
    return std::make_pair(result, ip_port_vec);
  }

  for (struct addrinfo *p = res; p != nullptr; p = p->ai_next) {
    void *addr;
    std::string ip;
    int resolved_port = 0;
    if (p->ai_family == AF_INET) {
      struct sockaddr_in *ipv4 = reinterpret_cast<struct sockaddr_in *>(p->ai_addr);
      addr = &(ipv4->sin_addr);
      resolved_port = ntohs(ipv4->sin_port);
    } else {
      struct sockaddr_in6 *ipv6 = reinterpret_cast<struct sockaddr_in6 *>(p->ai_addr);
      addr = &(ipv6->sin6_addr);
      resolved_port = ntohs(ipv6->sin6_port);
    }

    char ip_buffer[INET6_ADDRSTRLEN];
    inet_ntop(p->ai_family, addr, ip_buffer, sizeof(ip_buffer));
    ip = ip_buffer;
    std::string ip_port = ip + ":" + std::to_string(resolved_port);
    ip_port_vec.push_back(ip_port);
  }
  return std::make_pair(result, ip_port_vec);
}

bool IsValidV4IPAddr(const std::string & ip4_addr_str) {
    struct sockaddr_in sa;
    // return code 1 means convert succeed
    return inet_pton(AF_INET, ip4_addr_str.c_str(), &(sa.sin_addr)) == 1;
}

bool IsValidV6IPAddr(const std::string & ip6_addr_str) {
    struct in6_addr addr6;
    // return code 1 means convert succeed
    return inet_pton(AF_INET6, ip6_addr_str.c_str(), &addr6) == 1;
}

bool IsValidPortNum(int port_num) {
  return port_num > 0 && port_num <= 65535;
}

}  // namespace utils
}  // namespace simm
