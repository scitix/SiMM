#pragma once

#include <string>
#include <utility>
#include <vector>

namespace simm {
namespace utils {

typedef std::pair<std::string, std::string> PodNameIp;
typedef std::vector<PodNameIp> PodNameIpVec;

struct TerminatingPodInfo {
  std::string pod_name;
  std::string node_name;
  std::string deletion_ts;
};

std::pair<int, PodNameIpVec> GetPodIpsOfHeadlessService(const std::string &ns,
                                                        const std::string &service_name,
                                                        const std::string &service_port_name);

std::string GetHeadlessServiceNamespace(const std::string ns = "namespace");
std::pair<int, std::string> GetServiceClusterIp(const std::string &ns,
                                                const std::string &service_name,
                                                const std::string &service_port_name);
bool IsNodeLabelKeyExist(const std::string &node_name, const std::string &label_to_check);
std::pair<bool, std::string> SetNodeLabel(const std::vector<std::string> &node_names,
                                          const std::string &key,
                                          const std::string &value,
                                          const std::string &reason);
std::pair<bool, std::string> DeleteNodeLabel(const std::vector<std::string> &node_names,
                                             const std::string &label_name,
                                             const std::string &value,
                                             const std::string &reason);
bool DeletePod(const std::string &pod_name, const std::string &ns = "namespace", bool force = false);
std::string GetTopFreeMemoryNode();
std::vector<std::pair<std::string, size_t>> GetNodesWithLabel(const std::string &label_name);
std::vector<std::string> GetUbiloaderShmNodes(const std::string &ns = "namespace");
std::vector<std::string> GetUbiloaderPendingDataSvcPods(const std::string &ns = "namespace");
std::vector<TerminatingPodInfo> GetUbiloaderTerminatingDataSvcPods(const std::string &ns = "namespace");
std::vector<std::string> GetNotReadyNodes();
// k8s_timestamp format: "2025-02-26T11:35:44Z"
int64_t GetTimeIntervalToNow(const std::string &k8s_timestamp);

}  // namespace utils
}  // namespace simm
