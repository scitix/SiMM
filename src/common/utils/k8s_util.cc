#include "common/utils/k8s_util.h"

#include <curl/curl.h>
#include <gflags/gflags.h>
#include <openssl/sha.h>

#include <algorithm>
#include <cstddef>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/errcode/errcode_def.h"
#include "common/logging/logging.h"
#include "common/utils/ip_util.h"
#include "common/utils/json.hpp"
#include "common/utils/string_util.h"
#include "common/utils/time_util.h"

DECLARE_string(k8s_api_server);
DECLARE_string(k8s_service_account);
DECLARE_int32(cm_rpc_inter_port);

namespace simm {
namespace utils {

size_t CurlWriteCallback(void *contents, size_t size, size_t nmemb, std::string *output) {
  size_t total_size = size * nmemb;
  output->append((char *)contents, total_size);
  return total_size;
}

std::string ReadStringFromFile(const std::string &file_path) {
  std::ifstream file(file_path);
  if (!file) {
    LOG_ERROR("Failed to open {}: {}", file_path.c_str(), ERR_STR());
    return "";
  }
  std::string output;
  std::getline(file, output);
  return output;
}

void CurlDebugCallback(CURL *, curl_infotype curl_infotype, char *data, [[maybe_unused]] size_t size, void *) {
  if (curl_infotype == CURLINFO_TEXT) {
    std::cout << "curl command: " << data << std::endl;
    LOG_WARN("curl command: {}", data);
  }
}

std::pair<int, PodNameIpVec> GetPodIpsOfHeadlessService(const std::string &ns,
                                                        const std::string &service_name,
                                                        const std::string &service_port_name) {
  std::string k8s_api_server = FLAGS_k8s_api_server;
  std::string k8s_service_account = FLAGS_k8s_service_account;
  std::string ca_cert_file = k8s_service_account + "/ca.crt";
  std::string token_file = k8s_service_account + "/token";
  std::string endpoint_api_url = k8s_api_server + "/api/v1/namespaces/" + ns + "/endpoints/" + service_name;
  int ret = 0;
  std::string port_number_str;
  PodNameIpVec pod_name_ip_vec;
  CURL *curl = curl_easy_init();
  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, endpoint_api_url.c_str());
    curl_easy_setopt(curl, CURLOPT_CAINFO, ca_cert_file.c_str());
    struct curl_slist *headers = nullptr;
    std::string bearer_token = ReadStringFromFile(token_file);
    if (bearer_token.empty()) {
      LOG_ERROR("k8s token file {} not found", token_file);
      ret = -1;
      return std::make_pair(ret, pod_name_ip_vec);
    }
    std::string bearer_string = "Authorization: Bearer " + bearer_token;
    headers = curl_slist_append(headers, bearer_string.c_str());
    headers = curl_slist_append(headers, " Accept: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
    // curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);

    std::string response_string;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CurlWriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
    // curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, CurlDebugCallback);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
      LOG_ERROR("curl_easy_perform failed: {}", curl_easy_strerror(res));
      ret = -1;
    } else {
      nlohmann::json endpoint_response = nlohmann::json::parse(response_string);
      if (endpoint_response.contains("subsets") && endpoint_response["subsets"].size() > 0) {
        if (endpoint_response["subsets"].size() > 1) {
          LOG_WARN("Expect 1 subset, actually {}", endpoint_response["subsets"].size());
        }
        const auto &subset = endpoint_response["subsets"].at(0);
        if (subset.contains("ports")) {
          for (const auto &port : subset["ports"]) {
            std::string port_name = port.contains("name") ? port["name"] : "";
            if (port_name == service_port_name) {
              int port_number = 0;
              if (port.contains("port")) {
                port_number = port["port"];
              } else {
                continue;
              }
              port_number_str = std::to_string(port_number);
              break;
            }
          }
        }
        if (port_number_str.empty()) {
          LOG_WARN("Failed to Get port_number of {}, set it to default FLAGS_cm_rpc_inter_port", service_port_name);
          port_number_str = std::to_string(FLAGS_cm_rpc_inter_port);
        }
        if (subset.contains("addresses")) {
          for (const auto &address : subset["addresses"]) {
            if (address.contains("ip") && address.contains("targetRef") && address["targetRef"].contains("name")) {
              std::string pod_name = address["targetRef"]["name"];
              std::string pod_ip = address["ip"];
              pod_ip += ":" + port_number_str;
              pod_name_ip_vec.push_back(std::make_pair(pod_name, pod_ip));
            }
          }
        }
      } else {
        LOG_ERROR("Invalid response format: {}", response_string);
        ret = -1;
      }
    }
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }
  if (pod_name_ip_vec.empty()) {
    ret = -1;
  }
  return std::make_pair(ret, pod_name_ip_vec);
}

std::string GetHeadlessServiceNamespace(const std::string ns) {
  // test whether namespace from env or "namespace1" or "namespace2" namespace
  // is effective
  std::string ret = ns;
  std::string service_domain_name = "simm-master-svc." + ns;
  int val_ret = 0;
  std::vector<std::string> vec_ret;
  bool timeout = false;
  std::chrono::time_point<std::chrono::system_clock> start_time = std::chrono::system_clock::now();
  std::chrono::milliseconds timeout_ms(2000);
  do {
    std::tie(val_ret, vec_ret) = GetIpAndPortbyDomain(service_domain_name.c_str(), 35026 + 2, true);
    timeout = hasTimeoutOccurred(start_time, timeout_ms);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  } while (val_ret != 0 && !timeout);
  if (val_ret != 0) {  // "namespace1" is not alive
    if (ns != "namespace1") {
      service_domain_name = "service";
      start_time = std::chrono::system_clock::now();
      do {
        std::tie(val_ret, vec_ret) = GetIpAndPortbyDomain(service_domain_name.c_str(), 35026 + 2, true);
        timeout = hasTimeoutOccurred(start_time, timeout_ms);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
      } while (val_ret != 0 && !timeout);
      if (val_ret == 0)
        ret = "namespace1";
    }
    if (val_ret != 0) {
      service_domain_name = "service";
      start_time = std::chrono::system_clock::now();
      do {
        std::tie(val_ret, vec_ret) = GetIpAndPortbyDomain(service_domain_name.c_str(), 35026 + 2, true);
        timeout = hasTimeoutOccurred(start_time, timeout_ms);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
      } while (val_ret != 0 && !timeout);
      if (val_ret == 0)
        ret = "namespace2";
    }
  }
  return ret;
}

std::pair<int, std::string> GetServiceClusterIp(const std::string &ns,
                                                const std::string &service_name,
                                                const std::string &service_port_name) {
  std::string k8s_api_server = FLAGS_k8s_api_server;
  std::string k8s_service_account = FLAGS_k8s_service_account;
  std::string ca_cert_file = k8s_service_account + "/ca.crt";
  std::string token_file = k8s_service_account + "/token";
  std::string endpoint_api_url = k8s_api_server + "/api/v1/namespaces/" + ns + "/services/" + service_name;
  int ret = 0;
  std::string cluster_ip = "";
  std::string port_number_str = "";
  CURL *curl = curl_easy_init();
  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, endpoint_api_url.c_str());
    curl_easy_setopt(curl, CURLOPT_CAINFO, ca_cert_file.c_str());
    struct curl_slist *headers = nullptr;
    std::string bearer_token = ReadStringFromFile(token_file);
    std::string bearer_string = "Authorization: Bearer " + bearer_token;
    headers = curl_slist_append(headers, bearer_string.c_str());
    headers = curl_slist_append(headers, " Accept: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    std::string response_string;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CurlWriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
    // curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, CurlDebugCallback);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
      LOG_ERROR("curl_easy_perform failed: {}", curl_easy_strerror(res));
      ret = -1;
    } else {
      nlohmann::json response = nlohmann::json::parse(response_string);
      if (response.contains("spec")) {
        if (response["spec"].contains("ports")) {
          for (auto &port : response["spec"]["ports"]) {
            std::string port_name = port.contains("name") ? port["name"] : "";
            if (port_name == service_port_name) {
              int port_number = 0;
              if (port.contains("port")) {
                port_number = port["port"];
              } else {
                continue;
              }
              port_number_str = std::to_string(port_number);
              break;
            }
          }
        }
        if (port_number_str.empty()) {
          LOG_WARN("Failed to Get port_number of {} for service {}", service_port_name, service_name);
          ret = -1;
        }
        if (response["spec"].contains("clusterIP")) {
          cluster_ip = response["spec"]["clusterIP"];
          cluster_ip += ":" + port_number_str;
        } else {
          LOG_ERROR("Failed to Get ip for service {}", service_name);
          ret = -1;
        }
      } else {
        ret = -1;
      }
    }
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }
  return std::make_pair(ret, cluster_ip);
}

bool IsNodeLabelKeyExist(const std::string &node_name, const std::string &lable_to_check) {
  std::string k8s_api_server = FLAGS_k8s_api_server;
  std::string k8s_service_account = FLAGS_k8s_service_account;
  std::string ca_cert_file = k8s_service_account + "/ca.crt";
  std::string token_file = k8s_service_account + "/token";
  std::string endpoint_api_url = k8s_api_server + "/api/v1/nodes/" + node_name;
  bool ret = false;
  CURL *curl = curl_easy_init();
  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, endpoint_api_url.c_str());
    curl_easy_setopt(curl, CURLOPT_CAINFO, ca_cert_file.c_str());
    struct curl_slist *headers = nullptr;
    std::string bearer_token = ReadStringFromFile(token_file);
    std::string bearer_string = "Authorization: Bearer " + bearer_token;
    headers = curl_slist_append(headers, bearer_string.c_str());
    headers = curl_slist_append(headers, " Accept: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    std::string response_string;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CurlWriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
    // curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, CurlDebugCallback);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
      LOG_ERROR("curl_easy_perform failed: {}", curl_easy_strerror(res));
      ret = false;
    } else {
      nlohmann::json node_data = nlohmann::json::parse(response_string);
      if (node_data.contains("metadata") && node_data["metadata"].contains("labels")) {
        const auto &lables = node_data["metadata"]["labels"];
        if (lables.contains(lable_to_check)) {
          ret = true;
        } else {
          ret = false;
        }
      } else {
        ret = false;
      }
    }
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }
  return ret;
}

std::vector<std::pair<std::string, size_t>> GetNodesWithLabel(const std::string &label_name) {
  std::string k8s_api_server = FLAGS_k8s_api_server;
  std::string k8s_service_account = FLAGS_k8s_service_account;
  std::string ca_cert_file = k8s_service_account + "/ca.crt";
  std::string token_file = k8s_service_account + "/token";
  std::string endpoint_api_url = k8s_api_server + "/api/v1/nodes/";
  std::vector<std::pair<std::string, size_t>> ret_nodes = {};
  CURL *curl = curl_easy_init();
  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, endpoint_api_url.c_str());
    curl_easy_setopt(curl, CURLOPT_CAINFO, ca_cert_file.c_str());
    struct curl_slist *headers = nullptr;
    std::string bearer_token = ReadStringFromFile(token_file);
    std::string bearer_string = "Authorization: Bearer " + bearer_token;
    headers = curl_slist_append(headers, bearer_string.c_str());
    headers = curl_slist_append(headers, " Accept: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    std::string response_string;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CurlWriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
    // curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, CurlDebugCallback);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
      LOG_ERROR("curl_easy_perform failed: {}", curl_easy_strerror(res));
    } else {
      nlohmann::json nodes = nlohmann::json::parse(response_string);
      if (nodes.contains("items")) {
        for (auto &node : nodes["items"]) {
          if (node.contains("metadata") && node["metadata"].contains("labels") &&
              (node["metadata"]["labels"].contains(label_name))) {
            std::string node_name = node["metadata"]["name"];
            size_t mem_free = 0;
            if (node.contains("status") && node["status"].contains("allocatable") &&
                node["status"]["allocatable"].contains("memory")) {
              std::string mem_free_str = node["status"]["allocatable"]["memory"].get<std::string>();
              std::string::size_type pos = mem_free_str.find_first_not_of("0123456789");
              if (pos != std::string::npos) {
                mem_free = std::stoul(mem_free_str.substr(0, pos));
                std::string mem_unit = mem_free_str.substr(pos);
                if (mem_unit == "Ki") {
                  mem_free *= 1024;
                } else if (mem_unit == "Mi") {
                  mem_free *= 1024 * 1024;
                } else if (mem_unit == "Gi") {
                  mem_free *= 1024 * 1024 * 1024;
                }
              } else {
                mem_free = std::stoul(mem_free_str);
              }
            }
            ret_nodes.push_back({node_name, mem_free});
          }
        }
      }
    }
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }
  return ret_nodes;
}

std::vector<std::string> GetsimmShmNodes(const std::string &ns) {
  std::string k8s_api_server = FLAGS_k8s_api_server;
  std::string k8s_service_account = FLAGS_k8s_service_account;
  std::string ca_cert_file = k8s_service_account + "/ca.crt";
  std::string token_file = k8s_service_account + "/token";
  std::string pod_label = "simm-data-svc-shm";
  std::string endpoint_api_url = k8s_api_server + "/api/v1/namespaces/" + ns + "/pods?labelSelector=svc%3D" + pod_label;
  std::vector<std::string> ret = {};
  CURL *curl = curl_easy_init();
  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, endpoint_api_url.c_str());
    curl_easy_setopt(curl, CURLOPT_CAINFO, ca_cert_file.c_str());
    struct curl_slist *headers = nullptr;
    std::string bearer_token = ReadStringFromFile(token_file);
    std::string bearer_string = "Authorization: Bearer " + bearer_token;
    headers = curl_slist_append(headers, bearer_string.c_str());
    headers = curl_slist_append(headers, " Accept: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    std::string response_string;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CurlWriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
    // curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, CurlDebugCallback);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
      LOG_ERROR("curl_easy_perform failed: {}", curl_easy_strerror(res));
    } else {
      nlohmann::json pods = nlohmann::json::parse(response_string);
      if (pods.contains("items")) {
        for (auto &pod : pods["items"]) {
          if (pod.contains("spec") && pod["spec"].contains("nodeName")) {
            std::string node_name = pod["spec"]["nodeName"];
            ret.push_back(node_name);
          }
        }
      }
    }
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }

  return ret;
}

// k8s_timestamp format: "2025-02-26T11:35:44Z"
int64_t GetTimeIntervalToNow(const std::string &k8s_timestamp) {
  auto now = std::chrono::system_clock::now();

  std::istringstream ss(k8s_timestamp);
  std::tm k8stime = {};
  ss >> std::get_time(&k8stime, "%Y-%m-%dT%H:%M:%SZ");

  std::time_t k8stime_t = std::mktime(&k8stime);
  auto k8s_time_point = std::chrono::system_clock::from_time_t(k8stime_t);

  auto duration = now - k8s_time_point;
  auto duration_minutes = std::chrono::duration_cast<std::chrono::minutes>(duration).count();

  return duration_minutes;
}

std::vector<std::string> GetsimmPendingDataSvcPods(const std::string &ns) {
  std::string k8s_api_server = FLAGS_k8s_api_server;
  std::string k8s_service_account = FLAGS_k8s_service_account;
  std::string ca_cert_file = k8s_service_account + "/ca.crt";
  std::string token_file = k8s_service_account + "/token";
  std::string pod_label = "simm-data-svc";
  std::string endpoint_api_url = k8s_api_server + "/api/v1/namespaces/" + ns + "/pods?labelSelector=app=" + pod_label;
  std::vector<std::string> ret = {};
  CURL *curl = curl_easy_init();
  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, endpoint_api_url.c_str());
    curl_easy_setopt(curl, CURLOPT_CAINFO, ca_cert_file.c_str());
    struct curl_slist *headers = nullptr;
    std::string bearer_token = ReadStringFromFile(token_file);
    std::string bearer_string = "Authorization: Bearer " + bearer_token;
    headers = curl_slist_append(headers, bearer_string.c_str());
    headers = curl_slist_append(headers, " Accept: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    std::string response_string;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CurlWriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
    // curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, CurlDebugCallback);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
      LOG_ERROR("curl_easy_perform failed: {}", curl_easy_strerror(res));
    } else {
      nlohmann::json pods = nlohmann::json::parse(response_string);
      if (pods.contains("items")) {
        for (const auto &pod : pods["items"]) {
          std::string pod_status = pod["status"]["phase"];
          if (pod_status == "Pending") {
            if (pod["metadata"].contains("deletionTimestamp")) {
              continue;
            }
            std::string pod_name = pod["metadata"]["name"];
            std::string creation_ts = pod["metadata"]["creationTimestamp"];
            int64_t time_last = GetTimeIntervalToNow(creation_ts);
            // Pending pod and ContainerCreating pod are both with Pending
            // pod_status, check if the pod is created more than 5 minutes ago
            LOG_INFO(
                "Found Pending or ContainerCreating pod {} lasts {} seconds, "
                "creation timestamp: {}",
                pod_name,
                time_last,
                creation_ts);
            // TODO: exception handling for the case that the pod is
            // created less than 5 minutes ago and the pod is still in
            // ContainerCreating status
            if (time_last > 5) {
              ret.push_back(pod_name);
            }
          }
        }
      }
    }
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }

  return ret;
}

std::vector<TerminatingPodInfo> GetsimmTerminatingDataSvcPods(const std::string &ns) {
  std::string k8s_api_server = FLAGS_k8s_api_server;
  std::string k8s_service_account = FLAGS_k8s_service_account;
  std::string ca_cert_file = k8s_service_account + "/ca.crt";
  std::string token_file = k8s_service_account + "/token";
  std::string pod_label = "simm-data-svc";
  std::string endpoint_api_url = k8s_api_server + "/api/v1/namespaces/" + ns + "/pods?labelSelector=app=" + pod_label;
  std::vector<TerminatingPodInfo> ret = {};
  CURL *curl = curl_easy_init();
  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, endpoint_api_url.c_str());
    curl_easy_setopt(curl, CURLOPT_CAINFO, ca_cert_file.c_str());
    struct curl_slist *headers = nullptr;
    std::string bearer_token = ReadStringFromFile(token_file);
    std::string bearer_string = "Authorization: Bearer " + bearer_token;
    headers = curl_slist_append(headers, bearer_string.c_str());
    headers = curl_slist_append(headers, " Accept: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    std::string response_string;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CurlWriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
    // curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, CurlDebugCallback);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
      LOG_ERROR("curl_easy_perform failed: {}", curl_easy_strerror(res));
    } else {
      nlohmann::json pods = nlohmann::json::parse(response_string);
      if (pods.contains("items")) {
        for (const auto &pod : pods["items"]) {
          std::string pod_status = pod["status"]["phase"];
          if (pod["metadata"].contains("deletionTimestamp") && pod["spec"].contains("nodeName")) {
            // for terminating pods, pod_status may be Pending or Running, but
            // deletionTimestamp is set.
            std::string pod_name = pod["metadata"]["name"];
            std::string node_name = pod["spec"]["nodeName"];
            std::string deletion_ts = pod["metadata"]["deletionTimestamp"];
            int64_t time_last = GetTimeIntervalToNow(deletion_ts);
            LOG_INFO(
                "Found Terminating pod %s on node %s lasts %ld seconds, with "
                "pod_status = %s, deletion timestamp: %s",
                pod_name.c_str(),
                node_name.c_str(),
                time_last,
                pod_status.c_str(),
                deletion_ts.c_str());
            if (time_last > 2) {
              ret.push_back({pod_name, node_name, deletion_ts});
            }
          }
        }
      }
    }
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }

  return ret;
}

std::vector<std::string> GetNotReadyNodes() {
  std::string k8s_api_server = FLAGS_k8s_api_server;
  std::string k8s_service_account = FLAGS_k8s_service_account;
  std::string ca_cert_file = k8s_service_account + "/ca.crt";
  std::string token_file = k8s_service_account + "/token";
  std::string pod_label = "simm-data-svc";
  std::string endpoint_api_url = k8s_api_server + "/api/v1/nodes";
  std::vector<std::string> ret = {};
  CURL *curl = curl_easy_init();
  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, endpoint_api_url.c_str());
    curl_easy_setopt(curl, CURLOPT_CAINFO, ca_cert_file.c_str());
    struct curl_slist *headers = nullptr;
    std::string bearer_token = ReadStringFromFile(token_file);
    std::string bearer_string = "Authorization: Bearer " + bearer_token;
    headers = curl_slist_append(headers, bearer_string.c_str());
    headers = curl_slist_append(headers, " Accept: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    std::string response_string;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CurlWriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
    // curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, CurlDebugCallback);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
      LOG_ERROR("curl_easy_perform failed: {}", curl_easy_strerror(res));
    } else {
      nlohmann::json data = nlohmann::json::parse(response_string);
      if (data.contains("items")) {
        for (auto &node : data["items"]) {
          if (!node.contains("metadata") || !node["metadata"].contains("name") || !node.contains("status") ||
              !node["status"].contains("conditions")) {
            continue;
          }
          for (auto &condition : node["status"]["conditions"]) {
            if (condition.contains("type") && condition["type"] == "Ready" && condition.contains("status") &&
                condition["status"] == "Unknown") {  // != "True")
              std::string node_name = node["metadata"]["name"];
              ret.push_back(node_name);
            }
          }
        }
      }
    }
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }

  return ret;
}

}  // namespace utils
}  // namespace simm
