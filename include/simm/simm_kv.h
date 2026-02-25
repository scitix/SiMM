/*
 * Copyright (c) 2026 Scitix Tech PTE. LTD.
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

#include <functional>
#include <string>
#include <vector>

namespace simm {
namespace clnt {

class Data;
class DataView;

using AsyncCallback = std::function<void(int)>;

class KVStore {
 public:
  KVStore();
  virtual ~KVStore() {};

  /*
   * Allocate a Data block of given size from SiMM client mempool
   * @param block_size : size of Data block to be allocated
   * @return Data : structure of the allocated block
   */
  Data Allocate(size_t block_size) const;

  /*
   * Get one key's value from SiMM KV store, blocking call
   * @param key : target key to get
   * @param get_data : DataView structure to hold the value
   * @return value length(should be >=0) : non-zero(error code) if failed, error codes definition refer to errcode_def.h
   */
  int32_t Get(const std::string &key, DataView get_data);

  /*
   * Put one key & value into SiMM KV store, blocking call
   * @param key : target key to put
   * @param put_data : DataView structure to hold the value
   * @return errorcode : 0 if success, non-zero if failed
   */
  int16_t Put(const std::string &key, DataView put_data);

  /*
   * Lookup to check if target kv exists in SiMM KV store, blocking call
   * @param key : target key to lookup
   * @return errorcode : 0 if success, non-zero if failed
   */
  int16_t Exists(const std::string &key);

  /*
   * Delete one key & value from SiMM KV store, blocking call
   * @param key : target key to delete
   * @return errorcode : 0 if success, non-zero if failed
   */
  int16_t Delete(const std::string &key);

  /*
   * Get multiple keys' values from SiMM KV store in batch mode, blocking call
   * @param keys : target keys to get
   * @param datas : DataView structures to hold the values
   * @return value lengths vector(should be >=0) : non-zero(error code) if failed
   */
  std::vector<int32_t> MGet(const std::vector<std::string> &keys, std::vector<DataView> datas);

  /*
   * Put multiple keys' values into SiMM KV store in batch mode, blocking call
   * @param keys : target keys to put
   * @param datas : DataView structures to hold the values
   * @return errorcodes vector : 0 if success, non-zero if failed
   */
  std::vector<int16_t> MPut(const std::vector<std::string> &keys, std::vector<DataView> datas);

  /*
   * Lookup multiple keys from SiMM KV store in batch mode, blocking call
   * @param keys : target keys to put
   * @return errorcodes vector : 0 if success, non-zero if failed
   */
  std::vector<int16_t> MExists(const std::vector<std::string> &keys);

  /*
   * Get one key's value from SiMM KV store, non-blocking call
   * @param key : target key to get
   * @param get_data : DataView structure to hold the value
   * @param callback : user defined callback function when async operation is done
   * @return value length(should be >=0) : non-zero(error code) if failed
   */
  int16_t AsyncGet(const std::string &key, DataView get_data, AsyncCallback callback);

  /*
   * Put one key & value into SiMM KV store, non-blocking call
   * @param key : target key to put
   * @param put_data : DataView structure to hold the value
   * @param callback : user defined callback function when async operation is done
   * @return errorcode : 0 if success, non-zero if failed
   */
  int16_t AsyncPut(const std::string &key, DataView put_data, AsyncCallback callback);

  /*
   * Lookup to check if target kv exists in SiMM KV store, non-blocking call
   * @param key : target key to lookup
   * @param callback : user defined callback function when async operation is done
   * @return errorcode : 0 if success, non-zero if failed
   */
  int16_t AsyncExists(const std::string &key, AsyncCallback callback);

  /*
   * Delete one key & value from SiMM KV store, non-blocking call
   * @param key : target key to delete
   * @param callback : user defined callback function when async operation is done
   * @return errorcode : 0 if success, non-zero if failed
   */
  int16_t AsyncDelete(const std::string &key, AsyncCallback callback);

  /*
   * Put multiple keys' values into SiMM KV store in batch mode, non-blocking call
   * @param keys : target keys to put
   * @param datas : DataView structures to hold the values
   * @param callback : user defined callback function when async operation is done
   * @return errorcodes vector : 0 if success, non-zero if failed
   */
  std::vector<int32_t> AsyncMGet(const std::vector<std::string> &keys,
                                 std::vector<DataView> datas,
                                 AsyncCallback callback);

  /*
   * Put multiple keys' values into SiMM KV store in batch mode, non-blocking call
   * @param keys : target keys to put
   * @param datas : DataView structures to hold the values
   * @param callback : user defined callback function when async operation is done
   * @return errorcodes vector : 0 if success, non-zero if failed
   */
  std::vector<int16_t> AsyncMPut(const std::vector<std::string> &keys,
                                 std::vector<DataView> datas,
                                 AsyncCallback callback);
  /*
   * Lookup multiple keys from SiMM KV store in batch mode, non-blocking call
   * @param keys : target keys to put
   * @param callback : user defined callback function when async operation is done
   * @return errorcodes vector : 0 if success, non-zero if failed
   */
  std::vector<int16_t> AsyncMExists(const std::vector<std::string> &keys, AsyncCallback callback);
};

}  // namespace clnt
}  // namespace simm
