/**
 * storage/base.cpp - Persist
 *
 * Copyright 2020 Ketan Goyal
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

/**
 * @brief Contains factory method implementation.
 */

#include <persist/core/storage/base.hpp>
#include <persist/core/storage/file_storage.hpp>
#include <persist/core/storage/memory_storage.hpp>

#include <regex>

namespace persist {

/**
 * @brief Supported Backend Storages
 */
enum class StorageType { FILE, MEMORY };
const std::unordered_map<std::string, StorageType> StorageTypeMap = {
    {"file", StorageType::FILE}, {"memory", StorageType::MEMORY}};

/**
 * Connection String Class
 *
 * The class parsers a given connection string and exposes the different
 * arguments needed to construct a backens storage object. It assumes that the
 * string has the following schema:
 *
 *      <type>://<host>/<path>?<arg_1=val_1&arg_2=val_2>
 *
 * where
 * - type [required]: the type of backens storage
 * - host [optional]: hostname where the storage should be stored
 * - path [required]: path on host where the storage is located
 * - arg_1..n [optional]: additional argument names
 * - val_1..n [optional]: values of associated with the additional arguments
 *
 * NOTE: Currently simple parser is implemented which just detects the `type`.
 *
 * TODO: Prase arguments like `pageSize`.
 */
class ConnectionString {
  PERSIST_PRIVATE 

public:  
  std::string type;
  std::string path;
  std::string args;

  // Constructor
  ConnectionString(std::string connectionString) 
  : type{}, path{}, args{} {
  
    std::regex test_rx{"(\\w+)(?:\\:\\/{2})([\\/A-z=0-9\\.]+)(?:\\?)([&A-z=0-9]+)+"};
    std::smatch sm{}; 
    std::regex_match(connectionString, sm, test_rx);
    type = sm.str(1);
    path = sm.str(2);
    args = sm.str(3);
  }
};

std::unique_ptr<Storage> Storage::create(std::string connectionString) {
  ConnectionString _connectionString(connectionString);
  

  switch (StorageTypeMap.at(_connectionString.type)) {
  case StorageType::FILE:
    return std::make_unique<FileStorage>(_connectionString.path);
    break;
  case StorageType::MEMORY:
    return std::make_unique<MemoryStorage>();
  }
}

} // namespace persist
