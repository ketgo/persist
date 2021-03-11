/**
 * storage/factory.hpp - Persist
 *
 * Copyright 2021 Ketan Goyal
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

#ifndef PERSIST_CORE_STORAGE_FACTORY_HPP
#define PERSIST_CORE_STORAGE_FACTORY_HPP

#include <persist/core/storage/base.hpp>
#include <persist/core/storage/file_storage.hpp>
#include <persist/core/storage/memory_storage.hpp>

/**
 * Storage type seperator in connection string
 */
#define STORAGE_TYPE_SEPERATOR "://"

namespace persist {

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
 * TODO:
 *  - Prase arguments like `pageSize`.
 *  - Exception for incorrectly formated connection string.
 */
class ConnectionString {
public:
  std::string raw;
  std::string type;
  std::string path;

  // Constructor
  ConnectionString(const std::string &connectionString)
      : raw(connectionString) {
    std::string seperator = STORAGE_TYPE_SEPERATOR;
    std::string::size_type loc = raw.find(seperator);
    type = raw.substr(0, loc);
    path = raw.substr(loc + seperator.size());
  }
};

/************************************************************************/

/**
 * @brief Supported Backend Storages
 */
enum class StorageType { FILE, MEMORY };
const std::unordered_map<std::string, StorageType> StorageTypeMap = {
    {"file", StorageType::FILE}, {"memory", StorageType::MEMORY}};

/**
 * @brief Factory method to create backend storage object
 *
 * @param connectionString url containing the type of storage backend and its
 * arguments. The url schema is `<type>://<host>/<path>?<args>`. For example a
 * file storage url looks like `file:///myCollection.db` where the backend
 * uses the file `myCollection.db` in the root folder `/` to store data.
 */
static std::unique_ptr<Storage> CreateStorage(std::string connectionString) {
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
#endif /* PERSIST_CORE_STORAGE_FACTORY_HPP */
