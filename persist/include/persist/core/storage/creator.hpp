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

#ifndef PERSIST_CORE_STORAGE_CREATOR_HPP
#define PERSIST_CORE_STORAGE_CREATOR_HPP

#include <persist/core/config.hpp>

#include <persist/core/storage/base.hpp>
#include <persist/core/storage/file_storage.hpp>
#include <persist/core/storage/memory_storage.hpp>

namespace persist {

/**
 * @brief Supported Backend Storages
 */
enum class StorageType { FILE, MEMORY };
const std::unordered_map<std::string, StorageType> StorageTypeMap = {
    {"file", StorageType::FILE}, {"memory", StorageType::MEMORY}};

/**
 * @brief Method to create backend storage object
 *
 * @param connection_string Constant reference to connection string object.
 *
 * @tparam PageType The type of page stored by the storage.
 */
template <class PageType>
std::unique_ptr<Storage<PageType>>
CreateStorage(const ConnectionString &connection_string) {
  switch (StorageTypeMap.at(connection_string.type)) {
  case StorageType::FILE:
    return std::make_unique<FileStorage<PageType>>(connection_string.path,
                                                   connection_string.page_size);
    break;
  case StorageType::MEMORY:
    return std::make_unique<MemoryStorage<PageType>>(
        connection_string.page_size);
  }
}

/**
 * @brief Method to remove backend storage object
 *
 * @param connection_string Constant referene to connection string object.
 *
 * @tparam PageType The type of page stored by the storage.
 */
template <class PageType>
void RemoveStorage(const ConnectionString &connection_string) {
  std::unique_ptr<Storage<PageType>> storage =
      CreateStorage<PageType>(connection_string);
  storage->Open();
  storage->Remove();
  storage->Close();
}

} // namespace persist

#endif /* PERSIST_CORE_STORAGE_CREATOR_HPP */
