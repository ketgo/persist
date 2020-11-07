/**
 * collection.hpp - Persist
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

#ifndef COLLECTION_HPP
#define COLLECTION_HPP

#include <memory>

#include <persist/core/defs.hpp>
#include <persist/core/ops_manager.hpp>
#include <persist/core/page_table.hpp>

namespace persist {

/**
 * Collection Base Class
 *
 * The collection base class contains data members and methods common for all
 * types of collections.
 */
template <class RecordManagerType> class Collection {
  PERSIST_PROTECTED
  /**
   * @brief Pointer to backend storage
   */
  std::unique_ptr<Storage> storage;

  /**
   * @brief Collection page table
   */
  PageTable pageTable;

  /**
   * @brief Operations manager
   */
  OpsManager<RecordManagerType> manager;

  /**
   * @brief Flag indicating if the collection is open
   */
  bool opened;

public:
  /**
   * @brief Construct a new Collection object
   *
   * @param connectionString url containing the type of storage backend and its
   * arguments. The url schema is `<type>://<host>/<path>?<args>`. For example a
   * file storage url looks like `file:///myCollection.db` where the backend
   * uses the file `myCollection.db` in the root folder `/` to store data.
   * @param cacheSize the amount of memory in bytes to use for internal cache.
   */
  Collection(std::string connectionString)
      : storage(Storage::create(connectionString)),
        pageTable(*storage, DEFAULT_CACHE_SIZE), manager(pageTable),
        opened(false) {}
  Collection(std::string connectionString, uint64_t cacheSize)
      : storage(Storage::create(connectionString)),
        pageTable(*storage, cacheSize), manager(pageTable), opened(false) {}

  /**
   *Open the collection. This method starts the record manager which in turn
   *sets up the connection with backend storage, e.g. file.
   */
  void open();

  /**
   * Check if the collection is open.
   */
  bool is_open() { return opened; }

  /**
   * Close the collection. This method stops the record manager which in turn
   * tears down the connection with backend storage.
   */
  void close();
};

} // namespace persist

#endif /* COLLECTION_HPP */
