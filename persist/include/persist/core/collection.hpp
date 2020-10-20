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

#include <persist/core/defs.hpp>
#include <persist/core/record_manager.hpp>

namespace persist {

/**
 * Collection Base Class
 *
 * The collection base class contains data members and methods common for all
 * types of collections.
 */
class Collection {
  PERSIST_PROTECTED
  /**
   * @brief Record manager used to persist collection records.
   */
  RecordManager manager;

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
      : manager(connectionString, DEFAULT_CACHE_SIZE), opened(false) {}
  Collection(std::string connectionString, uint64_t cacheSize)
      : manager(connectionString, cacheSize), opened(false) {}

  /**
   *Open the collection. This method starts the record manager which in turn
   *sets up the connection with backend storage, e.g. file.
   */
  void open() {
    if (!opened) {
      manager.start();
      opened = true;
    }
  }

  /**
   * Check if the collection is open.
   */
  bool is_open() { return opened; }

  /**
   * Close the collection. This method stops the record manager which in turn
   * tears down the connection with backend storage.
   */
  void close() {
    if (opened) {
      manager.stop();
      opened = false;
    }
  }
};

} // namespace persist

#endif /* COLLECTION_HPP */
