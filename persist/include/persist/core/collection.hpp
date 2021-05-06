/**
 * collection.hpp - Persist
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

#ifndef PERSIST__CORE__COLLECTION__COLLECTION_HPP
#define PERSIST__CORE__COLLECTION__COLLECTION_HPP

#include <memory>

#include <persist/core/metadata_manager.hpp>
#include <persist/core/page/record_page/page.hpp>
#include <persist/core/page_manager.hpp>

/**
 * @brief Collection base class containing boiler plate code.
 *
 * @tparam ReplacerType The type of page replacer to be used by buffer manager.
 * Default set to LRUReplacer.
 * @tparam FreeSpaceManagerType The type of free space manager. Default set to
 * FSLManager.
 */
template <class ReplacerType, class FreeSpaceManagerType,
          class RecordManagerType>
class Collection {
  PERSIST_PROTECTED

  /**
   * @brief Unique pointer to data storage.
   *
   */
  std::unique_ptr<Storage<RecordPage>> storage;

  /**
   * @brief Buffer manager.
   *
   */
  BufferManager<RecordPage, ReplacerType> buffer_manager;

  /**
   * @brief Free space manager.
   *
   */
  FreeSpaceManagerType fsm;

  /**
   * @brief Page manager
   *
   */
  PageManager<RecordPage, ReplacerType, FreeSpaceManagerType> page_manager;

  /**
   * @brief Metadata manager
   *
   */
  MetadataManager<ReplacerType, FreeSpaceManagerType> metadata_manager;

  /**
   * @brief Record manager
   *
   */
  RecordManagerType record_manager;

  /**
   * @brief Flag indicating collection is openned.
   *
   */
  bool openned;

public:
  /**
   * @brief Construct a new Collection object
   *
   * @param connection_string Url containing the type of storage backend and its
   * arguments. The url schema is `<type>://<host>/<path>?<args>`. For example a
   * file storage url looks like `file:///myCollection.db` where the backend
   * uses the file `myCollection.db` in the root folder `/` to store data.
   * @param cache_size Amount of memory in bytes to use for internal cache.
   * @param fsm_cache_size Amount of memory in bytes to use for internal FSM
   * cache.
   */
  Collection(const std::string &connection_string,
             size_t cache_size = DEFAULT_BUFFER_SIZE,
             size_t fsm_cache_size = DEFAULT_FSM_BUFFER_SIZE)
      : storage(CreateStorage<RecordPage>(
            ConnectionString(connection_string, DATA_STORAGE_EXTENTION))),
        buffer_manager(storage.get(), cache_size),
        fsm(connection_string, fsm_cache_size),
        page_manager(buffer_manager, fsm), metadata_manager(page_manager),
        record_manager(page_manager), openned(false) {}

  /**
   * @brief Open collection.
   *
   */
  void Open() {
    if (!openned) {
      record_manager.Start();
    }
  }

  /**
   * @brief Close collection.
   *
   */
  void Close() {
    if (openned) {
      record_manager.Stop();
    }
  }
};

#endif /* PERSIST__CORE__COLLECTION__COLLECTION_HPP */
