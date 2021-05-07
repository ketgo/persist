/**
 * metadata_manager.hpp - Persist
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

#ifndef PERSIST__CORE__METADATA_MANAGER_HPP
#define PERSIST__CORE__METADATA_MANAGER_HPP

#include <persist/core/metadata.hpp>
#include <persist/core/page_manager.hpp>

namespace persist {

/**
 * @brief Collection metadata manager. It is responsible for storing collection
 * metadata, consisting of size, location of first element, and location of last
 * element.
 *
 * TODO: Use direrectly the page manger as template parameter insted of replacer
 * and fsm.
 *
 * @tparam ReplacerType The type of page replacer to be used by buffer manager.
 * Default set to LRUReplacer.
 * @tparam FreeSpaceManagerType The type of free space manager. Default set to
 * FSLManager.
 */
template <class ReplacerType, class FreeSpaceManagerType>
class MetadataManager {
  PERSIST_PRIVATE :
      /**
       * @brief Metadata storage location. It is always stored as the first
       * record in the collection.
       *
       */
      const RecordLocation location = {1, 1};

  /**
   * @brief Reference to the collections page manager.
   *
   */
  PageManager<RecordPage, ReplacerType, FreeSpaceManagerType> &page_manager;

  /**
   * @brief Flag indicating manager started.
   *
   */
  bool started;

public:
  /**
   * @brief Construct a new Metadata Manager object.
   *
   * @param page_manager Reference to page manager
   *
   */
  MetadataManager(
      PageManager<RecordPage, ReplacerType, FreeSpaceManagerType> &page_manager)
      : page_manager(page_manager), started(false) {}

  /**
   * @brief Start metadata manager.
   *
   */
  void Start() {
    if (!started) {
      // Start page manager
      page_manager.Start();
      started = true;
    }
  }

  /**
   * @brief Stop metadata manager.
   *
   */
  void Stop() {
    if (started) {
      // Start page manager
      page_manager.Stop();
      started = false;
    }
  }

  /**
   * @brief Load metadata from backend storage.
   *
   * @param metadata
   */
  void LoadMetadata(Metadata &metadata);

  /**
   * @brief Dump metadata to backend storage.
   *
   * @param metadata
   */
  void DumpMetadata(Metadata &metadata);
};

} // namespace persist

#endif /* PERSIST__CORE__METADATA_MANAGER_HPP */
