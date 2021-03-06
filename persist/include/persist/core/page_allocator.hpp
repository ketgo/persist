/**
 * page_allocator.hpp - Persist
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

#ifndef PERSIST_CORE_PAGEALLOCATOR_HPP
#define PERSIST_CORE_PAGEALLOCATOR_HPP

#include <persist/core/buffer/buffer_manager.hpp>
#include <persist/core/fsm/fsl.hpp>
#include <persist/core/page/record_page/page.hpp>

namespace persist {

/**
 * @brief The page allocator allocates and de-allocates pages for storage of
 * records.
 *
 * @tparam ReplacerType The type of page replacer to be used by buffer manager.
 * Default set to LRUReplacer.
 * @tparam FreeSpaceManagerType The type of free space manager. Default set to
 * FSLManager.
 */
template <class ReplacerType = LRUReplacer,
          class FreeSpaceManagerType = FSLManager>
class PageAllocator {
  static_assert(std::is_base_of<FreeSpaceManager, FreeSpaceManagerType>::value,
                "FreeSpaceManagerType must be derived from "
                "persist::FreeSpaceManager class.");

  PERSIST_PRIVATE
  /**
   * @brief Reference to buffer manager containing buffer of pages.
   *
   */
  BufferManager<RecordPage, ReplacerType> &buffer_manager;

  /**
   * @brief Reference to free space manager
   *
   */
  FreeSpaceManagerType &fsm;

  /**
   * @brief Flag indicating allocator started.
   *
   */
  bool started;

public:
  /**
   * @brief Construct a new Page Allocator object
   *
   * @param buffer_manager Reference to buffer manager.
   * @param fsm Reference to free space manager.
   */
  PageAllocator(BufferManager<RecordPage, ReplacerType> &buffer_manager,
                FreeSpaceManagerType &fsm)
      : buffer_manager(buffer_manager), fsm(fsm) {}

  /**
   * @brief Start page allocator.
   *
   */
  void Start() {
    if (!started) {
      buffer_manager.Start();
      fsm.Start();
      started = true;
    }
  }

  /**
   * @brief Stop page allocator.
   *
   */
  void Stop() {
    if (started) {
      buffer_manager.Stop();
      fsm.Stop();
      started = false;
    }
  }

  /**
   * Get page with given ID. The page is loaded from the backend storage if it
   * is not already found in the buffer. In case the page is not found in the
   * backend storage a PageNotFoundError exception is raised.
   *
   * @param page_id Page identifer
   * @returns Page handle object
   */
  PageHandle<RecordPage> GetPage(PageId page_id) {
    return buffer_manager.Get(page_id);
  }

  /**
   * @brief Get a new page. The method creates a new page and loads it into the
   * internal buffer.
   *
   * @returns Page handle object
   */
  PageHandle<RecordPage> GetNewPage() {
    // Create new page
    auto page = buffer_manager.GetNew();
    // Manage free space in new page
    fsm.Manage(*page);

    return page;
  }

  /**
   * @brief Get a page with free space. If no such page is available then a new
   * page is loaded into the buffer and its handle returned.
   *
   * @param size_hint Desired free space size. Note that the size is treated
   * only as a hint.
   * @returns Page handle object
   */
  PageHandle<RecordPage> GetFreeOrNewPage(size_t size_hint) {
    // Get ID of page with free space
    PageId free_page_id = fsm.GetPageId(size_hint);
    // If no free page found then return a new page
    if (!free_page_id) {
      return GetNewPage();
    }
    return GetPage(free_page_id);
  }
};

} // namespace persist

#endif /* PERSIST_CORE_PAGEALLOCATOR_HPP */
