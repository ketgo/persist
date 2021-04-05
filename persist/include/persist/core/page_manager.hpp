/**
 * page_manager.hpp - Persist
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

#ifndef PERSIST_CORE_PAGEMANAGER_HPP
#define PERSIST_CORE_PAGEMANAGER_HPP

#include <persist/core/buffer/buffer_manager.hpp>
#include <persist/core/fsm/base.hpp>

#include <persist/utility/mutex.hpp>

namespace persist {

/**
 * @brief The page manager handles pages for storage of data records. It
 * comprises of buffer manager and free space manager.
 *
 * @tparam PageType The type of page.
 * @tparam ReplacerType The type of page replacer to be used by buffer manager.
 * @tparam FreeSpaceManagerType The type of free space manager.
 */
template <class PageType, class ReplacerType, class FreeSpaceManagerType>
class PageManager {
  static_assert(std::is_base_of<FreeSpaceManager, FreeSpaceManagerType>::value,
                "FreeSpaceManagerType must be derived from "
                "persist::FreeSpaceManager class.");

  PERSIST_PRIVATE
  /**
   * @brief Recursive lock for thread safety
   *
   */
  // TODO: Need granular locking
  typedef typename persist::Mutex<std::recursive_mutex> Mutex;
  Mutex lock; //<- lock for achieving thread safety via mutual exclusion
  typedef typename persist::LockGuard<Mutex> LockGuard;

  /**
   * @brief Reference to buffer manager containing buffer of pages.
   *
   */
  BufferManager<PageType, ReplacerType> &buffer_manager GUARDED_BY(lock);

  /**
   * @brief Reference to free space manager
   *
   */
  FreeSpaceManagerType &fsm GUARDED_BY(lock);

  /**
   * @brief Flag indicating allocator started.
   *
   */
  bool started GUARDED_BY(lock);

public:
  /**
   * @brief Construct a new Page Allocator object
   *
   * @param buffer_manager Reference to buffer manager.
   * @param fsm Reference to free space manager.
   */
  PageManager(BufferManager<PageType, ReplacerType> &buffer_manager,
              FreeSpaceManagerType &fsm)
      : buffer_manager(buffer_manager), fsm(fsm), started(false) {}

  /**
   * @brief Start page allocator.
   *
   * @thread_safe
   *
   */
  void Start() {
    LockGuard guard(lock);

    if (!started) {
      buffer_manager.Start();
      fsm.Start();
      started = true;
    }
  }

  /**
   * @brief Stop page allocator.
   *
   * @thread_safe
   */
  void Stop() {
    LockGuard guard(lock);

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
   * @thread_safe
   *
   * @param page_id Page identifer
   * @returns Page handle object
   */
  PageHandle<PageType> GetPage(PageId page_id) {
    LockGuard guard(lock);

    // Get page from buffer manager
    auto page = buffer_manager.Get(page_id);
    // TODO: This is inefficient since for every GetPage the fsm is getting
    // registered.
    page->RegisterObserver(&fsm);

    return page;
  }

  /**
   * @brief Get a new page. The method creates a new page and loads it into the
   * internal buffer.
   *
   * @thread_safe
   *
   * @returns Page handle object
   */
  PageHandle<PageType> GetNewPage() {
    LockGuard guard(lock);

    // Create new page
    auto page = buffer_manager.GetNew();
    // Register fsm with page
    page->RegisterObserver(&fsm);
    // Manage free space in new page
    fsm.Manage(*page.Get());

    return page;
  }

  /**
   * @brief Get a page with free space. If no such page is available then a new
   * page is loaded into the buffer and its handle returned.
   *
   * @thread_safe
   *
   * @param size_hint Desired free space size. Note that the size is treated
   * only as a hint.
   * @returns Page handle object
   */
  PageHandle<PageType> GetFreeOrNewPage(size_t size_hint) {
    LockGuard guard(lock);

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

#endif /* PERSIST_CORE_PAGEMANAGER_HPP */
