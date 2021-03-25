/**
 * fsl.hpp - Persist
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

#ifndef PERSIST_CORE_FSM_FSL_HPP
#define PERSIST_CORE_FSM_FSL_HPP

#include <cstring>
#include <memory>
#include <set>

#include <persist/core/buffer/buffer_manager.hpp>
#include <persist/core/fsm/base.hpp>
#include <persist/core/page/fsm_page/fsl_page.hpp>
#include <persist/core/storage/base.hpp>

#include <persist/utility/mutex.hpp>

namespace persist {

/**
 * @brief Free Space List Manager
 *
 * The class manages list of free pages.
 *
 */
class FSLManager : public FreeSpaceManager {
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
   * @brief Pointer to backend storage.
   *
   */
  Storage<FSLPage> *storage PT_GUARDED_BY(lock);
  /**
   * @brief Log record buffer manager.
   *
   */
  BufferManager<FSLPage> buffer_manager;
  /**
   * @brief Flag indicating log manager started.
   *
   */
  bool started GUARDED_BY(lock);
  /**
   * @brief Last page ID
   *
   */
  PageId last_page_id GUARDED_BY(lock);

  /**
   * @brief Find FSLPage to contain free space page of specified ID. The lookup
   * algorithm assumes that all the free space page IDs are stored in ascending
   * order.
   *
   * @thread_safe
   *
   * @param page_id Constant reference to identifer of page with free space.
   * @returns Identifer of FSLPage. '0' returned value indicates no FSLPage
   * found.
   */
  PageId Find(const PageId &page_id) {
    LockGuard guard(lock);

    PageId rvalue = last_page_id;
    // Check last FSLPage
    PageId max_page_id = buffer_manager.Get(rvalue)->GetMaxPageId();
    if (page_id > max_page_id) {
      // Load new FSLPages
      auto new_page = buffer_manager.GetNew();
      last_page_id = new_page->GetId();
      return last_page_id;

    } else {
      // Loop through rest of the FSLPages
      while (rvalue > 0) {
        auto page = buffer_manager.Get(rvalue);
        if (page_id > page->GetMinPageId()) {
          break;
        }
        --rvalue;
      }
    }

    return rvalue;
  }

public:
  /**
   * @brief Construct a new FSL object
   *
   * @param storage Pointer to backend FSL sotrage
   * @param cache_size FSL buffer cache
   *
   */
  explicit FSLManager(Storage<FSLPage> *storage,
                      size_t cache_size = DEFAULT_FSL_BUFFER_SIZE)
      : started(false), last_page_id(0), storage(storage),
        buffer_manager(storage, cache_size) {}

  /**
   * @brief Start free space manager.
   *
   */
  void Start() override {
    LockGuard guard(lock);

    if (!started) {
      // Start buffer manager.
      buffer_manager.Start();
      // Get last page ID
      last_page_id = storage->GetPageCount();
      // Create a new page if no last page found
      if (!last_page_id) {
        auto new_page = buffer_manager.GetNew();
        last_page_id = new_page->GetId();
      }
    }
  }

  /**
   * @brief Stop free space manager.
   *
   * @thread_safe
   *
   */
  void Stop() override {
    LockGuard guard(lock);

    if (started) {
      // Stop buffer manager.
      buffer_manager.Stop();
    }
  }

  /**
   * @brief Get ID of page with free space. The size hint is ignored and the
   * last recorded page ID in the free list is returned. If the free list is
   * empty then '0' is returned.
   *
   * @thread_safe
   *
   * @param size_hint Desired free space size.
   * @returns Page identifer if a page with free space found else '0'.
   */
  PageId GetPageId(size_t size_hint) override {
    LockGuard guard(lock);

    auto last_page = buffer_manager.Get(last_page_id);
    if (last_page->free_pages.empty()) {
      return 0;
    }
    return *std::prev(last_page->free_pages.end());
  }

  /**
   * @brief Manage free space details of specified page.
   *
   * @thread_safe
   *
   * @param page Constant reference to a Page object.
   */
  void Manage(const Page &page) override {
    LockGuard guard(lock);

    PageId page_id = page.GetId();
    auto _page = buffer_manager.Get(Find(page_id));
    // Check if page has free space and update free space list accordingly. Note
    // that since FSL is used to get pages with free space for INSERT page
    // operation, free space for only INSERT is checked.
    if (page.GetFreeSpaceSize(Operation::INSERT) > 0) {
      _page->free_pages.insert(page_id);
    } else {
      _page->free_pages.erase(page_id);
    }
  }

  /**
   * @brief Flush all free space page information to storage. This method is
   * used by transaction manager when a transaction is committed.
   *
   * @thread_safe
   */
  void Flush() override {
    LockGuard guard(lock);

    buffer_manager.FlushAll();
  }
};

} // namespace persist

#endif /* PERSIST_CORE_FSM_FSL_HPP */
