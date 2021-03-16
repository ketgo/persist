/**
 * buffer_manager.hpp - Persist
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

#ifndef PERSIST_CORE_BUFFER_MANAGER_HPP
#define PERSIST_CORE_BUFFER_MANAGER_HPP

#include <list>
#include <memory>
#include <set>
#include <unordered_map>

#include <persist/core/buffer/page_handle.hpp>
#include <persist/core/buffer/replacer/creator.hpp>
#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>
#include <persist/core/fsm/creator.hpp>
#include <persist/core/page/creator.hpp>
#include <persist/core/storage/base.hpp>
#include <persist/utility/mutex.hpp>

// At the minimum 2 pages are needed in memory by record manager.
#define MINIMUM_BUFFER_SIZE 2

namespace persist {

// TODO: Read-write lock for page.

/**
 * @brief Buffer Manager
 *
 * The buffer manager handles buffer of pages loaded in memory from a backend
 * storage. The reading of pages while wrting of modifed pages are perfromed in
 * compliance with the page repleacement policy.
 *
 */
class BufferManager : public PageObserver {
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
   * Page Slot Struct
   *
   * The data structure contains page pointer and status information. A
   * collection of slots make up the memory buffer of a page table.
   */
  struct PageSlot {
    std::unique_ptr<Page> page;
    bool modified;

    /**
     * @brief Construct a new Page Slot object
     *
     */
    PageSlot() : page(nullptr), modified(false) {}
  };

  Storage *storage;                      //<- opened backend storage
  std::unique_ptr<FreeSpaceManager> fsm; //<- free space manager
  std::unique_ptr<Replacer> replacer;    //<- page replacer

  size_t max_size GUARDED_BY(lock); //<- maximum size of buffer
  typedef typename std::unordered_map<PageId, PageSlot> Buffer;
  Buffer buffer GUARDED_BY(lock); //<- buffer of page slots
  bool started GUARDED_BY(lock);  //<- flag indicating buffer manager started

  /**
   * Add page to buffer.
   *
   * @param page pointer reference to page
   */
  void Put(std::unique_ptr<Page> &page) {
    LockGuard guard(lock);

    // If buffer is full then remove the victum page
    if (max_size != 0 && buffer.size() == max_size) {
      // Get victum page ID from replacer
      PageId victum_page_id = replacer->GetVictumId();
      // Write victum page to storage if modified
      Flush(victum_page_id);
      // Remove page from buffer
      buffer.erase(victum_page_id);
      // Replacer can stop tracking the victum page
      replacer->Forget(victum_page_id);
    }

    PageId page_id = page->GetId();
    // Upsert page to buffer
    buffer[page_id].page = std::move(page);
    // Register buffer manager as observer to inserted page
    buffer[page_id].page->RegisterObserver(this);
    // Replacer starts tracking page for victum page discovery
    replacer->Track(page_id);
  }

public:
  /**
   * Construct a new BufferManager object.
   *
   * @param storage Pointer to backend storage.
   * @param max_size Maximum buffer size. If set to 0, no maximum limit is set.
   * @param replacer_type Type of page replacer to be used by buffer manager.
   * @param fsm_type Type of free space manager to be used by buffer manager.
   *
   */
  BufferManager(Storage *storage, size_t max_size = DEFAULT_BUFFER_SIZE,
                ReplacerType replacer_type = ReplacerType::LRU,
                FSMType fsm_type = FSMType::FSL)
      : storage(storage), max_size(max_size), started(false) {
    // Check buffer size value
    if (max_size != 0 && max_size < MINIMUM_BUFFER_SIZE) {
      throw BufferManagerError("Invalid value for max buffer size. The max "
                               "size can be 0 or greater than 2.");
    }
    replacer = persist::CreateReplacer(replacer_type);
    fsm = persist::CreateFSM(fsm_type, storage);
  }

  /**
   * @brief Destroy the Buffer Manager object
   *
   */
  ~BufferManager() {}

  /**
   * @brief Handle page modification.
   *
   * @param page_id ID of the page modified
   */
  void HandleModifiedPage(PageId page_id) NO_THREAD_SAFETY_ANALYSIS override {
    Mark(page_id);
  }

  /**
   * @brief Start buffer manager.
   *
   * @thread_unsafe The method is not thread safe as it is expected that the
   * user starts the buffer manager before spawning any threads.
   *
   */
  void Start() NO_THREAD_SAFETY_ANALYSIS {
    LockGuard guard(lock);

    if (!started) {
      // Start backend storage
      storage->Open();
      // Start free space manager
      fsm->Start();
      // Set state to started
      started = true;
    }
  }

  /**
   * @brief Stop buffer manager.
   *
   * All the modified pages loaded onto the buffer are flushed to backend
   * storage before stopping the manager.
   *
   * @thread_unsafe The method is not thread safe as it is expected that the
   * user stops the buffer manager after joining all the threads.
   */
  void Stop() NO_THREAD_SAFETY_ANALYSIS {
    LockGuard guard(lock);

    if (started) {
      // Stop free space manager
      fsm->Stop();
      // Flush all loaded pages
      FlushAll();
      // Close backend storage
      storage->Close();
      // Set state to stopped
      started = false;
    }
  }

  /**
   * Get a new page. The method creates a new page and loads it into buffer.
   * The `numPage` attribute in the storage metadata is increased by one.
   *
   * @thread_safe
   *
   * @tparam PageType Type of Page
   * @returns page handle object
   */
  template <class PageType> PageHandle<PageType> GetNew() {
    LockGuard guard(lock);

    // Allocate space for new page
    PageId page_id = storage->Allocate();
    // Create an empty page
    std::unique_ptr<Page> page =
        persist::CreatePage<PageType>(page_id, storage->GetPageSize());
    // Manage free space
    fsm->Manage(*page);
    // Load the new page in buffer
    Put(page);

    // Return loaded page
    return Get<PageType>(page_id);
  }

  /**
   * Get a page with free space. If no such page is available then a new page
   * is loaded into the buffer and its handle returned.
   *
   * @thread_safe
   *
   * @tparam PageType Type of Page
   * @returns page handle object
   */
  template <class PageType> PageHandle<PageType> GetFreeOrNew() {
    LockGuard guard(lock);

    // Get ID of page with free space from FSL. Currently the last ID in free
    // space list is used.
    // TODO: Implement a smart free space manager instead of just using the last
    // page in FSL
    PageId page_id = fsm->GetPageId(0);

    // Create new page if no page with free space is available
    if (!page_id) {
      return GetNew<PageType>();
    }

    return Get<PageType>(page_id);
  }

  /**
   * Get page with given ID. The page is loaded from the backend storage if it
   * is not already found in the buffer. In case the page is not found in the
   * backend storage a PageNotFoundError exception is raised.
   *
   * @thread_safe
   *
   * @tparam PageType Type of Page
   * @param page_id page ID
   * @returns page handle object
   */
  template <class PageType> PageHandle<PageType> Get(PageId page_id) {
    LockGuard guard(lock);

    // Check if page not present in buffer
    if (buffer.find(page_id) == buffer.end()) {
      // Load page from storage
      std::unique_ptr<Page> page = storage->Read(page_id);
      // Insert page in buffer in accordance with LRU strategy
      Put(page);
    }

    // Create and return page handle object
    PageType *page_ptr = static_cast<PageType *>(buffer.at(page_id).page.get());
    return PageHandle<PageType>(page_ptr, replacer.get());
  }

  /**
   * Save a single page to backend storage. The page will be stored only
   * if it is marked as modified and is unpinned.
   *
   * @thread_safe
   *
   * @param page_id page identifer
   * @returns `true` if page is flushed else `false`
   */
  bool Flush(PageId page_id) {
    LockGuard guard(lock);

    // Find page in buffer
    typedef typename Buffer::iterator BufferPosition;
    BufferPosition it = buffer.find(page_id);
    // Save page if found, modified, and not pinned
    if (it != buffer.end() && it->second.modified &&
        !replacer->IsPinned(page_id)) {
      // Persist page on backend storage
      storage->Write(*(it->second.page));
      // Since the page has been saved it is now considered as un-modified
      it->second.modified = false;
      // Page successfully flushed
      return true;
    }
    // Page not flushed
    return false;
  }

  /**
   * @brief Save all modified and unpinned pages to backend storage.
   *
   * @thread_safe
   */
  void FlushAll() {
    LockGuard guard(lock);

    // Flush all pages in buffer
    for (const auto &element : buffer) {
      Flush(element.first);
    }
  }

  /**
   * @brief This method marks the page slot for given page ID as modified and
   * updates the free space list.
   *
   * @thread_safe
   *
   * @param page_id page identifier
   */
  void Mark(PageId page_id) {
    LockGuard guard(lock);

    auto &slot = buffer.at(page_id);
    // Mark slot as modified
    slot.modified = true;
    // Manage free space
    fsm->Manage(*slot.page);
  }

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Check if Page with given ID is loaded.
   *
   * @thread_safe
   *
   * @param page_id ID of page to check if loaded
   * @returns `true` if page is loaded else `false`
   */
  bool IsPageLoaded(PageId page_id) {
    LockGuard guard(lock);

    return buffer.find(page_id) != buffer.end();
  }

  /**
   * @brief Check if buffer is full. The method can be used to detect if the
   * buffer is full and any Page loaded not present in the buffer would result
   * in replacement.
   *
   * @thread_safe
   *
   * @returns `true` if full else `false`
   */
  bool IsFull() {
    LockGuard guard(lock);

    return buffer.size() == max_size;
  }

  /**
   * @brief Check if buffer is empty.
   *
   * @returns `true` if empty else `false`
   */
  bool IsEmpty() {
    LockGuard guard(lock);

    return buffer.empty();
  }
#endif
};

} // namespace persist

#endif /* PERSIST_CORE_BUFFER_MANAGER_HPP */
