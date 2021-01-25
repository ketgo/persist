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

#ifndef BUFFER_MANAGER_HPP
#define BUFFER_MANAGER_HPP

#include <list>
#include <memory>
#include <set>
#include <unordered_map>

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>
#include <persist/core/fsl.hpp>
#include <persist/core/page/base.hpp>
#include <persist/core/storage/base.hpp>

#include <persist/core/buffer/page_handle.hpp>
#include <persist/core/buffer/replacer/base.hpp>

// TODO: Thread safety for concurrent transactions

// At the minimum 2 pages are needed in memory by record manager.
#define MINIMUM_BUFFER_SIZE 2

namespace persist {

/**
 * @brief Buffer Manager
 *
 * The buffer manager handles buffer of pages loaded in memory from a backend
 * storage. The reading of pages while wrting of modifed pages are perfromed in
 * compliance with the page repleacement policy.
 *
 * @tparam PageType type of page handled by the buffer manager
 * @tparam ReplacerType type of page replacer used by buffer manager
 */
template <class PageType, class ReplacerType>
class BufferManager : public PageObserver {
  static_assert(std::is_base_of<Page, PageType>::value,
                "PageType must be derived from Page class.");
  static_assert(std::is_base_of<Replacer, ReplacerType>::value,
                "ReplacerType must be derived from Replacer class.");

  PERSIST_PRIVATE
  /**
   * Page Slot Struct
   *
   * The data structure contains page pointer and status information. A
   * collection of slots make up the memory buffer of a page table.
   */
  struct PageSlot {
    std::unique_ptr<PageType> page;
    bool modified;

    /**
     * @brief Construct a new Page Slot object
     *
     */
    PageSlot() : page(nullptr), modified(false) {}
  };

  Storage<PageType> &storage; //<- opened backend storage
  std::unique_ptr<FSL> fsl;   //<- free space list

  uint64_t maxSize;      //<- maximum size of buffer
  ReplacerType replacer; //<- page replacer
  typedef typename std::unordered_map<PageId, PageSlot> Buffer;
  Buffer buffer; //<- buffer of page slots
  bool started;  //<- flag indicating buffer manager started

  /**
   * Add page to buffer.
   *
   * @param page pointer reference to page
   */
  void put(std::unique_ptr<PageType> &page) {
    // If buffer is full then remove the victum page
    if (maxSize != 0 && buffer.size() == maxSize) {
      // Get victum page ID from replacer
      PageId victumPageId = replacer.getVictumId();
      // Write victum page to storage if modified
      flush(victumPageId);
      // Remove page from buffer
      buffer.erase(victumPageId);
      // Replacer can stop tracking the victum page
      replacer.forget(victumPageId);
    }

    PageId pageId = page->getId();
    // Upsert page to buffer
    buffer[pageId].page = std::move(page);
    // Register buffer manager as observer to inserted page
    buffer[pageId].page->registerObserver(this);
    // Replacer starts tracking page for victum page discovery
    replacer.track(pageId);
  }

public:
  /**
   * Construct a new Page Table object
   *
   * @param storage reference to backend storage.
   * @param maxSize maximum buffer size. If set to 0 then no maximum limit is
   * set.
   *
   */
  BufferManager(Storage<PageType> &storage,
                uint64_t maxSize = DEFAULT_BUFFER_SIZE)
      : storage(storage), maxSize(maxSize), started(false) {
    // Check buffer size value
    if (maxSize != 0 && maxSize < MINIMUM_BUFFER_SIZE) {
      throw BufferManagerError("Invalid value for max buffer size. The max "
                               "size can be 0 or greater than 2.");
    }
  }

  /**
   * @brief Destroy the Buffer Manager object
   *
   */
  ~BufferManager() {}

  /**
   * @brief Start buffer manager.
   *
   */
  void start() {
    if (!started) {
      // Start backend storage
      storage.open();
      // Load free space list
      fsl = storage.read();
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
   */
  void stop() {
    if (started) {
      // Flush all loaded pages
      flushAll();
      // Close backend storage
      storage.close();
      // Set state to closed
      started = false;
    }
  }

  /**
   * Get a new page. The method creates a new page and loads it into buffer.
   * The `numPage` attribute in the storage metadata is increased by one.
   *
   * @returns page handle object
   */
  PageHandle<PageType> getNew() {
    // Allocate space for new page
    PageId pageId = storage->allocate();
    // Create entry for new page in free space list
    fsl->freePages.insert(pageId);
    // Create an empty page
    std::unique_ptr<PageType> page =
        std::make_unique<PageType>(pageId, storage->getPageSize());
    // Load the new page in buffer
    put(page);

    // Return loaded page
    return get(pageId);
  }

  /**
   * Get a page with free space. If no such page is available then a new page
   * is created.
   *
   * @returns page handle object
   */
  PageHandle<PageType> getFree() {
    // Create new page if no page with free space is available
    if (fsl->freePages.empty()) {
      return getNew();
    }
    // Get ID of page with free space from FSL. Currently the last ID in free
    // space list is used.
    // TODO: Implement a smart free space manager instead of just using the last
    // page in FSL
    PageId pageId = *std::prev(fsl->freePages.end());

    return get(pageId);
  }

  /**
   * Get page with given ID. The page is loaded from the backend storage if it
   * is not already found in the buffer. In case the page is not found in the
   * backend storage a PageNotFoundError exception is raised.
   *
   * @param pageId page ID
   * @returns page handle object
   */
  PageHandle<PageType> get(PageId pageId) {
    // Check if page not present in buffer
    if (buffer.find(pageId) == buffer.end()) {
      // Load page from storage
      std::unique_ptr<PageType> page = storage.read(pageId);
      // Insert page in buffer in accordance with LRU strategy
      put(page);
    }

    // Create and return page handle object
    return PageHandle<PageType>(buffer.at(pageId).page.get(), &replacer);
  }

  /**
   * Save a single page to backend storage. The page will be stored only
   * if it is marked as modified.
   *
   * @param pageId page identifer
   */
  void flush(PageId pageId) {
    // Find page in buffer
    typedef typename Buffer::iterator BufferPosition;
    BufferPosition it = buffer.find(pageId);
    // Save page if found, modified, and not pinned
    if (it != buffer.end() && it->second.modified &&
        !replacer.isPinned(pageId)) {
      // Persist FSL
      storage.write(*fsl);
      // Persist page
      storage.write(*(it->second.page));
      // Since the page has been saved it is now considered as un-modified
      it->second.modified = false;
    }
  }

  /**
   * @brief Save all modified pages to backend storage.
   *
   */
  void flushAll() {
    // Flush all pages in buffer
    for (const auto &element : buffer) {
      flush(element.first);
    }
  }

  /**
   * @brief Handle page modifications. This method marks the page slot for given
   * page ID as modified and updates the free space list.
   *
   * @param pageId page identifier
   */
  void handleModifiedPage(PageId pageId) override {
    buffer.at(pageId).modified = true;

    // TODO: The below logic of adding page to FSL should be part of free space
    // manager.

    // Check if page has free space and update free space list accordingly. Note
    // that since FSL is used to get pages with free space for INSERT page
    // operation, free space for only INSERT is checked.
    if (buffer.at(pageId).page->freeSpace(Page::Operation::INSERT) > 0) {
      // Note: FSL uses set which takes care of duplicates so no need to check
      fsl->freePages.insert(pageId);
    } else {
      fsl->freePages.erase(pageId);
    }
  }
};

} // namespace persist

#endif /* BUFFER_MANAGER_HPP */
