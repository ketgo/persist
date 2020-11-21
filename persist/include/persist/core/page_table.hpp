/**
 * buffer_manager.hpp - Persist
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

/**
 * Page Table Header
 */

#ifndef PAGE_TABLE_HPP
#define PAGE_TABLE_HPP

#include <list>
#include <memory>
#include <set>
#include <unordered_map>

#include <persist/core/defs.hpp>
#include <persist/core/metadata.hpp>
#include <persist/core/page.hpp>
#include <persist/core/storage/base.hpp>

#define DEFAULT_MAX_BUFFER_SIZE 10000
// At the minimum 2 pages are needed in memory by record manager.
#define MINIMUM_MAX_BUFFER_SIZE 2

namespace persist {

/**
 * Page Table Class
 *
 * The page table is a buffer of pages loaded in memory. It is responsible for
 * reading and writing modified pages to backend storage in compliance with LRU
 * page relplacement policy.
 */
class PageTable : public PageObserver {
  PERSIST_PRIVATE
  /**
   * Page Slot Struct
   *
   * The data structure contains page pointer and status information. A
   * collection of slots make up the memory buffer of a page table.
   */
  struct PageSlot {
    std::unique_ptr<Page> page;
    std::unique_ptr<MetaDataDelta> metaDelta;
    bool modified;
  };

  Storage &storage;                   //<- backend storage
  std::unique_ptr<MetaData> metadata; //<- storage metadata
  uint64_t maxSize;                   //<- maximum size of buffer

  std::list<PageSlot> buffer; //<- buffer of page slots
  typedef std::unordered_map<PageId, std::list<PageSlot>::iterator> PageSlotMap;
  PageSlotMap map; //<- stores mapped references to page slots in the buffer

  bool opened; //<- flag indicating page table is opened

  /**
   * Add page to buffer.
   *
   * @param page pointer reference to page
   */
  void put(std::unique_ptr<Page> &page);

public:
  /**
   * Construct a new Page Table object
   *
   * @param storage reference to backend storage.
   * @param maxSize maximum buffer size. If set to 0 then no maximum limit is
   * set.
   *
   * TODO:
   *  - pinning slots with pages in use
   *  - multi-threaded and multi-process access control
   */
  PageTable(Storage &storage, uint64_t maxSize = DEFAULT_MAX_BUFFER_SIZE);

  /**
   * @brief Open page table. The method opens the backend storage and sets up
   * the table metadata.
   */
  void open();

  /**
   * @brief Close page table. The method clears the internal cache and closes
   * the backend storage.
   */
  void close();

  /**
   * Get a new page. The method creates a new page and loads it into buffer.
   * The `numPage` attribute in the storage metadata is increased by one.
   *
   * @returns referece to page in buffer
   */
  Page &getNew();

  /**
   * Get a page with free space. If no such page is available then a new page
   * is created.
   *
   * @returns referece to page in buffer
   */
  Page &getFree();

  /**
   * Get page with given ID. The page is loaded from the backend storage if it
   * is not already found in the buffer. In case the page is not found in the
   * backend then PageNotFoundError exception is raised.
   *
   * @param pageId page ID
   * @returns referece to page in buffer
   */
  Page &get(PageId pageId);

  /**
   * Mark a page with given ID as modified and the free space map in the
   * storage metadata is updated.
   *
   * @param pageId page identifier
   */
  void mark(PageId pageId);

  /**
   * Save a single page to backend storage. The page will be stored only
   * if it is marked as modified.
   *
   * @param pageId page identifer
   */
  void flush(PageId pageId);

  /**
   * @brief Handle page modifications
   */
  void handleModifiedPage(PageId pageId) override;
};

} // namespace persist

#endif /* PAGE_TABLE_HPP */
