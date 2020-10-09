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

#include <persist/core/metadata.hpp>
#include <persist/core/page.hpp>
#include <persist/core/storage/base.hpp>

#define DEFAULT_MAX_BUFFER_SIZE 10000

namespace persist {

/**
 * Page Table Class
 *
 * The page table is a buffer of pages loaded in memory. It is responsible for
 * reading and writing modified pages to backend storage in compliance with LRU
 * page relplacement policy.
 */
class PageTable {
private:
  /**
   * Page Table Session Class
   *
   * Any operations perfored on the page table are grouped under a session to
   * facilitates atomicity and concurency control.
   */
  class Session {
  private:
    /**
     * @brief Reference to page table associated with the session.
     */
    PageTable &table;
    /**
     * @brief Set of staged page ID.
     */
    std::set<PageId> staged;

  public:
    /**
     * Constructor
     */
    Session(PageTable &table) : table(table) {}

    /**
     * Stage the page with given ID for commit. This adds the page ID to the
     * stage list and marks the corresponding page as modified.
     *
     * @param pageId page identifier
     */
    void stage(PageId pageId);

    /**
     * Persist all modified pages and metadata to backend storage.
     */
    void commit();
  };

  /**
   * Page Slot Struct
   *
   * The data structure contains page pointer and status information. A
   * collection of slots make up the memory buffer of a page table.
   */
  struct PageSlot {
    std::unique_ptr<Page> page;
    bool modified;
  };

  Storage &storage;                   //<- backend storage
  std::unique_ptr<MetaData> metadata; //<- storage metadata
  uint64_t maxSize;                   //<- maximum size of buffer

  std::list<PageSlot> buffer; //<- buffer of page slots
  std::unordered_map<PageId, std::list<PageSlot>::iterator>
      map; //<- stores mapped references to pages in the buffer

  /**
   * Add page to buffer.
   *
   * @param page pointer reference to page
   */
  void put(std::unique_ptr<Page> &page);

  /**
   * Mark a page with given ID as modified and the free space map in the storage
   * metadata is updated.
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

public:
  /**
   * Construct a new Page Table object
   *
   * @param storage reference to backend storage
   * @param maxSize maximum buffer size
   */
  PageTable(Storage &storage);
  PageTable(Storage &storage, uint64_t maxSize);

  /**
   * Get a new page. The method creates a new page and loads it into buffer. The
   * `numPage` attribute in the storage metadata is increased by one.
   *
   * @returns referece to page in buffer
   */
  Page &getNew();

  /**
   * Get a page with free space. If no such page is available then a new page is
   * created.
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
   * @brief Creates a Session object
   *
   * @return Session new session object
   */
  Session createSession() { return Session(*this); }
};

} // namespace persist

#endif /* PAGE_TABLE_HPP */
