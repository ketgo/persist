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
 * Block Manager Header
 */

#ifndef PAGE_TABLE_HPP
#define PAGE_TABLE_HPP

#include <list>
#include <memory>
#include <unordered_map>

#include <persist/core/page.hpp>
#include <persist/core/storage/base.hpp>

#define DEFAULT_MAX_BUFFER_SIZE 10000

namespace persist {

/**
 * Page Buffer Type
 */
typedef std::list<std::unique_ptr<Page>> PageBuffer;

/**
 * Page Table Class
 *
 * The page table is a buffer of pages loaded in memory. It is responsible for
 * reading and writing modified pages to backend storage in compliance with LRU
 * page relplacement policy.
 *
 * - Get block with given identifer
 * - Get block with free space
 * - Create new block if no blocks with free space are available
 * - Track modified blocks and write them to storage
 */
class PageTable {
private:
  Storage &storage;                            //<- backend storage
  std::unique_ptr<Storage::MetaData> metadata; //<- page storage metadata
  PageBuffer buffer;                           //<- buffer of pages
  std::unordered_map<PageId, PageBuffer::iterator>
      map;          //<- Stores mapped references to pages in the buffer
  uint64_t maxSize; //<- maximum size of buffer

  /**
   * Add page to buffer.
   *
   * @param dataBlock pointer reference to page
   */
  void put(std::unique_ptr<Page> &dataBlock);

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
   * Get a page with free space. If no such page is available a new page is
   * created and loaded in buffer.
   *
   * @returns referece to page in buffer
   */
  Page &get();

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
   * Save all modifed pages in the buffer to backend storage.
   */
  void flush();
};

} // namespace persist

#endif /* PAGE_TABLE_HPP */
