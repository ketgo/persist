/**
 * memory_storage.hpp - Persist
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

#ifndef MEMORY_STORAGE_HPP
#define MEMORY_STORAGE_HPP

#include <memory>
#include <unordered_map>

#include <persist/core/storage/base.hpp>

namespace persist {
/**
 * Memory Storage
 *
 * In memory backend storage to store data in RAM. Note that this is a volatile
 * storage and should be used accordingly.
 */
template <class PageType> class MemoryStorage : public Storage<PageType> {
private:
  uint64_t pageSize;                           //<- page size
  uint64_t pageCount;                          //<- Number of pages in storage
  FSL fsl;                                     //<- free space list
  std::unordered_map<PageId, ByteBuffer> data; //<- pages stored as map

public:
  /**
   * Constructor
   */
  MemoryStorage() : pageSize(DEFAULT_PAGE_SIZE), pageCount(0) {}
  MemoryStorage(uint64_t pageSize) : pageSize(pageSize), pageCount(0) {}

  /**
   * @brief Open memory storage. No operation is performed.
   */
  void open() override {}

  /**
   * Check if memory storage is open. Always returns `true`.
   */
  bool is_open() override { return true; }

  /**
   * Close memory storage. No operation is performed.
   */
  void close() override {}

  /**
   * Remove storage. Data is cleared.
   */
  void remove() override {
    fsl.freePages.clear();
    data.clear();
    pageCount = 0;
  }

  /**
   * @brief Get page size.
   *
   * @returns page size used in storage
   */
  uint64_t getPageSize() override { return pageSize; }

  /**
   * @brief Get page count.
   *
   * @returns number of pages in storage
   */
  uint64_t getPageCount() override { return pageCount; }

  /**
   * Read free space list from storage. If no free list is found then pointer to
   * an empty FSL object is returned.
   *
   * @return pointer to FSL object
   */
  std::unique_ptr<FSL> read() override {
    std::unique_ptr<FSL> _fsl = std::make_unique<FSL>(fsl);

    return _fsl;
  }

  /**
   * Write FSL object to storage.
   *
   * @param fsl reference to FSL object to be written
   */
  void write(FSL &fsl) override { this->fsl.freePages = fsl.freePages; }

  /**
   * Read Page with given identifier from storage.
   *
   * @param pageId page identifier
   * @returns pointer to Page object
   */
  std::unique_ptr<PageType> read(PageId pageId) override {
    if (data.find(pageId) == data.end()) {
      throw PageNotFoundError(pageId);
    }
    std::unique_ptr<PageType> page = std::make_unique<PageType>(0, pageSize);
    page->load(Span(data.at(pageId)));

    return page;
  }

  /**
   * Write Page object to storage.
   *
   * @param page reference to Page object to be written
   */
  void write(PageType &page) override {
    PageId pageId = page.getId();
    data[pageId] = ByteBuffer(pageSize);
    page.dump(Span(data.at(pageId)));
  }

  /**
   * @brief Allocate a new page in storage. The identifier of the newly created
   * page is returned.
   *
   * @returns identifier of the newly allocated page
   */
  PageId allocate() override {
    // Increase page count by 1. No need to write an empty page to storage since
    // it will be automatically handled by buffer manager.
    pageCount += 1;
    return pageCount;
  }

  /**
   * @brief Deallocate page with given identifier.
   *
   * @param pageId identifier of the page to deallocate
   */
  void deallocate(PageId pageId) override {
    // TODO: No operation performed for now
  }
};

} // namespace persist

#endif /* MEMORY_STORAGE_HPP */
