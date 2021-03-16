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

#ifndef PERSIST_CORE_STORAGE_MEMORY_STORAGE_HPP
#define PERSIST_CORE_STORAGE_MEMORY_STORAGE_HPP

#include <memory>
#include <unordered_map>

#include <persist/core/page/serializer.hpp>
#include <persist/core/storage/base.hpp>

namespace persist {
/**
 * Memory Storage
 *
 * In memory backend storage to store data in RAM. Note that this is a volatile
 * storage and should be used accordingly.
 */
class MemoryStorage : public Storage {
private:
  size_t page_size;                            //<- page size
  uint64_t page_count;                         //<- Number of pages in storage
  FSL fsl;                                     //<- free space list
  std::unordered_map<PageId, ByteBuffer> data; //<- pages stored as map

public:
  /**
   * Constructor
   */
  MemoryStorage() : page_size(DEFAULT_PAGE_SIZE), page_count(0) {}
  MemoryStorage(size_t page_size) : page_size(page_size), page_count(0) {}

  /**
   * @brief Open memory storage. No operation is performed.
   */
  void Open() override {}

  /**
   * Check if memory storage is open. Always returns `true`.
   */
  bool IsOpen() override { return true; }

  /**
   * Close memory storage. No operation is performed.
   */
  void Close() override {}

  /**
   * Remove storage. Data is cleared.
   */
  void Remove() override {
    data.clear();
    page_count = 0;
  }

  /**
   * @brief Get page size.
   *
   * @returns page size used in storage
   */
  size_t GetPageSize() override { return page_size; }

  /**
   * @brief Get page count.
   *
   * @returns number of pages in storage
   */
  uint64_t GetPageCount() override { return page_count; }

  /**
   * Read Page with given identifier from storage.
   *
   * @param page_id page identifier
   * @returns pointer to Page object
   */
  std::unique_ptr<Page> Read(PageId page_id) override {
    if (data.find(page_id) == data.end()) {
      throw PageNotFoundError(page_id);
    }
    std::unique_ptr<Page> page = persist::LoadPage(data.at(page_id));

    return page;
  }

  /**
   * Read free space list from storage. If no free list is found then pointer to
   * an empty FSL object is returned.
   *
   * @return pointer to FSL object
   */
  std::unique_ptr<FSL> Read() override {
    std::unique_ptr<FSL> _fsl = std::make_unique<FSL>(fsl);

    return _fsl;
  }

  /**
   * Write FSL object to storage.
   *
   * @param fsl reference to FSL object to be written
   */
  void Write(FSL &fsl) override { this->fsl.freePages = fsl.freePages; }

  /**
   * Write Page object to storage.
   *
   * @param page reference to Page object to be written
   */
  void Write(Page &page) override {
    PageId page_id = page.GetId();
    data[page_id] = ByteBuffer(page_size);
    persist::DumpPage(page, data.at(page_id));
  }

  /**
   * @brief Allocate a new page in storage. The identifier of the newly created
   * page is returned.
   *
   * @returns identifier of the newly allocated page
   */
  PageId Allocate() override {
    // Increase page count by 1. No need to write an empty page to storage since
    // it will be automatically handled by buffer manager.
    page_count += 1;
    return page_count;
  }

  /**
   * @brief Deallocate page with given identifier.
   *
   * @param page_id identifier of the page to deallocate
   */
  void Deallocate(PageId page_id) override {
    // TODO: No operation performed for now
  }
};

} // namespace persist

#endif /* PERSIST_CORE_STORAGE_MEMORY_STORAGE_HPP */
