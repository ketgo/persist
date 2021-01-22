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
  uint64_t pageSize;                           //<- page block size
  MetaData metadata;                           //<- storage metadata
  std::unordered_map<PageId, ByteBuffer> data; //<- pages stored as map

public:
  /**
   * Constructor
   */
  MemoryStorage() : pageSize(DEFAULT_PAGE_SIZE) {}
  MemoryStorage(uint64_t pageSize) : pageSize(pageSize) {}

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
    metadata.freePages.clear();
    metadata.numPages = 0;
    data.clear();
  }

  /**
   * Read storage metadata information. In case no metadata information is
   * available a pointer to new metadata object is returned.
   *
   * @return pointer to MetaData object
   */
  std::unique_ptr<MetaData> read() override {
    std::unique_ptr<MetaData> _metadata = std::make_unique<MetaData>(metadata);

    return _metadata;
  }

  /**
   * Write MetaData object to storage.
   *
   * @param metadata reference to MetaData object to be written
   */
  void write(MetaData &metadata) override {
    this->metadata.pageSize = metadata.pageSize;
    this->metadata.numPages = metadata.numPages;
    this->metadata.freePages = metadata.freePages;
  }

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
};

} // namespace persist

#endif /* MEMORY_STORAGE_HPP */
