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

#include <persist/core/exceptions/storage.hpp>
#include <persist/core/page/serializer.hpp>
#include <persist/core/storage/base.hpp>

namespace persist {
/**
 * Memory Storage
 *
 * In memory backend storage to store data in RAM. Note that this is a volatile
 * storage and should be used accordingly.
 *
 * @tparam PageType The type of page stored by storage.
 */
template <class PageType> class MemoryStorage : public Storage<PageType> {
  using Storage<PageType>::page_size;
  using Storage<PageType>::page_count;

  PERSIST_PRIVATE
  std::unordered_map<PageId, ByteBuffer> data; //<- pages stored as map

public:
  /**
   * Constructor a new MemoryStorage object.
   *
   * @param page_size storage size of data block. Default set to 1024
   */
  MemoryStorage() = default;
  MemoryStorage(size_t page_size) : Storage<PageType>(page_size) {}

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
   * Read Page with given identifier from storage.
   *
   * @param page_id page identifier
   * @returns pointer to Page object
   */
  std::unique_ptr<PageType> Read(PageId page_id) override {
    if (data.find(page_id) == data.end()) {
      throw PageNotFoundError(page_id);
    }
    std::unique_ptr<PageType> page =
        persist::LoadPage<PageType>(data.at(page_id));

    return page;
  }

  /**
   * Write Page object to storage.
   *
   * @param page reference to Page object to be written
   */
  void Write(PageType &page) override {
    PageId page_id = page.GetId();
    data[page_id] = ByteBuffer(page_size);
    persist::DumpPage(page, data.at(page_id));
  }
};

} // namespace persist

#endif /* PERSIST_CORE_STORAGE_MEMORY_STORAGE_HPP */
