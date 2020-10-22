/**
 * memory_storage.cpp - Persist
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

#include <persist/core/exceptions.hpp>
#include <persist/core/storage/memory_storage.hpp>

namespace persist {

MemoryStorage::MemoryStorage() : pageSize(DEFAULT_PAGE_SIZE) {}

MemoryStorage::MemoryStorage(uint64_t pageSize) : pageSize(pageSize) {}

void MemoryStorage::open() {}

bool MemoryStorage::is_open() { return true; }

void MemoryStorage::close() {}

void MemoryStorage::remove() {
  metadata.freePages.clear();
  metadata.numPages = 0;
  data.clear();
}

std::unique_ptr<MetaData> MemoryStorage::read() {
  std::unique_ptr<MetaData> _metadata = std::make_unique<MetaData>(metadata);

  return _metadata;
}

void MemoryStorage::write(MetaData &metadata) {
  this->metadata.pageSize = metadata.pageSize;
  this->metadata.numPages = metadata.numPages;
  this->metadata.freePages = metadata.freePages;
}

std::unique_ptr<Page> MemoryStorage::read(PageId pageId) {
  if (data.find(pageId) == data.end()) {
    throw PageNotFoundError(pageId);
  }
  std::unique_ptr<Page> page = std::make_unique<Page>(0, pageSize);
  page->load(Span(data.at(pageId)));

  return page;
}

void MemoryStorage::write(Page &page) {
  PageId pageId = page.getId();
  data[pageId] = ByteBuffer(pageSize);
  page.dump(Span(data.at(pageId)));
}

} // namespace persist