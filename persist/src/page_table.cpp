/**
 * buffer_manager.cpp - Persist
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

#include <persist/core/page_table.hpp>

namespace persist {

PageTable::PageTable(Storage &storage)
    : storage(storage), maxSize(DEFAULT_MAX_BUFFER_SIZE) {
  // Read metadata
  metadata = storage.read();
}

PageTable::PageTable(Storage &storage, uint64_t maxSize)
    : storage(storage), maxSize(maxSize) {
  // Read metadata
  metadata = storage.read();
}

void PageTable::put(std::unique_ptr<Page> &page) {
  // If buffer is full then remove the LRU page
  if (buffer.size() == maxSize) {
    // Get last page in buffer
    std::unique_ptr<Page> &lruPage = buffer.back();
    PageId lruPageId = lruPage->getId();
    // Write to storage if page is updated
    if (lruPage->isModified()) {
      storage.write(*lruPage);
    }
    // Remove page from buffer
    buffer.erase(map[lruPageId]);
    map.erase(lruPageId);
  }
  PageId pageId = page->getId();
  // Check if pageId present in cache
  if (map.find(pageId) == map.end()) {
    // Page ID not present in cache
    buffer.push_front(std::move(page));
    map[pageId] = buffer.begin();
  } else {
    // Page ID present in cache
    *map[pageId] = std::move(page);
  }
}

Page &PageTable::get() {
  // TODO
}

Page &PageTable::get(PageId pageId) {
  // Check if page not present in buffer
  if (map.find(pageId) == map.end()) {
    // Load page from storage
    std::unique_ptr<Page> page = storage.read(pageId);
    // Insert page in buffer in accordance with LRU strategy
    put(page);
  }
  // Move the entry for given pageId to front in accordance with LRU strategy
  buffer.splice(buffer.begin(), buffer, map[pageId]);

  return *(*map[pageId]);
}

void PageTable::flush() {}

} // namespace persist
