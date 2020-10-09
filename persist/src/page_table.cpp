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

#include <persist/core/exceptions.hpp>
#include <persist/core/page_table.hpp>
#include <persist/core/record_block.hpp>

namespace persist {

/********************
 * Page Table
 *******************/

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

// Private Methods

void PageTable::put(std::unique_ptr<Page> &page) {
  // If buffer is full then remove the LRU page
  if (buffer.size() == maxSize) {
    // Get last page in buffer
    PageSlot &lruPageSlot = buffer.back();
    PageId lruPageId = lruPageSlot.page->getId();
    // Write page to storage if modified
    flush(lruPageId);
    // Remove page from buffer
    buffer.erase(map[lruPageId]);
    map.erase(lruPageId);
  }
  PageId pageId = page->getId();
  // Check if pageId present in cache
  if (map.find(pageId) == map.end()) {
    // Page ID not present in cache. Insert it into a new slot in the buffer.
    buffer.push_front(
        {std::move(page), std::make_unique<MetaDataDelta>(), false});
    map[pageId] = buffer.begin();
  } else {
    // Page ID present in cache. Updated the page slot
    map.at(pageId)->page = std::move(page);
  }
}

void PageTable::mark(PageId pageId) {
  map.at(pageId)->modified = true;
  // Check if page has free space and update metadata delta accordingly
  if (map.at(pageId)->page->freeSpace() > MIN_RECORD_BLOCK_SIZE) {
    map.at(pageId)->metaDelta->addFreePage(pageId);
  } else {
    map.at(pageId)->metaDelta->removeFreePage(pageId);
  }
}

void PageTable::flush(PageId pageId) {
  // Save page if modified
  if (map.at(pageId)->modified) {
    // NOTE: System failure can lead to invalid state of the storage. That is,
    // the metadata gets updated but the page insert fails. Need failure
    // recovery via operation logs and commit checkpoints.

    // Persist updated metadata
    // Refactor: Keep copy of persisted metadata in cache
    std::unique_ptr<MetaData> _metadata = storage.read();
    map.at(pageId)->metaDelta->apply(*_metadata);
    storage.write(*_metadata);

    // Persist page
    storage.write(*(map.at(pageId)->page));
    // Since the page has been saved it is now marked as un-modified.
    map.at(pageId)->modified = false;
  }
}

// Public Methods

Page &PageTable::getNew() {
  // Create new page and load it on buffer
  PageId pageId = metadata->numPages + 1;
  std::unique_ptr<Page> page =
      std::make_unique<Page>(pageId, metadata->pageSize);
  put(page);
  // Set page slot metadata delta
  map.at(pageId)->metaDelta->numPagesUp();
  map.at(pageId)->metaDelta->addFreePage(pageId);

  // Update metadata
  metadata->numPages += 1;
  metadata->freePages.insert(pageId);

  return get(pageId);
}

Page &PageTable::getFree() {
  // Create new page if no free space page is available
  if (metadata->freePages.empty()) {
    return getNew();
  }
  // Get ID of page with free space. Currently the last ID in free space list is
  // used.
  PageId pageId = *std::prev(metadata->freePages.end());

  return get(pageId);
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
  buffer.splice(buffer.begin(), buffer, map.at(pageId));

  return *(map.at(pageId)->page);
}

/*******************
 * Page Table Session
 ******************/

void PageTable::Session::stage(PageId pageId) {
  // Stage page ID
  staged.insert(pageId);
  // Mark page for commit
  table.mark(pageId);
}

void PageTable::Session::commit() {
  // Flush all staged pages
  for (auto pageId : staged) {
    table.flush(pageId);
  }
}

} // namespace persist
