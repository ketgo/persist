/**
 * record_manager.cpp - Persist
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
#include <persist/core/record_manager.hpp>

namespace persist {

static uint64_t cachesizeToBufferCount(uint64_t cacheSize) {
  // TODO: Calculate page table max size from given cache size in bytes
  return cacheSize;
}

RecordManager::RecordManager(std::string storageURL, uint64_t cacheSize)
    : storage(Storage::create(storageURL)),
      pageTable(*storage, cachesizeToBufferCount(cacheSize)), started(false) {}

void RecordManager::start() {
  if (!storage->is_open()) {
    storage->open();
    started = true;
  }
}

void RecordManager::stop() {
  if (storage->is_open()) {
    storage->close();
    started = false;
  }
}

void RecordManager::get(ByteBuffer &buffer, RecordBlock::Location location) {
  // Check if record manager has started
  if (!started) {
    throw RecordManagerNotStartedError();
  }
  if (location.is_null()) {
    throw RecordNotFoundError("Invalid location provided.");
  }

  RecordBlock::Location readLocation = location;
  while (!readLocation.is_null()) {
    // Get record block
    Page &page = pageTable.get(readLocation.pageId);
    RecordBlock &recordBlock = page.getRecordBlock(readLocation.slotId);
    for (auto x : recordBlock.data) {
      buffer.push_back(x);
    }
    // Update read location to next block
    readLocation = recordBlock.nextLocation();
  }
}

RecordBlock::Location RecordManager::insert(ByteBuffer &buffer) {
  if (!started) {
    throw RecordManagerNotStartedError();
  }
  // Null record block containing the starting location of the inserted data
  RecordBlock nullRecordBlock;
  // Null location representing the location of the null record block
  RecordBlock::Location nullLocation;

  // Bookkeeping variables
  uint64_t toWriteSize = buffer.size(), writtenSize = 0;
  // Pointer to previous record block in linked list. Begins with pointing to
  // the null record block.
  RecordBlock *prevRecordBlock = &nullRecordBlock;
  // Pointer to previous record block location. Begins with pointing to the
  // location of the null record block.
  RecordBlock::Location *prevLocation = &nullLocation;
  // Start loop to write content in linked record blocks
  while (toWriteSize > 0) {
    // Get a free page
    Page &page = pageTable.getFree();
    prevRecordBlock->nextLocation().pageId = page.getId();

    // Create record block to add to page
    RecordBlock recordBlock;

    // Set previous record block location
    recordBlock.prevLocation().pageId = prevLocation->pageId;
    recordBlock.prevLocation().slotId = prevLocation->slotId;

    // Compute availble space to write data in page
    uint64_t writeSpace = page.freeSpace(true) - sizeof(RecordBlock::Header);
    // Check if available space is greater than the content to write
    if (toWriteSize <= writeSpace) {
      // Enough space available so write the all the left over data
      recordBlock.data.insert(recordBlock.data.end(),
                              buffer.begin() + writtenSize,
                              buffer.begin() + toWriteSize);
      prevRecordBlock->nextLocation().slotId = page.addRecordBlock(recordBlock);
      writtenSize += toWriteSize;
      toWriteSize = 0;
    } else {
      // Not enough space available so write partial data
      recordBlock.data.insert(recordBlock.data.end(),
                              buffer.begin() + writtenSize,
                              buffer.begin() + writeSpace);
      PageSlotId slotId = page.addRecordBlock(recordBlock);
      prevRecordBlock->nextLocation().slotId = slotId;
      writtenSize += writeSpace;
      toWriteSize -= writeSpace;

      // Update previous record block and location pointers
      prevLocation = &prevRecordBlock->nextLocation();
      prevRecordBlock = &page.getRecordBlock(slotId);
    }
  }

  return nullRecordBlock.nextLocation();
}

void RecordManager::update(ByteBuffer &buffer, RecordBlock::Location location) {
  if (!started) {
    throw RecordManagerNotStartedError();
  }
}

void RecordManager::remove(RecordBlock::Location location) {
  if (!started) {
    throw RecordManagerNotStartedError();
  }
}

} // namespace persist
