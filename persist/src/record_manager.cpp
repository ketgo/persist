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

// Private Methods

RecordBlock::Location RecordManager::insert(PageTable::Session &session,
                                            Span span,
                                            RecordBlock::Location location) {
  // Null record block representing the previous from first record block
  RecordBlock nullRecordBlock;

  // Bookkeeping variables
  uint64_t toWriteSize = span.size, writtenSize = 0;
  // Pointer to previous record block in the doubly linked list. Begins with
  // pointing to the null record block.
  RecordBlock *prevRecordBlock = &nullRecordBlock;
  // Pointer to previous record block location.
  RecordBlock::Location *prevLocation = &location;
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
    // Pointer to one past element already written
    Byte *pos = span.start + writtenSize;
    // Check if available space is greater than the content to write
    if (toWriteSize <= writeSpace) {
      // Enough space available so write all the left over data
      for (int i = 0; i < toWriteSize; i++) {
        recordBlock.data.push_back(*(pos + i));
      }
      prevRecordBlock->nextLocation().slotId = page.addRecordBlock(recordBlock);
      writtenSize += toWriteSize;
      toWriteSize = 0;
    } else {
      // Not enough space available so write partial data
      for (int i = 0; i < writeSpace; i++) {
        recordBlock.data.push_back(*(pos + i));
      }
      PageSlotId slotId = page.addRecordBlock(recordBlock);
      prevRecordBlock->nextLocation().slotId = slotId;
      writtenSize += writeSpace;
      toWriteSize -= writeSpace;

      // Update previous record block and location pointers
      prevLocation = &prevRecordBlock->nextLocation();
      prevRecordBlock = &page.getRecordBlock(slotId);
    }
    // Stage page for commit
    session.stage(page.getId());
  }

  return nullRecordBlock.nextLocation();
}

void RecordManager::remove(PageTable::Session &session,
                           RecordBlock::Location location) {
  // Start removing record blocks
  RecordBlock::Location removeLocation = location;
  try {
    while (!removeLocation.isNull()) {
      PageId pageId = removeLocation.pageId;
      PageSlotId slotId = removeLocation.slotId;

      // Get record block
      Page &page = pageTable.get(pageId);
      RecordBlock &recordBlock = page.getRecordBlock(slotId);
      // Set next remove location
      removeLocation = recordBlock.nextLocation();
      // Remove record block
      page.removeRecordBlock(slotId);
      session.stage(pageId);
    }
  } catch (NotFoundException &err) {
    // If a not found exception is thrown for the starting record block then
    // throw `RecordNotFoundError` exception else throw `RecordCorruptError`
    // excpetion.
    if (removeLocation == location) {
      throw RecordNotFoundError(err.what());
    } else {
      throw RecordCorruptError();
    }
  }
}

// Public Methods

void RecordManager::start() {
  if (!started) {
    pageTable.open();
    started = true;
  }
}

void RecordManager::stop() {
  if (started) {
    pageTable.close();
    started = false;
  }
}

void RecordManager::get(ByteBuffer &buffer, RecordBlock::Location location) {
  // Check if record manager has started
  if (!started) {
    throw RecordManagerNotStartedError();
  }

  // Check if provided location is null
  if (location.isNull()) {
    throw RecordNotFoundError("Invalid location provided.");
  }

  // Start page table session
  PageTable::Session session = pageTable.createSession();

  // Start reading record blocks
  RecordBlock::Location readLocation = location;
  try {
    while (!readLocation.isNull()) {
      // Get record block
      Page &page = pageTable.get(readLocation.pageId);
      RecordBlock &recordBlock = page.getRecordBlock(readLocation.slotId);
      for (auto x : recordBlock.data) {
        buffer.push_back(x);
      }
      // Update read location to next block
      readLocation = recordBlock.nextLocation();
    }
  } catch (NotFoundException &err) {
    // If a not found exception is thrown for the starting record block then
    // throw `RecordNotFoundError` exception else throw `RecordCorruptError`
    // excpetion.
    if (readLocation == location) {
      throw RecordNotFoundError(err.what());
    } else {
      throw RecordCorruptError();
    }
  }

  // Commit staged pages
  session.commit();
}

RecordBlock::Location RecordManager::insert(ByteBuffer &buffer) {
  // Check if record manager has started
  if (!started) {
    throw RecordManagerNotStartedError();
  }

  // Start page table session
  PageTable::Session session = pageTable.createSession();
  // Insert data from buffer
  RecordBlock::Location location = insert(session, Span(buffer));
  // Commit staged pages in session
  session.commit();

  return location;
}

void RecordManager::remove(RecordBlock::Location location) {
  // Check if record manager has started
  if (!started) {
    throw RecordManagerNotStartedError();
  }

  // Check if provided location is null
  if (location.isNull()) {
    throw RecordNotFoundError("Invalid location provided.");
  }

  // Start page table session
  PageTable::Session session = pageTable.createSession();
  // Remove record blocks
  remove(session, location);
  // Commit staged pages
  session.commit();
}

void RecordManager::update(ByteBuffer &buffer, RecordBlock::Location location) {
  // Check if record manager has started
  if (!started) {
    throw RecordManagerNotStartedError();
  }

  // Check if provided location is null
  if (location.isNull()) {
    throw RecordNotFoundError("Invalid location provided.");
  }

  // Start page table session
  PageTable::Session session = pageTable.createSession();

  // Bookkeeping variables
  uint64_t toWriteSize = buffer.size(), writtenSize = 0;
  // Start update
  RecordBlock::Location updateLocation = location;
  try {
    while (!updateLocation.isNull() && toWriteSize > 0) {
      // Get record block
      Page &page = pageTable.get(updateLocation.pageId);
      RecordBlock &recordBlock = page.getRecordBlock(updateLocation.slotId);

      // TODO: Update record block

      // Update read location to next block
      updateLocation = recordBlock.nextLocation();
    }

    // Insert rest of the buffer
    if (toWriteSize > 0) {
    }

    // Remove remaining record blocks containing old data
    if (!updateLocation.isNull()) {
      // TODO: Set record block next location to null
      remove(session, updateLocation);
    }
  } catch (NotFoundException &err) {
    // If a not found exception is thrown for the starting record block then
    // throw `RecordNotFoundError` exception else throw `RecordCorruptError`
    // excpetion.
    if (updateLocation == location) {
      throw RecordNotFoundError(err.what());
    } else {
      throw RecordCorruptError();
    }
  }

  // Commit staged pages
  session.commit();
}

} // namespace persist
