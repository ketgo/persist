/**
 * list/record_manager.cpp - Persist
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
#include <persist/list/record_manager.hpp>

namespace persist {

// Private Methods

RecordBlock::Location
ListRecordManager::_insert(Transaction &txn, Span span,
                           RecordBlock::Location location) {

  // TODO: Fix issue with span of size 0 not getting stored.

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
    PageId pageId = page.getId();

    // Create record block to add to page
    RecordBlock recordBlock;
    // Compute availble space to write data in page
    uint64_t writeSpace = page.freeSpace(true) - sizeof(RecordBlock::Header);
    if (toWriteSize < writeSpace) {
      writeSpace = toWriteSize;
    }
    // Pointer to one past element already written
    Byte *pos = span.start + writtenSize;
    // Write data to record block and add to page
    for (int i = 0; i < writeSpace; i++) {
      recordBlock.data.push_back(*(pos + i));
    }
    auto inserted = page.addRecordBlock(txn, recordBlock);

    // Create double linkage between record blocks
    prevRecordBlock->nextLocation().pageId = pageId;
    prevRecordBlock->nextLocation().slotId = inserted.first;
    inserted.second->prevLocation().pageId = prevLocation->pageId;
    inserted.second->prevLocation().slotId = prevLocation->slotId;

    // Stage page for commit
    txn.stage(pageId);
    pageTable.mark(pageId);

    // Update previous record block and location pointers
    prevLocation = &prevRecordBlock->nextLocation();
    prevRecordBlock = inserted.second;

    // Update counters
    writtenSize += writeSpace;
    toWriteSize -= writeSpace;
  }

  return nullRecordBlock.nextLocation();
}

void ListRecordManager::_remove(Transaction &txn,
                                RecordBlock::Location location) {
  // Start removing record blocks
  RecordBlock::Location removeLocation = location;
  try {
    while (!removeLocation.isNull()) {
      PageId pageId = removeLocation.pageId;
      PageSlotId slotId = removeLocation.slotId;

      // Get record block
      Page &page = pageTable.get(pageId);
      RecordBlock &recordBlock = page.getRecordBlock(txn, slotId);
      // Set next remove location
      removeLocation = recordBlock.nextLocation();
      // Remove record block
      page.removeRecordBlock(txn, slotId);
      txn.stage(pageId);
      pageTable.mark(pageId);
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

void ListRecordManager::get(Transaction &txn, ByteBuffer &buffer,
                            RecordLocation location) {
  // Check if record manager has started
  if (!started) {
    throw RecordManagerNotStartedError();
  }

  // Check if provided location is null
  if (location.isNull()) {
    throw RecordNotFoundError("Invalid location provided.");
  }

  // Start reading record blocks
  RecordBlock::Location readLocation = location;
  try {
    while (!readLocation.isNull()) {
      // Get record block
      Page &page = pageTable.get(readLocation.pageId);
      RecordBlock &recordBlock = page.getRecordBlock(txn, readLocation.slotId);
      // TODO: Optimize by resizing buffer to buffer.size() +
      // recordBlock.data.size(); This way we reduce number of memory allocation
      // calls in the buffer.
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
}

RecordLocation ListRecordManager::insert(Transaction &txn, ByteBuffer &buffer) {
  // Check if record manager has started
  if (!started) {
    throw RecordManagerNotStartedError();
  }

  // Insert data from buffer
  RecordLocation location = _insert(txn, Span(buffer));

  return location;
}

void ListRecordManager::remove(Transaction &txn, RecordLocation location) {
  // Check if record manager has started
  if (!started) {
    throw RecordManagerNotStartedError();
  }

  // Check if provided location is null
  if (location.isNull()) {
    throw RecordNotFoundError("Invalid location provided.");
  }

  // Remove record blocks
  _remove(txn, location);
}

void ListRecordManager::update(Transaction &txn, ByteBuffer &buffer,
                               RecordLocation location) {
  // Check if record manager has started
  if (!started) {
    throw RecordManagerNotStartedError();
  }

  // Check if provided location is null
  if (location.isNull()) {
    throw RecordNotFoundError("Invalid location provided.");
  }

  // Bookkeeping variables
  uint64_t toWriteSize = buffer.size(), writtenSize = 0;
  // Start update
  RecordBlock::Location updateLocation = location;
  RecordBlock *recordBlockPtr, recordBlock;
  try {
    // In-place update record blocks
    while (!updateLocation.isNull() && toWriteSize > 0) {
      // Get record block
      Page &page = pageTable.get(updateLocation.pageId);
      recordBlockPtr = &page.getRecordBlock(txn, updateLocation.slotId);

      // Compute availble space to write data in page
      uint64_t writeSpace = recordBlockPtr->data.size() + page.freeSpace();
      if (toWriteSize < writeSpace) {
        writeSpace = toWriteSize;
      }
      // Create updated record block
      Byte *pos = buffer.data() + writtenSize;
      for (int i = 0; i < writeSpace; i++) {
        recordBlock.data.push_back(*(pos + i));
      }
      recordBlock.nextLocation() = recordBlockPtr->nextLocation();
      recordBlock.prevLocation() = recordBlockPtr->prevLocation();
      // Update page
      page.updateRecordBlock(txn, updateLocation.slotId, recordBlock);

      // Stage page for commit
      txn.stage(updateLocation.pageId);
      pageTable.mark(updateLocation.pageId);

      // Update location to next block
      updateLocation = recordBlockPtr->nextLocation();

      // Update counters
      writtenSize += writeSpace;
      toWriteSize -= writeSpace;
    }

    // Insert rest of the buffer
    if (toWriteSize > 0) {
      recordBlockPtr->nextLocation() = _insert(
          txn, Span(buffer.data() + writtenSize, toWriteSize), updateLocation);
    }

    // Remove remaining record blocks containing old data
    if (!updateLocation.isNull()) {
      recordBlockPtr->nextLocation().setNull();
      _remove(txn, updateLocation);
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
}

} // namespace persist
