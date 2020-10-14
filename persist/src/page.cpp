/**
 * block.cpp - Persist
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

/**
 * Page Implementation
 */

#include <persist/core/exceptions.hpp>
#include <persist/core/page.hpp>

// ----------------------------------------------------------------------------
// TODO:
//  - Little and Big Eddien mismatch during serialization. Maybe this is not
//  even needed.
// ----------------------------------------------------------------------------

namespace persist {

/************************
 * Page Header
 ***********************/

Checksum Page::Header::_checksum() {

  // Implemented hash function based on comment in
  // https://stackoverflow.com/questions/20511347/a-good-hash-function-for-a-vector

  Checksum seed = size();

  seed = std::hash<PageId>()(pageId) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  seed ^=
      std::hash<PageId>()(nextPageId) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  seed ^=
      std::hash<PageId>()(prevPageId) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  seed ^= std::hash<size_t>()(slots.size()) + 0x9e3779b9 + (seed << 6) +
          (seed >> 2);
  for (auto &slot : slots) {
    seed ^= std::hash<PageSlotId>()(slot.id) + 0x9e3779b9 + (seed << 6) +
            (seed >> 2);
    seed ^= std::hash<uint64_t>()(slot.offset) + 0x9e3779b9 + (seed << 6) +
            (seed >> 2);
    seed ^= std::hash<uint64_t>()(slot.size) + 0x9e3779b9 + (seed << 6) +
            (seed >> 2);
  }

  return seed;
}

uint64_t Page::Header::tail() {
  if (slots.empty()) {
    return pageSize;
  }
  return slots.back().offset;
}

Page::Header::Slot *Page::Header::createSlot(uint64_t size) {
  // Get ID of the last slot
  PageSlotId lastId;
  if (slots.empty()) {
    lastId = 0;
  } else {
    lastId = slots.back().id;
  }
  // Create slot and add it to the list of slots
  slots.push_back({lastId + 1, tail() - size, size});
  return &slots.back();
}

void Page::Header::freeSlot(Slot *slot) {
  // Adjust slot offsets
  Slots::iterator it = std::prev(slots.end());
  while (it->offset < slot->offset) {
    it->offset += slot->size;
    --it;
  }
  // Remove slot from the list of slots
  if (it->offset == slot->offset) {
    slots.erase(it);
  }
}

void Page::Header::load(Span input) {
  if (input.size < fixedSize) {
    throw PageParseError();
  }
  slots.clear(); //<- clears slots in case they are loaded

  // Load bytes
  Byte *pos = input.start;
  std::memcpy((void *)&pageId, (const void *)pos, sizeof(PageId));
  pos += sizeof(PageId);
  std::memcpy((void *)&nextPageId, (const void *)pos, sizeof(PageId));
  pos += sizeof(PageId);
  std::memcpy((void *)&prevPageId, (const void *)pos, sizeof(PageId));
  pos += sizeof(PageId);
  size_t slotsCount;
  std::memcpy((void *)&slotsCount, (const void *)pos, sizeof(size_t));
  // Check if slots count value is valid
  size_t maxSlotsCount = (input.size - fixedSize) / sizeof(Slot);
  if (slotsCount > maxSlotsCount) {
    throw PageCorruptError();
  }
  pos += sizeof(size_t);
  while (slotsCount > 0) {
    Slot slot;
    std::memcpy((void *)&slot, (const void *)pos, sizeof(Slot));
    slots.push_back(slot);
    pos += sizeof(Slot);
    --slotsCount;
  }
  std::memcpy((void *)&checksum, (const void *)pos, sizeof(Checksum));

  // Check for corruption by matching checksum
  if (_checksum() != checksum) {
    throw PageCorruptError();
  }
}

void Page::Header::dump(Span output) {
  if (output.size < size()) {
    throw PageParseError();
  }

  // Compute and set checksum
  checksum = _checksum();

  // Dump bytes
  Byte *pos = output.start;
  std::memcpy((void *)pos, (const void *)&pageId, sizeof(PageId));
  pos += sizeof(PageId);
  std::memcpy((void *)pos, (const void *)&nextPageId, sizeof(PageId));
  pos += sizeof(PageId);
  std::memcpy((void *)pos, (const void *)&prevPageId, sizeof(PageId));
  pos += sizeof(PageId);
  size_t slotsCount = slots.size();
  std::memcpy((void *)pos, (const void *)&slotsCount, sizeof(size_t));
  pos += sizeof(size_t);
  for (Slot &slot : slots) {
    std::memcpy((void *)pos, (const void *)&slot, sizeof(Slot));
    pos += sizeof(Slot);
  }
  std::memcpy((void *)pos, (const void *)&checksum, sizeof(Checksum));
}

/************************
 * Page
 ***********************/

Page::Page(PageId pageId, uint64_t pageSize) : header(pageId, pageSize) {
  // Check block size greater than minimum size
  if (pageSize < MINIMUM_PAGE_SIZE) {
    throw PageSizeError(pageSize);
  }
}

uint64_t Page::freeSpace(bool exclude) {
  if (exclude) {
    return header.tail() - header.size() - sizeof(Header::Slot);
  }
  return header.tail() - header.size();
}

RecordBlock &Page::getRecordBlock(PageSlotId slotId) {
  // Check if slot exists
  RecordBlockMap::iterator it = recordBlocks.find(slotId);
  if (it == recordBlocks.end()) {
    throw RecordBlockNotFoundError(slotId);
  }
  return it->second.first;
}

PageSlotId Page::addRecordBlock(RecordBlock &recordBlock) {
  // Create slot for record block
  Header::Slot *slot = header.createSlot(recordBlock.size());
  // Insert record block at slot
  // TODO: Use move simantic instead of copy
  recordBlocks.insert({slot->id, {recordBlock, slot}});

  return slot->id;
}

void Page::removeRecordBlock(PageSlotId slotId) {
  // Check if slot exists in the Page
  RecordBlockMap::iterator it = recordBlocks.find(slotId);
  if (it == recordBlocks.end()) {
    throw RecordBlockNotFoundError(slotId);
  }
  // Adjusting header
  header.freeSlot(it->second.second);
  // Removing record block from cache
  recordBlocks.erase(it);
}

void Page::load(Span input) {
  if (input.size < header.pageSize) {
    throw PageParseError();
  }
  recordBlocks.clear(); //<- clears record blocks in case they are loaded

  // Load Page header
  header.load(input);
  // Load record blocks
  for (auto slot : header.slots) {
    Span span;
    span.start = input.start + slot.offset;
    span.size = slot.size;
    RecordBlock recordBlock;
    recordBlock.load(span);
    recordBlocks.insert({slot.id, {recordBlock, &slot}});
  }
}

void Page::dump(Span output) {
  if (output.size < header.pageSize) {
    throw PageParseError();
  }

  Span span;
  // Dump header
  span.start = output.start;
  span.size = header.size();
  header.dump(span);
  // Dump free space
  span.start += span.size;
  span.size = freeSpace();
  std::memset((void *)span.start, 0, span.size);
  // Dump record blocks
  for (auto &element : recordBlocks) {
    span.start += span.size;
    span.size = element.second.first.size();
    element.second.first.dump(span);
  }
}

} // namespace persist
