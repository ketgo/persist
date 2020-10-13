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

#include <nlohmann/json.hpp>

#include "utility.hpp"

#include <persist/core/common.hpp>
#include <persist/core/exceptions.hpp>
#include <persist/core/page.hpp>

using json = nlohmann::json;

// ---------------------------------------------------------------------------
// TODO:
// - Increase performance by moving away from JSON based serialization.
// ---------------------------------------------------------------------------

namespace persist {

/************************
 * Page Header
 ***********************/

Page::Header::Header()
    : pageId(0), nextPageId(0), prevPageId(0), pageSize(DEFAULT_PAGE_SIZE) {}

Page::Header::Header(PageId pageId)
    : pageId(pageId), nextPageId(0), prevPageId(0),
      pageSize(DEFAULT_PAGE_SIZE) {}

Page::Header::Header(PageId pageId, uint64_t tail)
    : pageId(pageId), nextPageId(0), prevPageId(0), pageSize(tail) {}

uint64_t Page::Header::size() {
  // TODO: Use binary header serialization so that no dump is needed for
  // size calculation.
  return dump().size();
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

void Page::Header::load(ByteBuffer &input) {
  // Load JSON from UBJSON
  try {
    json data = json::from_ubjson(input, false);
    data.at("pageId").get_to(pageId);
    data.at("nextPageId").get_to(nextPageId);
    data.at("prevPageId").get_to(prevPageId);
    data.at("pageSize").get_to(pageSize);
    json slotsData = data.at("slots");
    slots.clear();
    for (auto &slotData : slotsData) {
      Page::Header::Slot slot;
      slotData.at("id").get_to(slot.id);
      slotData.at("offset").get_to(slot.offset);
      slotData.at("size").get_to(slot.size);
      slots.push_back(slot);
    }
  } catch (json::parse_error &err) {
    throw PageParseError(err.what());
  }
}

ByteBuffer &Page::Header::dump() {
  // Create JSON object from header
  try {
    json data;
    data["pageId"] = pageId;
    data["nextPageId"] = nextPageId;
    data["prevPageId"] = prevPageId;
    data["pageSize"] = pageSize;
    data["slots"] = json::array();
    for (auto &slot : slots) {
      json slotData;
      slotData["id"] = slot.id;
      slotData["offset"] = slot.offset;
      slotData["size"] = slot.size;
      data["slots"].push_back(slotData);
    }
    // Convert JSON to UBJSON
    buffer = json::to_ubjson(data);
  } catch (json::parse_error &err) {
    throw PageParseError(err.what());
  }

  return buffer;
}

/************************
 * Page
 ***********************/

Page::Page() {
  // Resize internal buffer to specified block size
  buffer.resize(DEFAULT_PAGE_SIZE);
}

Page::Page(PageId pageId) : header(pageId) {
  // Resize internal buffer to specified block size
  buffer.resize(DEFAULT_PAGE_SIZE);
}

Page::Page(PageId pageId, uint64_t pageSize) : header(pageId, pageSize) {
  // Check block size greater than minimum size
  if (pageSize < MINIMUM_PAGE_SIZE) {
    throw PageSizeError(pageSize);
  }
  // Resize internal buffer to specified block size
  buffer.resize(pageSize);
}

uint64_t Page::freeSpace() { return header.tail() - header.size(); }

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

void Page::load(ByteBuffer &input) {
  // Load Page header
  header.load(input);
  // Load record blocks
  // TODO: Use spans to avoid vector constructions
  recordBlocks.clear();
  for (Header::Slots::iterator it = header.slots.begin();
       it != header.slots.end(); it++) {
    ByteBuffer::iterator start = input.begin() + it->offset;
    ByteBuffer::iterator end = start + it->size;
    ByteBuffer _input(start, end);
    RecordBlock recordBlock;
    recordBlock.load(_input);
    recordBlocks.insert({it->id, {recordBlock, &(*it)}});
  }
}

ByteBuffer &Page::dump() {
  // Dump record blocks
  std::vector<uint8_t> _output;
  for (auto &element : recordBlocks) {
    ByteBuffer &recordBlockBuffer = element.second.first.dump();
    uint64_t offset = element.second.second->offset;
    write(buffer, recordBlockBuffer, offset);
  }
  // Add header to buffer
  ByteBuffer &head = header.dump();
  write(buffer, head, 0);
  // Dump free space
  write(buffer, 0, head.size(), freeSpace());

  return buffer;
}

} // namespace persist
