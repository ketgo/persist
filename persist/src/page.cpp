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
// - Improved caching
// ---------------------------------------------------------------------------

namespace persist {

/************************
 * Page Header
 ***********************/

Page::Header::Header()
    : pageId(0), nextPageId(0), prevPageId(0), pageSize(DEFAULT_PAGE_SIZE) {}

Page::Header::Header(PageId blockId)
    : pageId(blockId), nextPageId(0), prevPageId(0),
      pageSize(DEFAULT_PAGE_SIZE) {}

Page::Header::Header(PageId blockId, uint64_t tail)
    : pageId(blockId), nextPageId(0), prevPageId(0), pageSize(tail) {}

void Page::Header::load(ByteBuffer &input) {
  // Load JSON from UBJSON
  try {
    json data = json::from_ubjson(input, false);
    data.at("blockId").get_to(pageId);
    data.at("nextBlockId").get_to(nextPageId);
    data.at("prevBlockId").get_to(prevPageId);
    data.at("blockSize").get_to(pageSize);
    json entriesData = data.at("entries");
    entries.clear();
    for (auto &entry_data : entriesData) {
      Page::Header::Entry entry;
      entry_data.at("offset").get_to(entry.offset);
      entry_data.at("size").get_to(entry.size);
      this->entries.push_back(entry);
    }
  } catch (json::parse_error &err) {
    throw PageParseError(err.what());
  }
}

ByteBuffer &Page::Header::dump() {
  // Create JSON object from header
  try {
    json data;
    data["blockId"] = pageId;
    data["nextBlockId"] = nextPageId;
    data["prevBlockId"] = prevPageId;
    data["blockSize"] = pageSize;
    data["entries"] = json::array();
    for (auto &entry : entries) {
      json entryData;
      entryData["offset"] = entry.offset;
      entryData["size"] = entry.size;
      data["entries"].push_back(entryData);
    }
    // Convert JSON to UBJSON
    buffer = json::to_ubjson(data);
  } catch (json::parse_error &err) {
    throw PageParseError(err.what());
  }

  return buffer;
}

uint64_t Page::Header::size() { return dump().size(); }

uint64_t Page::Header::tail() {
  if (entries.empty()) {
    return pageSize;
  }
  return entries.back().offset;
}

Page::Header::Entry *Page::Header::useSpace(uint64_t size) {
  // Add record block entry
  entries.push_back({tail() - size, size});
  return &entries.back();
}

void Page::Header::freeSpace(Entry *entry) {
  // Adjust entry offsets
  Entries::iterator it = std::prev(entries.end());
  while (it->offset < entry->offset) {
    it->offset += entry->size;
    --it;
  }
  if (it->offset == entry->offset) {
    entries.erase(it);
  }
}

/************************
 * Page
 ***********************/

Page::Page() : modified(false) {
  // Resize internal buffer to specified block size
  buffer.resize(DEFAULT_PAGE_SIZE);
}

Page::Page(PageId blockId) : header(blockId), modified(false) {
  // Resize internal buffer to specified block size
  buffer.resize(DEFAULT_PAGE_SIZE);
}

Page::Page(PageId blockId, uint64_t blockSize)
    : header(blockId, blockSize), modified(false) {
  // Check block size greater than minimum size
  if (blockSize < MINIMUM_PAGE_SIZE) {
    throw PageSizeError(blockSize);
  }
  // Resize internal buffer to specified block size
  buffer.resize(blockSize);
}

uint64_t Page::freeSpace() { return header.tail() - header.size(); }

RecordBlock &Page::getRecordBlock(RecordBlockId recordBlockId) {
  // Check if record block exists
  RecordBlockCache::iterator it = cache.find(recordBlockId);
  if (it == cache.end()) {
    throw RecordBlockNotFoundError(recordBlockId);
  }
  return it->second.first;
}

void Page::addRecordBlock(RecordBlock &recordBlock) {
  RecordBlockId recordBlockId = recordBlock.getId();
  // Check if record block does not exist in the Page
  // NOTE: Storing multiple record block with same ID in a single Page
  // is not allowed.
  RecordBlockCache::iterator it = cache.find(recordBlockId);
  if (it != cache.end()) {
    throw RecordBlockExistsError(recordBlockId);
  }
  // Append storage location entry to header for record block
  Header::Entry *entry = header.useSpace(recordBlock.size());
  // Insert record block to cache
  // TODO: Use move simantic instead of copy
  cache.insert({recordBlockId, {recordBlock, entry}});
  // Set the block to modified
  modified = true;
}

void Page::removeRecordBlock(RecordBlockId recordBlockId) {
  // Check if record block exists in the Page
  RecordBlockCache::iterator it = cache.find(recordBlockId);
  if (it == cache.end()) {
    throw RecordBlockNotFoundError(recordBlockId);
  }

  // Adjusting header
  header.freeSpace(it->second.second);
  // Removing record block from cache
  cache.erase(it);
  // Set the block to modified
  modified = true;
}

void Page::load(ByteBuffer &input) {
  // Load Page header
  header.load(input);
  // Load record blocks
  // TODO: Use spans to avoid vector constructions
  cache.clear();
  for (Header::Entries::iterator it = header.entries.begin();
       it != header.entries.end(); it++) {
    ByteBuffer::iterator start = input.begin() + it->offset;
    ByteBuffer::iterator end = start + it->size;
    ByteBuffer _input(start, end);
    RecordBlock recordBlock;
    recordBlock.load(_input);
    cache.insert({recordBlock.getId(), {recordBlock, &(*it)}});
  }
}

ByteBuffer &Page::dump() {
  // Dump record blocks
  std::vector<uint8_t> _output;
  for (auto &element : cache) {
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
