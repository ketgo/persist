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
 * Data Block Implementation
 */

#include <nlohmann/json.hpp>

#include "utility.hpp"

#include <persist/common.hpp>
#include <persist/data_block.hpp>
#include <persist/exceptions.hpp>

using json = nlohmann::json;

// ---------------------------------------------------------------------------
// TODO:
// - Increase performance by moving away from JSON based serialization.
// - Improved caching
// ---------------------------------------------------------------------------

namespace persist {

/************************
 * Data Block Header
 ***********************/

DataBlock::Header::Header()
    : blockId(0), nextBlockId(0), prevBlockId(0),
      blockSize(DEFAULT_DATA_BLOCK_SIZE) {}

DataBlock::Header::Header(DataBlockId blockId)
    : blockId(blockId), nextBlockId(0), prevBlockId(0),
      blockSize(DEFAULT_DATA_BLOCK_SIZE) {}

DataBlock::Header::Header(DataBlockId blockId, uint64_t tail)
    : blockId(blockId), nextBlockId(0), prevBlockId(0), blockSize(tail) {}

void DataBlock::Header::load(ByteBuffer &input) {
  // Load JSON from UBJSON
  try {
    json data = json::from_ubjson(input, false);
    data.at("blockId").get_to(blockId);
    data.at("nextBlockId").get_to(nextBlockId);
    data.at("prevBlockId").get_to(prevBlockId);
    data.at("blockSize").get_to(blockSize);
    json entriesData = data.at("entries");
    entries.clear();
    for (auto &entry_data : entriesData) {
      DataBlock::Header::Entry entry;
      entry_data.at("offset").get_to(entry.offset);
      entry_data.at("size").get_to(entry.size);
      this->entries.push_back(entry);
    }
  } catch (json::parse_error &err) {
    throw DataBlockParseError(err.what());
  }
}

ByteBuffer &DataBlock::Header::dump() {
  // Create JSON object from header
  try {
    json data;
    data["blockId"] = blockId;
    data["nextBlockId"] = nextBlockId;
    data["prevBlockId"] = prevBlockId;
    data["blockSize"] = blockSize;
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
    throw DataBlockParseError(err.what());
  }

  return buffer;
}

uint64_t DataBlock::Header::size() { return dump().size(); }

uint64_t DataBlock::Header::tail() {
  if (entries.empty()) {
    return blockSize;
  }
  return entries.back().offset;
}

DataBlock::Header::Entry *DataBlock::Header::useSpace(uint64_t size) {
  // Add record block entry
  entries.push_back({tail() - size, size});
  return &entries.back();
}

void DataBlock::Header::freeSpace(Entry *entry) {
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
 * Data Block
 ***********************/

DataBlock::DataBlock() {
  // Resize internal buffer to specified block size
  buffer.resize(DEFAULT_DATA_BLOCK_SIZE);
}

DataBlock::DataBlock(DataBlockId blockId) : header(blockId) {
  // Resize internal buffer to specified block size
  buffer.resize(DEFAULT_DATA_BLOCK_SIZE);
}

DataBlock::DataBlock(DataBlockId blockId, uint64_t blockSize)
    : header(blockId, blockSize) {
  // Check block size greater than minimum size
  if (blockSize < MINIMUM_DATA_BLOCK_SIZE) {
    throw DataBlockSizeError(blockSize);
  }
  // Resize internal buffer to specified block size
  buffer.resize(blockSize);
}

uint64_t DataBlock::freeSpace() { return header.tail() - header.size(); }

RecordBlock &DataBlock::getRecordBlock(RecordBlockId recordBlockId) {
  // Check if record block exists
  RecordBlockCache::iterator it = cache.find(recordBlockId);
  if (it == cache.end()) {
    throw RecordBlockNotFoundError(recordBlockId);
  }
  return it->second.first;
}

void DataBlock::addRecordBlock(RecordBlock &recordBlock) {
  RecordBlockId recordBlockId = recordBlock.getId();
  // Check if record block does not exist in the data block
  // NOTE: Storing multiple record block with same ID in a single data block
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
}

void DataBlock::removeRecordBlock(RecordBlockId recordBlockId) {
  // Check if record block exists in the data block
  RecordBlockCache::iterator it = cache.find(recordBlockId);
  if (it == cache.end()) {
    throw RecordBlockNotFoundError(recordBlockId);
  }

  // Adjusting header
  header.freeSpace(it->second.second);
  // Removing record block from cache
  cache.erase(it);
}

void DataBlock::load(ByteBuffer &input) {
  // Load data block header
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

ByteBuffer &DataBlock::dump() {
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
