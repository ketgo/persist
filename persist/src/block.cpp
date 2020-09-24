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

#include <persist/block.hpp>
#include <persist/common.hpp>
#include <persist/exceptions.hpp>

using json = nlohmann::json;

// ---------------------------------------------------------------------------
// TODO:
// - Increase performance by moveing away from JSON based block serialization.
// - Improved caching
// ---------------------------------------------------------------------------

namespace persist {

/************************
 * Record Block Header
 ***********************/

RecordBlock::Header::Header(RecordBlockId blockId)
    : blockId(blockId), nextDataBlockId(0), prevDataBlockId(0) {}

void RecordBlock::Header::load(ByteBuffer &input) {
  // Load JSON from UBJSON
  try {
    json data = json::from_ubjson(input, false);
    data.at("blockId").get_to(blockId);
    data.at("nextBlockId").get_to(nextDataBlockId);
    data.at("prevBlockId").get_to(prevDataBlockId);
  } catch (json::parse_error &err) {
    throw RecordBlockParseError(err.what());
  }
}

ByteBuffer &RecordBlock::Header::dump() {
  // Create JSON object from header
  try {
    json data;
    data["blockId"] = blockId;
    data["nextBlockId"] = nextDataBlockId;
    data["prevBlockId"] = prevDataBlockId;
    // Convert JSON to UBJSON
    buffer = json::to_ubjson(data);
  } catch (json::parse_error &err) {
    throw DataBlockParseError(err.what());
  }

  return buffer;
}

uint64_t RecordBlock::Header::size() { return dump().size(); }

/************************
 * Record Block
 ***********************/

RecordBlock::RecordBlock(RecordBlockId blockId) : header(blockId) {}

RecordBlock::RecordBlock(RecordBlock::Header &header) : header(header) {}

void RecordBlock::load(ByteBuffer &input) {
  header.load(input);
  data.insert(data.begin(), input.begin() + header.size(), input.end());
}

ByteBuffer &RecordBlock::dump() {
  // Check if internal buffer is empty
  if (!buffer.empty()) {
    buffer.clear();
  }
  ByteBuffer &head = header.dump();
  buffer.insert(buffer.end(), head.begin(), head.end());
  buffer.insert(buffer.end(), data.begin(), data.end());

  return buffer;
}

RecordBlockId &RecordBlock::getId() { return header.blockId; }

RecordBlockId &RecordBlock::getNextDataBlockId() {
  return header.nextDataBlockId;
}

void RecordBlock::setNextDataBlockId(DataBlockId blockId) {
  header.nextDataBlockId = blockId;
}

RecordBlockId &RecordBlock::getPrevDataBlockId() {
  return header.prevDataBlockId;
}

void RecordBlock::setPrevDataBlockId(DataBlockId blockId) {
  header.prevDataBlockId = blockId;
}

uint64_t RecordBlock::size() { return header.size() + data.size(); }

/************************
 * Data Block Header
 ***********************/

DataBlock::Header::Header(DataBlockId blockId)
    : blockId(blockId), tail(DEFAULT_DATA_BLOCK_SIZE) {}

DataBlock::Header::Header(DataBlockId blockId, uint64_t tail)
    : blockId(blockId), tail(tail) {}

void DataBlock::Header::load(ByteBuffer &input) {
  // Load JSON from UBJSON
  try {
    json data = json::from_ubjson(input, false);
    data.at("blockId").get_to(blockId);
    data.at("tail").get_to(tail);
    json entries_data = data.at("entries");
    entries.clear();
    for (auto &entry_data : entries_data) {
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
    data["tail"] = tail;
    data["entries"] = json::array();
    for (auto &entry : entries) {
      json entry_data;
      entry_data["offset"] = entry.offset;
      entry_data["size"] = entry.size;
      data["entries"].push_back(entry_data);
    }
    // Convert JSON to UBJSON
    buffer = json::to_ubjson(data);
  } catch (json::parse_error &err) {
    throw DataBlockParseError(err.what());
  }

  return buffer;
}

uint64_t DataBlock::Header::size() { return dump().size(); }

void DataBlock::Header::useSpace(uint64_t size) {
  // Decrease the size of available free space
  tail -= size;
}

void DataBlock::Header::freeSpace(uint64_t size) {
  // Adjust free space size
  tail += size;
}

/************************
 * Data Block
 ***********************/

DataBlock::DataBlock() : blockSize(DEFAULT_DATA_BLOCK_SIZE) {
  // Resize internal buffer to specified block size
  buffer.resize(blockSize);
}

DataBlock::DataBlock(DataBlockId blockId)
    : blockSize(DEFAULT_DATA_BLOCK_SIZE), header(blockId) {
  // Resize internal buffer to specified block size
  buffer.resize(blockSize);
}

DataBlock::DataBlock(DataBlockId blockId, uint64_t blockSize)
    : blockSize(blockSize), header(blockId, blockSize) {
  // Resize internal buffer to specified block size
  buffer.resize(blockSize);
}

DataBlockId &DataBlock::getId() { return header.blockId; }

uint64_t DataBlock::freeSpace() { return header.tail - header.size(); }

RecordBlock &DataBlock::getRecordBlock(RecordBlockId recordBlockId) {
  // Check if record block exists
  RecordBlockCache::iterator it = cache.find(recordBlockId);
  if (it == cache.end()) {
    throw RecordBlockNotFoundError(recordBlockId);
  }

  return it->second;
}

void DataBlock::addRecordBlock(RecordBlock &recordBlock) {
  RecordBlockId recordBlockId = recordBlock.getId();
  // Check if record block does not exist in the data block
  // NOTE: Storing multiple record block with same ID in a single data
  // is not allowed.
  RecordBlockCache::iterator it = cache.find(recordBlockId);
  if (it != cache.end()) {
    throw RecordBlockExistsError(recordBlockId);
  }
  // Append storage location entry to header for record block
  header.useSpace(recordBlock.size());
  // Insert record block to cache
  // TODO: Use move simantic instead of copy
  cache.insert({recordBlockId, recordBlock});
}

void DataBlock::removeRecordBlock(RecordBlockId recordBlockId) {
  // Check if record block exists in the data block
  RecordBlockCache::iterator it = cache.find(recordBlockId);
  if (it == cache.end()) {
    throw RecordBlockNotFoundError(recordBlockId);
  }

  // Adjusting header
  header.freeSpace(it->second.size());
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
    cache.insert({recordBlock.getId(), recordBlock});
  }
}

ByteBuffer &DataBlock::dump() {
  // Dump record blocks
  header.entries.clear();
  std::vector<uint8_t> _output;
  uint64_t offset = blockSize;
  for (auto &element : cache) {
    ByteBuffer &recordBlockBuffer = element.second.dump();
    offset -= recordBlockBuffer.size();
    header.entries.push_back({offset, recordBlockBuffer.size()});
    fillByteBuffer(buffer, recordBlockBuffer, offset);
  }
  // Add header to buffer
  ByteBuffer &head = header.dump();
  fillByteBuffer(buffer, head, 0);
  // Dump free space
  fillByteBuffer(buffer, 0, head.size(), freeSpace());

  return buffer;
}

} // namespace persist
