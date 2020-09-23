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

#include <iostream>

#include <nlohmann/json.hpp>

#include <persist/block.hpp>
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

DataBlock::Header::Entries::iterator
DataBlock::Header::useSpace(uint64_t size) {
  // Create entry for chunk of space
  entries.push_back({tail - size, size});
  // Decrease the size of available free space
  tail -= size;

  return std::prev(entries.end());
}

void DataBlock::Header::freeSpace(Entries::iterator it) {
  // Adjust free space size
  tail += it->size;
  // Adjusting offset of header entries
  for (Entries::iterator _it = it; _it != entries.end(); ++_it) {
    _it->offset += it->size;
  }
  // Removing entry for the chunk of space freed
  entries.erase(it);
}

/************************
 * Data Block
 ***********************/

DataBlock::DataBlock() : blockSize(DEFAULT_DATA_BLOCK_SIZE) {
  buffer.resize(blockSize);
}

DataBlock::DataBlock(DataBlockId blockId)
    : blockSize(DEFAULT_DATA_BLOCK_SIZE), header(blockId) {
  buffer.resize(blockSize);
}

DataBlock::DataBlock(DataBlockId blockId, uint64_t blockSize)
    : blockSize(blockSize), header(blockId, blockSize) {
  buffer.resize(blockSize);
}

void DataBlock::load(ByteBuffer &input) {
  // Load data block header
  header.load(input);
  // Load record blocks
  // TODO: Use spans as no new vector construction is required using it
  for (Header::Entries::iterator it = header.entries.begin();
       it != header.entries.end(); it++) {
    ByteBuffer::iterator start = input.begin() + it->offset;
    ByteBuffer::iterator end = start + it->size;
    ByteBuffer _input(start, end);
    RecordBlock recordBlock;
    recordBlock.load(_input);
    cache.insert({recordBlock.getId(), {it, recordBlock}});
  }
}

static void printb(ByteBuffer &buff) {
  for (int c : buff) {
    std::cout << c << ",";
  }
  std::cout << std::endl;
}

ByteBuffer &DataBlock::dump() {
  // Check if internal buffer is emtpy
  if (!buffer.empty()) {
    buffer.clear();
  }
  // Dump data block header
  ByteBuffer &head = header.dump();
  buffer.insert(buffer.end(), head.begin(), head.end());
  // Dump free space
  buffer.insert(buffer.end(), freeSpace(), 0);
  // Dump record blocks
  std::vector<uint8_t> _output;
  for (auto &element : cache) {
    uint64_t offset = element.second.first->offset;
    ByteBuffer &recordBlock = element.second.second.dump();
    std::cout << "RECORD_BLOCK=" << element.first << " OFFSET=" << offset
              << " SIZE=" << element.second.first->size
              << " DUMP_SIZE=" << recordBlock.size() << "\n";
    buffer.insert(buffer.begin() + offset, recordBlock.begin(),
                  recordBlock.end());
    printb(buffer);
  }

  return buffer;
}

DataBlockId &DataBlock::getId() { return header.blockId; }

uint64_t DataBlock::freeSpace() { return header.tail - header.size(); }

RecordBlock &DataBlock::getRecordBlock(RecordBlockId recordBlockId) {
  // Check if record block exists
  RecordBlockCache::iterator it = cache.find(recordBlockId);
  if (it == cache.end()) {
    throw RecordBlockNotFoundError(recordBlockId);
  }

  return it->second.second;
}

void DataBlock::addRecordBlock(RecordBlock &recordBlock) {
  // Check if record block does not exists
  // NOTE: Storing multiple record block with same ID in a single data
  // is not allowed.
  RecordBlockId recordBlockId = recordBlock.getId();
  RecordBlockCache::iterator it = cache.find(recordBlockId);
  if (it != cache.end()) {
    throw RecordBlockExistsError(recordBlockId);
  }
  std::cout << "ADDING RECORD BLOCK " << recordBlockId << "\n";

  // Append storage location entry to header for record block
  Header::Entries::iterator _it = header.useSpace(recordBlock.size());
  std::cout << "HEADER INFO \n\t";
  std::cout << "\ttail=" << header.tail << "\n";
  std::cout << "\tentries:\n";
  for (auto &entry : header.entries) {
    std::cout << "\t\toffset=" << entry.offset << " size=" << entry.size
              << "\n";
  }
  // Insert record block to cache
  // TODO: Use move simantic instead of copy
  cache.insert({recordBlockId, {_it, recordBlock}});
}

void DataBlock::removeRecordBlock(RecordBlockId recordBlockId) {
  // Check if record block exists
  RecordBlockCache::iterator it = cache.find(recordBlockId);
  if (it == cache.end()) {
    throw RecordBlockNotFoundError(recordBlockId);
  }

  // Adjusting header
  header.freeSpace(it->second.first);
  // Removing record block from cache
  cache.erase(it);
}

} // namespace persist
