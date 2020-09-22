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

void RecordBlock::Header::load(std::vector<uint8_t> &input) {
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

void RecordBlock::Header::dump(std::vector<uint8_t> &output) {
  // Create JSON object from header
  try {
    json data;
    data["blockId"] = blockId;
    data["nextBlockId"] = nextDataBlockId;
    data["prevBlockId"] = prevDataBlockId;
    // Convert JSON to UBJSON
    output = json::to_ubjson(data);
  } catch (json::parse_error &err) {
    throw DataBlockParseError(err.what());
  }
}

uint64_t RecordBlock::Header::size() {
  std::vector<uint8_t> output;
  dump(output);
  return output.size();
}

/************************
 * Record Block
 ***********************/

RecordBlock::RecordBlock(RecordBlockId blockId) : header(blockId) {}

RecordBlock::RecordBlock(RecordBlock::Header &header) : header(header) {}

void RecordBlock::load(std::vector<uint8_t> &input) {
  // Check if input buffer is emtpy
  if (input.empty()) {
    throw RecordBlockParseError("Input buffer empty.");
  }
  header.load(input);
  data.insert(data.begin(), input.begin() + header.size(), input.end());
}

void RecordBlock::dump(std::vector<uint8_t> &output) {
  // Check if output buffer is emtpy
  if (!output.empty()) {
    throw RecordBlockParseError("Output buffer not empty.");
  }
  header.dump(output);
  output.insert(output.end(), data.begin(), data.end());
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

void DataBlock::Header::load(std::vector<uint8_t> &input) {
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

void DataBlock::Header::dump(std::vector<uint8_t> &output) {
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
    output = json::to_ubjson(data);
  } catch (json::parse_error &err) {
    throw DataBlockParseError(err.what());
  }
}

uint64_t DataBlock::Header::size() {
  std::vector<uint8_t> output;
  dump(output);
  return output.size();
}

/************************
 * Data Block
 ***********************/

DataBlock::DataBlock(DataBlockId blockId)
    : blockSize(DEFAULT_DATA_BLOCK_SIZE), header(blockId) {}

DataBlock::DataBlock(DataBlockId blockId, uint64_t blockSize)
    : blockSize(blockSize), header(blockId, blockSize) {}

void DataBlock::load(std::vector<uint8_t> &input) {
  // Check input buffer size
  if (input.size() != blockSize) {
    throw DataBlockParseError("Input buffer size does not match block size.");
  }
  // Load data block header
  header.load(input);
}

void DataBlock::dump(std::vector<uint8_t> &output) {
  // Check if output buffer is emtpy
  if (!output.empty()) {
    throw DataBlockParseError("Output buffer not empty.");
  }
  // Dump data block header
  header.dump(output);
  // Dump free space
  output.insert(output.end(), freeSize(), 0);
  // Dump record blocks
}

DataBlockId &DataBlock::getId() { return header.blockId; }

uint64_t DataBlock::freeSize() { return header.tail - header.size(); }

RecordBlock &DataBlock::getRecordBlock(RecordBlockId recordBlockId) {}

void DataBlock::addRecordBlock(RecordBlock &recordBlock) {}

void DataBlock::removeRecordBlock(RecordBlockId recordBlockId) {}

} // namespace persist
