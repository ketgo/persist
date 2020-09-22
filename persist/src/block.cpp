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

#include <persist/block.hpp>
#include <persist/exceptions.hpp>

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace persist {

/************************
 * Record Block Header
 ***********************/

RecordBlock::Header::Header(RecordBlockId recordBlockId)
    : recordBlockId(recordBlockId) {}

void RecordBlock::Header::load(std::vector<uint8_t> &input) {}

void RecordBlock::Header::dump(std::vector<uint8_t> &output) {}

/************************
 * Record Block
 ***********************/

RecordBlock::RecordBlock(RecordBlockId recordBlockId) : header(recordBlockId) {}

RecordBlock::RecordBlock(RecordBlockId recordBlockId, std::string data)
    : header(recordBlockId), data(data) {}

void RecordBlock::load(std::vector<uint8_t> &input) {
  // Check if input buffer is emtpy
  if (input.empty()) {
    throw RecordBlockParseError();
  }
}

void RecordBlock::dump(std::vector<uint8_t> &output) {
  // Check if output buffer is emtpy
  if (!output.empty()) {
    throw RecordBlockParseError();
  }
}

/************************
 * Data Block Header
 ***********************/

DataBlock::Header::Header(DataBlockId blockId)
    : blockId(blockId), tail(BLOCK_SIZE) {}

void DataBlock::Header::load(std::vector<uint8_t> &input) {
  // Load JSON from UBJSON
  try {
    json data = json::from_ubjson(input, false);
    data.at("blockId").get_to(this->blockId);
    data.at("tail").get_to(this->tail);
    json entries_data = data.at("entries");
    this->entries.clear();
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
    data["blockId"] = this->blockId;
    data["tail"] = this->tail;
    data["entries"] = json::array();
    for (auto &entry : this->entries) {
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

DataBlock::DataBlock(DataBlockId blockId) : header(blockId) {}

void DataBlock::load(std::vector<uint8_t> &input) {
  // Check if input buffer is emtpy
  if (input.empty()) {
    throw DataBlockParseError();
  }
  // Load data block header
  header.load(input);
}

void DataBlock::dump(std::vector<uint8_t> &output) {
  // Check if output buffer is emtpy
  if (!output.empty()) {
    throw DataBlockParseError();
  }
  // Dump data block header
  header.dump(output);
}

uint64_t DataBlock::freeSize() { return header.tail - header.size(); }

} // namespace persist
