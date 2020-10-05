/**
 * record_block.cpp - Persist
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
 * Record Block Implementation
 */

#include <nlohmann/json.hpp>

#include <persist/core/exceptions.hpp>
#include <persist/core/record_block.hpp>

using json = nlohmann::json;

namespace persist {

/************************
 * Record Block Header
 ***********************/

RecordBlock::Header::Header(RecordBlockId blockId) : blockId(blockId) {}

void RecordBlock::Header::load(ByteBuffer &input) {
  // Load JSON from UBJSON
  try {
    json data = json::from_ubjson(input, false);
    data.at("blockId").get_to(blockId);
  } catch (json::parse_error &err) {
    throw RecordBlockParseError(err.what());
  }
}

ByteBuffer &RecordBlock::Header::dump() {
  // Create JSON object from header
  try {
    json data;
    data["blockId"] = blockId;
    // Convert JSON to UBJSON
    buffer = json::to_ubjson(data);
  } catch (json::parse_error &err) {
    throw PageParseError(err.what());
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

uint64_t RecordBlock::size() { return header.size() + data.size(); }

} // namespace persist