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

#include <persist/core/exceptions.hpp>
#include <persist/core/record_block.hpp>

namespace persist {

// ----------------------------------------------------------------------------
// TODO:
//  - Little and Big Eddien mismatch during serialization. Maybe this is not
//  even needed.
// ----------------------------------------------------------------------------

/************************
 * Record Block Header
 ***********************/

void RecordBlock::Header::load(Span input) {
  if (input.size < size()) {
    throw RecordBlockParseError();
  }

  // Load bytes
  std::memcpy((void *)this, (const void *)input.start, size());
}

void RecordBlock::Header::dump(Span output) {
  if (output.size < size()) {
    throw RecordBlockParseError();
  }
  // Dump bytes
  std::memcpy((void *)output.start, (const void *)this, size());
}

/************************
 * Record Block
 ***********************/

Checksum RecordBlock::_checksum() {

  // Implemented hash function based on comment in
  // https://stackoverflow.com/questions/20511347/a-good-hash-function-for-a-vector

  Checksum seed = size();

  seed = std::hash<PageId>()(header.nextLocation.pageId) + 0x9e3779b9 +
         (seed << 6) + (seed >> 2);
  seed ^= std::hash<PageSlotId>()(header.nextLocation.slotId) + 0x9e3779b9 +
          (seed << 6) + (seed >> 2);
  seed ^= std::hash<PageId>()(header.prevLocation.slotId) + 0x9e3779b9 +
          (seed << 6) + (seed >> 2);
  seed ^= std::hash<PageSlotId>()(header.prevLocation.slotId) + 0x9e3779b9 +
          (seed << 6) + (seed >> 2);
  for (auto &i : data) {
    seed ^= i + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  }

  return seed;
}

void RecordBlock::load(Span input) {
  if (input.size < size()) {
    throw RecordBlockParseError();
  }
  // Load header
  header.load(input);
  // Load data
  size_t dataSize = input.size - header.size();
  data.resize(dataSize);
  std::memcpy((void *)data.data(), (const void *)(input.start + header.size()),
              dataSize);

  // Check for corruption by matching checksum
  if (_checksum() != header.checksum) {
    throw RecordBlockCorruptError();
  }
}

void RecordBlock::dump(Span output) {
  if (output.size < size()) {
    throw RecordBlockParseError();
  }

  // Compute and set checksum
  header.checksum = _checksum();

  // Dump header
  header.dump(output);
  // Dump data
  std::memcpy((void *)(output.start + header.size()), (const void *)data.data(),
              data.size());
}

RecordBlock::Location &RecordBlock::getNextLocation() {
  return header.nextLocation;
}

void RecordBlock::setNextLocation(Location &location) {
  header.nextLocation.pageId = location.pageId;
  header.nextLocation.slotId = location.slotId;
}

RecordBlock::Location &RecordBlock::getPrevLocation() {
  return header.prevLocation;
}

void RecordBlock::setPrevLocation(Location &location) {
  header.prevLocation.pageId = location.pageId;
  header.prevLocation.slotId = location.slotId;
}

} // namespace persist