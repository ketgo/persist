/**
 * storage/metadata.cpp - Persist
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
 * This file contains implementation of the storage MetaData class.
 */

#include <nlohmann/json.hpp>

#include <persist/core/exceptions.hpp>
#include <persist/core/metadata.hpp>

using json = nlohmann::json;

// ----------------------------------------------------------------------------
// TODO:
//  - Little and Big Eddien mismatch during serialization. Maybe this is not
//  even needed.
// ----------------------------------------------------------------------------

namespace persist {

/*********************
 * MetaData
 ********************/

Checksum MetaData::_checksum() {

  // Implemented hash function based on comment in
  // https://stackoverflow.com/questions/20511347/a-good-hash-function-for-a-vector

  Checksum seed = size();

  seed =
      std::hash<uint64_t>()(pageSize) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  seed ^=
      std::hash<uint64_t>()(numPages) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  seed ^= std::hash<size_t>()(freePages.size()) + 0x9e3779b9 + (seed << 6) +
          (seed >> 2);
  for (PageId pageId : freePages) {
    seed ^=
        std::hash<PageId>()(pageId) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  }

  return seed;
}

void MetaData::load(Span input) {
  if (input.size < fixedSize) {
    throw MetaDataParseError();
  }
  freePages.clear(); //<- clears free pages ID in case they are loaded

  // Load bytes
  Byte *pos = input.start;
  std::memcpy((void *)&pageSize, (const void *)pos, sizeof(uint64_t));
  pos += sizeof(uint64_t);
  std::memcpy((void *)&numPages, (const void *)pos, sizeof(uint64_t));
  pos += sizeof(uint64_t);
  size_t freePagesCount;
  std::memcpy((void *)&freePagesCount, (const void *)pos, sizeof(uint64_t));
  // Check if free pages count value is valid
  int64_t maxFreePagesCount = (input.size - fixedSize) / sizeof(PageId);
  if (freePagesCount > maxFreePagesCount) {
    throw MetaDataCorruptError();
  }
  pos += sizeof(size_t);
  while (freePagesCount > 0) {
    PageId pageId;
    std::memcpy((void *)&pageId, (const void *)pos, sizeof(PageId));
    freePages.insert(pageId);
    pos += sizeof(PageId);
    --freePagesCount;
  }
  std::memcpy((void *)&checksum, (const void *)pos, sizeof(Checksum));

  // Check for corruption by matching checksum
  if (_checksum() != checksum) {
    throw MetaDataCorruptError();
  }
}

void MetaData::dump(Span output) {
  if (output.size < size()) {
    throw MetaDataParseError();
  }

  // Compute and set checksum
  checksum = _checksum();

  // Dump bytes
  Byte *pos = output.start;
  std::memcpy((void *)pos, (const void *)&pageSize, sizeof(uint64_t));
  pos += sizeof(uint64_t);
  std::memcpy((void *)pos, (const void *)&numPages, sizeof(uint64_t));
  pos += sizeof(uint64_t);
  size_t freePagesCount = freePages.size();
  std::memcpy((void *)pos, (const void *)&freePagesCount, sizeof(size_t));
  pos += sizeof(size_t);
  for (PageId pageId : freePages) {
    std::memcpy((void *)pos, (const void *)&pageId, sizeof(PageId));
    pos += sizeof(PageId);
  }
  std::memcpy((void *)pos, (const void *)&checksum, sizeof(Checksum));
}

/*************************
 * MetaDataDelta
 ************************/

void MetaDataDelta::clear() {
  numPages = 0;
  freePages.clear();
}

void MetaDataDelta::numPagesUp() { ++numPages; }

void MetaDataDelta::numPagesDown() { --numPages; }

void MetaDataDelta::addFreePage(PageId pageId) { freePages[pageId] = 1; }

void MetaDataDelta::removeFreePage(PageId pageId) { freePages[pageId] = -1; }

void MetaDataDelta::apply(MetaData &metadata) {
  // Update number of pages in metadata
  metadata.numPages += numPages;
  // Update free space list in metadata
  for (auto &x : freePages) {
    if (x.second != 0) {
      std::set<PageId>::iterator it = metadata.freePages.find(x.first);
      // Adds page ID if not in list
      if (it == metadata.freePages.end() && x.second > 0) {
        metadata.freePages.insert(x.first);
      }
      // Removes page ID if in list
      if (it != metadata.freePages.end() && x.second < 0) {
        metadata.freePages.erase(it);
      }
    }
  }
}

void MetaDataDelta::undo(MetaData &metadata) {
  // Update number of pages in metadata
  metadata.numPages -= numPages;
  // Update free space list in metadata
  for (auto &x : freePages) {
    if (x.second != 0) {
      std::set<PageId>::iterator it = metadata.freePages.find(x.first);
      // Adds page ID if not in list
      if (it == metadata.freePages.end() && x.second < 0) {
        metadata.freePages.insert(x.first);
      }
      // Removes page ID if in list
      if (it != metadata.freePages.end() && x.second > 0) {
        metadata.freePages.erase(it);
      }
    }
  }
}

void MetaDataDelta::load(ByteBuffer &input) {
  try {
    // Convert UBJSON to JSON
    json data = json::from_ubjson(input, false);
    data.at("numPages").get_to(numPages);
    data.at("freePages").get_to(freePages);
  } catch (json::parse_error &err) {
    throw MetaDataDeltaParseError(err.what());
  }
}

void MetaDataDelta::dump(ByteBuffer &output) {
  try {
    json data;
    data["numPages"] = numPages;
    data["freePages"] = freePages;
    // Convert JSON to UBJSON
    output = json::to_ubjson(data);
  } catch (json::parse_error &err) {
    throw MetaDataDeltaParseError(err.what());
  }
}

} // namespace persist
