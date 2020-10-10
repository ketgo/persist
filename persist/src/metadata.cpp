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

// ---------------------------------------------------------------------------
// TODO:
// - Increase performance by moving away from JSON based serialization.
// ---------------------------------------------------------------------------

namespace persist {

/*********************
 * MetaData
 ********************/

void MetaData::load(ByteBuffer &input) {
  // Load JSON from UBJSON
  try {
    json data = json::from_ubjson(input, false);
    data.at("pageSize").get_to(pageSize);
    data.at("numPages").get_to(numPages);
    data.at("freePages").get_to(freePages);
  } catch (json::parse_error &err) {
    throw MetaDataParseError(err.what());
  }
}

ByteBuffer &MetaData::dump() {
  // Create JSON object from header
  try {
    json data;
    data["pageSize"] = pageSize;
    data["numPages"] = numPages;
    data["freePages"] = freePages;
    // Convert JSON to UBJSON
    buffer = json::to_ubjson(data);
  } catch (json::parse_error &err) {
    throw MetaDataParseError(err.what());
  }
  return buffer;
}

/*************************
 * MetaDataDelta
 ************************/

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

ByteBuffer &MetaDataDelta::dump() {
  try {
    json data;
    data["numPages"] = numPages;
    data["freePages"] = freePages;
    // Convert JSON to UBJSON
    buffer = json::to_ubjson(data);
  } catch (json::parse_error &err) {
    throw MetaDataDeltaParseError(err.what());
  }
  return buffer;
}

} // namespace persist
