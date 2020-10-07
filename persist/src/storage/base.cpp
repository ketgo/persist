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
#include <persist/core/storage/base.hpp>

using json = nlohmann::json;

// ---------------------------------------------------------------------------
// TODO:
// - Increase performance by moving away from JSON based serialization.
// ---------------------------------------------------------------------------

namespace persist {

void Storage::MetaData::load(ByteBuffer &input) {
  // Load JSON from UBJSON
  try {
    json data = json::from_ubjson(input, false);
    data.at("pageSize").get_to(pageSize);
    data.at("firstPageId").get_to(firstPageId);
    data.at("lastPageId").get_to(lastPageId);
    data.at("freePages").get_to(freePages);
  } catch (json::parse_error &err) {
    throw MetaDataParseError(err.what());
  }
}

ByteBuffer &Storage::MetaData::dump() {
  // Create JSON object from header
  try {
    json data;
    data["pageSize"] = pageSize;
    data["firstPageId"] = firstPageId;
    data["lastPageId"] = lastPageId;
    data["freePages"] = freePages;
    // Convert JSON to UBJSON
    buffer = json::to_ubjson(data);
  } catch (json::parse_error &err) {
    throw MetaDataParseError(err.what());
  }
  return buffer;
}

} // namespace persist
