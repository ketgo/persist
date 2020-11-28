/**
 * list.cpp - Persist
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
 * List Implementation
 */

#include <cstring>

#include <persist/core/exceptions.hpp>
#include <persist/list/list.hpp>

namespace persist {

/****************************
 * List Node
 ****************************/

void List::Node::load(ByteBuffer &input) {
  // Load bytes
  Byte *pos = input.data();
  std::memcpy((void *)&next, (const void *)pos, sizeof(RecordLocation));
  pos += sizeof(RecordLocation);
  std::memcpy((void *)&previous, (const void *)pos, sizeof(RecordLocation));
  pos += sizeof(RecordLocation);
  size_t size = input.size() - 2 * sizeof(RecordLocation);
  record.resize(size);
  std::memcpy((void *)record.data(), (const void *)pos, size);
}

void List::Node::dump(ByteBuffer &output) {
  // Dump bytes
  size_t size = 2 * sizeof(RecordLocation) + sizeof(Byte) * record.size();
  output.resize(size);
  Byte *pos = output.data();
  std::memcpy((void *)pos, (const void *)&next, sizeof(RecordLocation));
  pos += sizeof(RecordLocation);
  std::memcpy((void *)pos, (const void *)&previous, sizeof(RecordLocation));
  pos += sizeof(RecordLocation);
  std::memcpy((void *)pos, (const void *)record.data(), record.size());
}

/****************************
 * List Iterator
 ****************************/

List::Iterator::Iterator(List *list, RecordLocation location)
    : list(list), location(location) {
  loadNode();
}

void List::Iterator::loadNode() {
  if (!list->is_open()) {
    throw CollectionNotOpenError();
  } else if (location.isNull()) {
    node.record.clear();
  } else {
    ByteBuffer buffer;
    list->get(buffer, location);
    node.load(buffer);
  }
}

} // namespace persist