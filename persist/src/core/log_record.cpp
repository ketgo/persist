/**
 * log_record.cpp - Persist
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

#include <cstring>

#include <persist/core/log_record.hpp>

namespace persist {

/************************
 * Log Record Header
 ***********************/

void LogRecord::Header::load(Span input) {}

void LogRecord::Header::dump(Span output) {}

/************************
 * Log Record
 ***********************/

Checksum LogRecord::_checksum() {

  // Implemented hash function based on comment in
  // https://stackoverflow.com/questions/20511347/a-good-hash-function-for-a-vector

  Checksum seed = recordBlockA.size() + recordBlockB.size();

  seed = std::hash<SeqNumber>()(header.seqNumber) + 0x9e3779b9 + (seed << 6) +
         (seed >> 2);
  seed ^= std::hash<SeqNumber>()(header.prevSeqNumber) + 0x9e3779b9 +
          (seed << 6) + (seed >> 2);
  seed ^= std::hash<TransactionId>()(header.transactionId) + 0x9e3779b9 +
          (seed << 6) + (seed >> 2);
  seed ^= std::hash<Type>()(type) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  seed ^= std::hash<PageId>()(location.pageId) + 0x9e3779b9 + (seed << 6) +
          (seed >> 2);
  seed ^= std::hash<PageSlotId>()(location.slotId) + 0x9e3779b9 + (seed << 6) +
          (seed >> 2);
  seed ^= std::hash<size_t>()(recordBlockA.size()) + 0x9e3779b9 + (seed << 6) +
          (seed >> 2);
  seed ^= std::hash<size_t>()(recordBlockB.size()) + 0x9e3779b9 + (seed << 6) +
          (seed >> 2);

  return seed;
}

void LogRecord::load(Span input) {}

void LogRecord::dump(Span output) {}

} // namespace persist
