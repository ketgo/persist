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

#include <persist/core/exceptions.hpp>
#include <persist/core/log_record.hpp>

namespace persist {

/************************
 * Log Record Header
 ***********************/

void LogRecord::Header::load(Span input) {
  if (input.size < size()) {
    throw LogRecordParseError();
  }

  // Load bytes
  Byte *pos = input.start;
  std::memcpy((void *)&seqNumber, (const void *)pos, sizeof(SeqNumber));
  pos += sizeof(SeqNumber);
  std::memcpy((void *)&prevSeqNumber, (const void *)pos, sizeof(SeqNumber));
  pos += sizeof(SeqNumber);
  std::memcpy((void *)&transactionId, (const void *)pos, sizeof(TransactionId));
  pos += sizeof(TransactionId);
  std::memcpy((void *)&checksum, (const void *)pos, sizeof(Checksum));
}

void LogRecord::Header::dump(Span output) {
  if (output.size < size()) {
    throw LogRecordParseError();
  }

  // Dump bytes
  Byte *pos = output.start;
  std::memcpy((void *)pos, (const void *)&seqNumber, sizeof(SeqNumber));
  pos += sizeof(SeqNumber);
  std::memcpy((void *)pos, (const void *)&prevSeqNumber, sizeof(SeqNumber));
  pos += sizeof(SeqNumber);
  std::memcpy((void *)pos, (const void *)&transactionId, sizeof(TransactionId));
  pos += sizeof(TransactionId);
  std::memcpy((void *)pos, (const void *)&checksum, sizeof(Checksum));
}

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

void LogRecord::load(Span input) {
  if (input.size < size()) {
    throw LogRecordParseError();
  }
  // Load header
  header.load(input);

  // Load bytes
  Byte *pos = input.start + header.size();
  std::memcpy((void *)&type, (const void *)pos, sizeof(Type));
  pos += sizeof(Type);
  std::memcpy((void *)&location, (const void *)pos,
              sizeof(RecordBlock::Location));
  pos += sizeof(RecordBlock::Location);
  uint64_t recordBlockASize;
  std::memcpy((void *)&recordBlockASize, (const void *)pos, sizeof(uint64_t));
  pos += sizeof(uint64_t);
  recordBlockA.load(Span(pos, recordBlockASize));
  pos += recordBlockASize;
  uint64_t recordBlockBSize;
  std::memcpy((void *)&recordBlockBSize, (const void *)pos, sizeof(uint64_t));
  pos += sizeof(uint64_t);
  recordBlockB.load(Span(pos, recordBlockBSize));

  // Check for corruption by matching checksum
  if (_checksum() != header.checksum) {
    throw LogRecordCorruptError();
  }
}

void LogRecord::dump(Span output) {
  if (output.size < size()) {
    throw LogRecordParseError();
  }

  // Compute and set checksum

  header.checksum = _checksum();
  // Dump header
  header.dump(output);

  // Dump bytes
  Byte *pos = output.start + header.size();
  std::memcpy((void *)pos, (const void *)&type, sizeof(Type));
  pos += sizeof(Type);
  std::memcpy((void *)pos, (const void *)&location,
              sizeof(RecordBlock::Location));
  pos += sizeof(RecordBlock::Location);
  uint64_t recordBlockASize = recordBlockA.size();
  std::memcpy((void *)pos, (const void *)&recordBlockASize, sizeof(uint64_t));
  pos += sizeof(uint64_t);
  recordBlockA.dump(Span(pos, recordBlockASize));
  pos += recordBlockASize;
  uint64_t recordBlockBSize = recordBlockB.size();
  std::memcpy((void *)pos, (const void *)&recordBlockBSize, sizeof(uint64_t));
  pos += sizeof(uint64_t);
  recordBlockB.dump(Span(pos, recordBlockBSize));
}

} // namespace persist
