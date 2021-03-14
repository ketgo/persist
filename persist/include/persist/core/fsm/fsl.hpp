/**
 * fsl.hpp - Persist
 *
 * Copyright 2021 Ketan Goyal
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

#ifndef PERSIST_CORE_FSL_HPP
#define PERSIST_CORE_FSL_HPP

#include <cstring>
#include <set>

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>

namespace persist {

/**
 * @brief Free Space List
 *
 * The class stores list of free pages.
 */
class FSL {
  PERSIST_PRIVATE
  /**
   * @brief Computes checksum for free space list.
   */
  Checksum _checksum() {
    // Implemented hash function based on comment in
    // https://stackoverflow.com/questions/20511347/a-good-hash-function-for-a-vector

    Checksum seed = size();

    seed ^= std::hash<size_t>()(freePages.size()) + 0x9e3779b9 + (seed << 6) +
            (seed >> 2);
    for (PageId pageId : freePages) {
      seed ^=
          std::hash<PageId>()(pageId) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }

    return seed;
  }

  /**
   * @brief Checksum to detect metadata corruption
   */
  Checksum checksum;

  /**
   * @brief Total size in bytes of fixed length data members of the metadata.
   * The value includes:
   * - sizeof(freePages.size())
   * - sizeof(Checksome)
   */
  static const size_t fixedSize = sizeof(size_t) + sizeof(Checksum);

public:
  /**
   * @brief List of free pages in the storage. These are pages containing free
   * space.
   */
  std::set<PageId> freePages;

  /**
   * Storage size of metadata. The size comprises of:
   * - sizeof(pageSize)
   * - sizeof(numPages)
   * - sizeof(freePages.size())
   * - freePages.size() * sizeof(PageId)
   * - sizeof(Checksome)
   */
  uint64_t size() { return fixedSize + sizeof(PageId) * freePages.size(); }

  /**
   * Load object from byte string
   *
   * @param input input buffer span to load
   */
  void load(Span input) {
    if (input.size < fixedSize) {
      throw FSLParseError();
    }

    // Load bytes
    Byte *pos = input.start;
    size_t freePagesCount;
    std::memcpy((void *)&freePagesCount, (const void *)pos, sizeof(uint64_t));
    pos += sizeof(size_t);
    // Check if free pages count value is valid
    int64_t maxFreePagesCount = (input.size - fixedSize) / sizeof(PageId);
    if (freePagesCount > maxFreePagesCount) {
      throw FSLCorruptError();
    }
    freePages.clear(); //<- clears free pages ID in case they are loaded
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
      throw FSLCorruptError();
    }
  }

  /**
   * Dump object as byte string
   *
   * @param output output buffer span to dump
   */
  void dump(Span output) {
    if (output.size < size()) {
      throw FSLParseError();
    }

    // Compute and set checksum
    checksum = _checksum();

    // Dump bytes
    Byte *pos = output.start;
    size_t freePagesCount = freePages.size();
    std::memcpy((void *)pos, (const void *)&freePagesCount, sizeof(size_t));
    pos += sizeof(size_t);
    for (PageId pageId : freePages) {
      std::memcpy((void *)pos, (const void *)&pageId, sizeof(PageId));
      pos += sizeof(PageId);
    }
    std::memcpy((void *)pos, (const void *)&checksum, sizeof(Checksum));
  }

#ifdef __PERSIST_DEBUG__
  /**
   * @brief Write FSM to output stream
   */
  friend std::ostream &operator<<(std::ostream &os, const FSL &fsm) {
    os << "--------- Free Space Map ---------\n";
    os << "Free Pages: \n";
    for (auto pageId : fsm.freePages) {
      os << "\tid: " << pageId << "\n";
    }
    os << "----------------------------";
    return os;
  }
#endif
};

} // namespace persist

#endif /* PERSIST_CORE_FSL_HPP */
