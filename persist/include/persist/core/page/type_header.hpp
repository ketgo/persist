/**
 * page/type_header.hpp - Persist
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

#ifndef PAGE_TYPE_HEADER_HPP
#define PAGE_TYPE_HEADER_HPP

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>

namespace persist {

/**
 * @brief Page Type Header
 *
 * The header stores type information of a page and is stored as part of all
 * pages. It is sued to infer the type of page when loading from any backend
 * storage.
 *
 */
class PageTypeHeader {
  PERSIST_PRIVATE
  /**
   * @brief The type identifer of the page.
   *
   */
  PageTypeId typeId;

  /**
   * @brief Header checksum
   *
   */
  Checksum checksum;

  Checksum _checksum() const {
    // Implemented hash function based on comment in
    // https://stackoverflow.com/questions/20511347/a-good-hash-function-for-a-vector

    Checksum seed = size();
    seed ^= std::hash<PageTypeId>()(typeId) + 0x9e3779b9 + (seed << 6) +
            (seed >> 2);
    return seed;
  }

public:
  /**
   * @brief Construct a new Page Type Header object
   *
   */
  PageTypeHeader() = default;

  /**
   * @brief Construct a new Page Type Header object
   *
   * @param typeId Constant reference to page type identifier
   */
  explicit PageTypeHeader(const PageTypeId &typeId) : typeId(typeId) {}

  /**
   * @brief The method returns size in bytes of the type header.
   *
   * @returns Header size in bytes.
   */
  static size_t size() { return sizeof(PageTypeHeader); }

  /**
   * @brief Get the page type identifer
   *
   * @returns Constant reference to page type identifier
   */
  const PageTypeId &getTypeId() const { return typeId; }

  /**
   * Load header from byte string.
   *
   * @param input input buffer span to load
   */
  void load(Span input) {
    if (input.size < size()) {
      throw PageParseError();
    }

    // Load bytes
    Byte *pos = input.start;
    std::memcpy((void *)&typeId, (const void *)pos, sizeof(PageTypeId));
    pos += sizeof(PageId);
    std::memcpy((void *)&checksum, (const void *)pos, sizeof(Checksum));

    // Check for corruption by matching checksum
    if (_checksum() != checksum) {
      throw PageCorruptError();
    }
  }

  /**
   * Dump header to byte string.
   *
   * @param output output buffer span to dump
   */
  void dump(Span output) {
    if (output.size < size()) {
      throw PageParseError();
    }

    // Compute and set checksum
    checksum = _checksum();

    // Dump bytes
    Byte *pos = output.start;
    std::memcpy((void *)pos, (const void *)&typeId, sizeof(PageTypeId));
    pos += sizeof(PageId);
    std::memcpy((void *)pos, (const void *)&checksum, sizeof(Checksum));
  }
};

} // namespace persist

#endif /* PAGE_TYPE_HEADER_HPP */
