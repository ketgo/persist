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

#ifndef PERSIST_CORE_PAGE_TYPE_HEADER_HPP
#define PERSIST_CORE_PAGE_TYPE_HEADER_HPP

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>

#include <persist/utility/serializer.hpp>

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
  PageTypeId type_id;

  /**
   * @brief Header checksum
   *
   */
  Checksum checksum;

public:
  /**
   * @brief Construct a new Page Type Header object
   *
   * @param type_id Page type identifier
   * @param checksum page checksum value
   */
  PageTypeHeader(PageTypeId type_id = 0, Checksum checksum = 0)
      : type_id(type_id), checksum(checksum) {}

  /**
   * @brief The method returns size in bytes of the type header.
   *
   * @returns Header size in bytes.
   */
  static size_t GetSize() { return sizeof(PageTypeHeader); }

  /**
   * @brief Get the page type identifer
   *
   * @returns Constant reference to page type identifier
   */
  const PageTypeId &GetTypeId() const { return type_id; }

  /**
   * @brief Get the page type identifer
   *
   * @returns Constant reference to page type identifier
   */
  const Checksum &GetChecksum() const { return checksum; }

  /**
   * Load header from byte string.
   *
   * @param input input buffer span to load
   */
  void Load(Span input) {
    if (input.size < GetSize()) {
      throw PageParseError();
    }

    load(input, type_id, checksum);
  }

  /**
   * Dump header to byte string.
   *
   * @param output output buffer span to dump
   */
  void Dump(Span output) {
    if (output.size < GetSize()) {
      throw PageParseError();
    }

    dump(output, type_id, checksum);
  }
};

} // namespace persist

#endif /* PERSIST_CORE_PAGE_TYPE_HEADER_HPP */
