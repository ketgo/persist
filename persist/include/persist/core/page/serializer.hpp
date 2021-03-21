/**
 * page/serializer.hpp - Persist
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

#ifndef PERSIST_CORE_PAGE_SERIALIZER_HPP
#define PERSIST_CORE_PAGE_SERIALIZER_HPP

#include <memory>

#include <persist/core/page/base.hpp>
#include <persist/core/page/creator.hpp>

#include <persist/utility/checksum.hpp>
#include <persist/utility/serializer.hpp>

namespace persist {

/**
 * @brief The method loads a page from byte buffer.
 *
 * @tparam PageType Type of page to load.
 * @param input Input buffer span to load.
 * @returns Unique pointer of base type to the created page object. The user
 * should cast the pointer to that of the desired page type.
 */
template <class PageType>
static std::unique_ptr<PageType> LoadPage(Span input) {
  static_assert(std::is_base_of<Page, PageType>::value,
                "Page must be derived from persist::Page");

  if (input.size < sizeof(Checksum)) {
    throw PageParseError();
  }
  // Create empty page
  auto page = persist::CreatePage<PageType>(0, input.size);
  // Load checksum
  Checksum _checksum;
  persist::load(input, _checksum);
  // Validate checksum
  if (checksum(input) != _checksum) {
    throw PageCorruptError();
  }
  // Load page
  page->Load(input);

  return page;
}

/**
 * @brief The method dumps the page to byte string.
 *
 * @param page Reference to the page object to dump.
 * @param output Output buffer span to dump.
 */
static void DumpPage(Page &page, Span output) {
  if (output.size < sizeof(Checksum)) {
    throw PageParseError();
  }
  // Dump page
  Span span = output + sizeof(Checksum);
  page.Dump(span);
  // Dump checksum
  Checksum _checksum = checksum(span);
  persist::dump(output, _checksum);
}

} // namespace persist

#endif /* PERSIST_CORE_PAGE_SERIALIZER_HPP */
