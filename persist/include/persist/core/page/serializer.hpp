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

#include <persist/core/page/factory.hpp>
#include <persist/core/page/type_header.hpp>

#include <persist/utility/checksum.hpp>

namespace persist {

/**
 * @brief The method loads a page from byte buffer.
 *
 * @param input Input buffer span to load.
 * @returns Unique pointer of base type to the created page object. The user
 * should cast the pointer to that of the desired page type.
 */
static std::unique_ptr<Page> LoadPage(Span input) {
  if (input.size < PageTypeHeader::GetSize()) {
    throw PageParseError();
  }

  PageTypeHeader type_header;
  type_header.Load(input);

  Span span = input + PageTypeHeader::GetSize();
  auto page = PageFactory::GetPage(type_header.GetTypeId(), 0, span.size);
  page->Load(span);

  // Validate checksum
  if (checksum(span) != type_header.GetChecksum()) {
    throw PageCorruptError();
  }

  return page;
}

/**
 * @brief The method dumps the page to byte string.
 *
 * @param page Reference to the page object to dump.
 * @param output Output buffer span to dump.
 */
static void DumpPage(Page &page, Span output) {
  if (output.size < PageTypeHeader::GetSize()) {
    throw PageParseError();
  }

  Span span = output + PageTypeHeader::GetSize();
  page.Dump(span);

  PageTypeHeader type_header(page.GetTypeId(), checksum(span));
  type_header.Dump(output);
}

} // namespace persist

#endif /* PERSIST_CORE_PAGE_SERIALIZER_HPP */
