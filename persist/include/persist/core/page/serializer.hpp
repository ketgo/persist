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

#ifndef PAGE_SERIALIZER_HPP
#define PAGE_SERIALIZER_HPP

#include <memory>

#include <persist/core/page/factory.hpp>
#include <persist/core/page/type_header.hpp>

namespace persist {

/**
 * @brief The method loads a page from byte buffer.
 *
 * @param input Input buffer span to load.
 * @returns Unique pointer of base type to the created page object. The user
 * should cast the pointer to that of the desired page type.
 */
static std::unique_ptr<Page> loadPage(Span input) {

  // 1. Get PageTypeId from input buffer
  // 2. Use PageFactory to create empty Page object
  // 3. Call the `load` method of the page object to load rest of the buffer

  if (input.size < PageTypeHeader::size()) {
    throw PageParseError();
  }

  PageTypeHeader type_header;
  type_header.load(input);
  Span span(input.start + PageTypeHeader::size(),
            input.size - PageTypeHeader::size());

  auto page = PageFactory::getPage(type_header.getTypeId(), 0, span.size);
  page->load(span);

  return page;
}

/**
 * @brief The method dumps the page to byte string.
 *
 * @param page Reference to the page object to dump.
 * @param output Output buffer span to dump.
 */
static void dumpPage(Page &page, Span output) {

  // 1. Get page type id from page object
  // 2. Dump the page type id to the output buffer
  // 3. Call the `dump` method of the page object to dump the page to rest of
  // the buffer

  if (output.size < PageTypeHeader::size()) {
    throw PageParseError();
  }

  PageTypeHeader type_header(page.getTypeId());
  type_header.dump(output);

  Span span(output.start + PageTypeHeader::size(),
            output.size - PageTypeHeader::size());
  page.dump(span);
}

} // namespace persist

#endif /* PAGE_SERIALIZER_HPP */
