/**
 * creator.hpp - Persist
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

#ifndef PERSIST_CORE_PAGE_CREATOR_HPP
#define PERSIST_CORE_PAGE_CREATOR_HPP

#include <memory>

#include <persist/core/common.hpp>
#include <persist/core/exceptions/page.hpp>
#include <persist/core/page/base.hpp>

namespace persist {

/**
 * @brief Create an empty page object of specified type.
 *
 * @tparam PageType The type of page to create.
 * @param page_id The page identifier.
 * @param page_size The page size.
 * @returns Unique pointer to the created page.
 */
template <class PageType>
static std::unique_ptr<PageType> CreatePage(PageId page_id, size_t page_size) {
  static_assert(std::is_base_of<Page, PageType>::value,
                "Page must be derived from persist::Page");

  // Check page size greater than minimum size
  if (page_size < MINIMUM_PAGE_SIZE) {
    throw PageSizeError(page_size);
  }
  // The page size is adjusted to incorporate checksum.
  return std::make_unique<PageType>(page_id, page_size - sizeof(Checksum));
}

} // namespace persist

#endif /* PERSIST_CORE_PAGE_CREATOR_HPP */
