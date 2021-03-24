/**
 * fsm_page/fsl_page.hpp - Persist
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

#ifndef PERSIST_CORE_PAGE_FSMPAGE_FSLPAGE_HPP
#define PERSIST_CORE_PAGE_FSMPAGE_FSLPAGE_HPP

#include <set>

#include <persist/core/exceptions/page.hpp>
#include <persist/core/page/base.hpp>

#include <persist/utility/serializer.hpp>

namespace persist {

/**
 * @brief Free Space List Page
 *
 * The page stores list identifers of pages containing free space.
 */
class FSLPage : public Page {
  PERSIST_PRIVATE

  PageId page_id;   //<- Page identifier
  size_t page_size; //<- Page size

  size_t max_free_space; //<- Maximum free space in page
  PageId max_page_id;    //<- Maximum page ID value stored in page.
  PageId min_page_id;    //<- Minimum page ID value stored in page.

public:
  PageId next_page_id; //<- ID of linked next page
  PageId prev_page_id; //<- ID of linked previous page. This is set to the last
                       // FSLPage for root node.

  std::set<PageId>
      free_pages; //<- Sorted set of free page identifiers. Note a set data
                  // structure is used to prevent duplicates.

  /**
   * @brief Construct a new FSLPage object
   *
   * @param page_id Page identifier
   * @param page_size Page size
   */
  FSLPage(PageId page_id = 0, size_t page_size = DEFAULT_PAGE_SIZE)
      : page_id(page_id), page_size(page_size),
        max_free_space(page_size - 3 * sizeof(PageId) - sizeof(size_t)),
        max_page_id(page_id * (max_free_space / sizeof(PageId))),
        min_page_id((page_id - 1) * (max_free_space / sizeof(PageId)) + 1),
        next_page_id(0), prev_page_id(0) {}

  /**
   * Get page identifier.
   *
   * @returns Page identifier
   */
  const PageId &GetId() const override { return page_id; }

  /**
   * @brief Get the storage free space size in the page. The free space size is
   * independent of the operation.
   *
   * @param operation Operaion to be performed
   * @returns Free space in bytes
   */
  size_t GetFreeSpaceSize(Operation operation) const override {
    size_t free_space = max_free_space - sizeof(PageId) * free_pages.size();
    // Checks if the page has the minimum space required to insert a page
    // identifier
    if (free_space < sizeof(PageId)) {
      return 0;
    }
    return free_space;
  }

  /**
   * @brief Get the storage size of page.
   *
   * @returns Storage size of page.
   */
  size_t GetStorageSize() const override { return page_size; }

  PageId GetMaxPageId() const { return max_page_id; }

  PageId GetMinPageId() const { return min_page_id; }

  /**
   * Load page object from byte string.
   *
   * @param input input buffer span to load
   */
  void Load(Span input) override {
    if (input.size < page_size) {
      throw PageParseError();
    }
    free_pages.clear(); //<- clears free pages in case they are loaded
    // Load bytes
    persist::load(input, page_id, next_page_id, prev_page_id, free_pages);
  }

  /**
   * Dump page object as byte string.
   *
   * @param output output buffer span to dump
   */
  void Dump(Span output) override {
    if (output.size < page_size) {
      throw PageParseError();
    }
    // Dump bytes
    persist::dump(output, page_id, next_page_id, prev_page_id, free_pages);
    // Dump free space
    std::memset((void *)output.start, 0, output.size);
  }
};

} // namespace persist

#endif /* PERSIST_CORE_PAGE_FSMPAGE_FSLPAGE_HPP */
