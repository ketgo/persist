/**
 * storage/base.hpp - Persist
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

/**
 * Backend Storage Interface
 *
 * The header file exposes interface to implement non-volatile backend storage.
 * List of supported backends are:
 * - file
 * - memory
 */

#ifndef PERSIST_CORE_STORAGE_BASE_HPP
#define PERSIST_CORE_STORAGE_BASE_HPP

#include <memory>

#include <persist/core/page/base.hpp>

// TODO: Add interface for segmenting storage. Instead of storing all the data
// into one big chunk of persistent memory, split into multiple smaller chunks.
// For example, storing data into multiple heap files.

namespace persist {

/**
 * @brief Storage Abstract Class
 *
 * Exposes interface to open and close a backend storage.
 *
 * @tparam PageType  The type of page stored by storage.
 */
template <class PageType> class Storage {
  static_assert(std::is_base_of<Page, PageType>::value,
                "PageType must be derived from Page class.");

public:
  /**
   * @brief Destroy the Storage object
   *
   */
  virtual ~Storage() {} //<- Virtual destructor

  /**
   * @brief Open storage.
   */
  virtual void Open() = 0;

  /**
   * @brief Check if storage is open.
   */
  virtual bool IsOpen() = 0;

  /**
   * @brief Close storage.
   */
  virtual void Close() = 0;

  /**
   * @brief Remove storage.
   */
  virtual void Remove() = 0;

  /**
   * @brief Get page size.
   *
   * @returns Page size used in storage
   */
  virtual size_t GetPageSize() = 0;

  /**
   * @brief Get page count.
   *
   * @returns Number of pages in storage
   */
  virtual uint64_t GetPageCount() = 0;

  /**
   * @brief Read Page with given identifier from storage.
   *
   * @param page_id Page identifier
   * @returns Unique pointer to Page object
   */
  virtual std::unique_ptr<PageType> Read(PageId page_id) = 0;

  /**
   * @brief Write Page object to storage.
   *
   * @param page reference to Page object to be written
   */
  virtual void Write(PageType &page) = 0;

  /**
   * @brief Allocate a new page in storage. The identifier of the newly created
   * page is returned.
   *
   * @returns identifier of the newly allocated page
   */
  virtual PageId Allocate() = 0;

  /**
   * @brief Deallocate page with given identifier.
   *
   * @param page_id identifier of the page to deallocate
   */
  virtual void Deallocate(PageId page_id) = 0;
};

} // namespace persist

#endif /* PERSIST_CORE_STORAGE_BASE_HPP */
