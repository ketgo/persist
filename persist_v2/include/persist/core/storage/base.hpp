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

#ifndef STORAGE_BASE_HPP
#define STORAGE_BASE_HPP

#include <list>
#include <memory>

#include <persist/core/metadata.hpp>
#include <persist/core/page/base.hpp>

namespace persist {

/**
 * @brief Storage Abstract Class
 *
 * Exposes interface to open and close a backend storage.
 *
 * @tparam PageType type of page to store
 */
template <class PageType> class Storage {
  // static_assert(std::is_base_of<PageType, Page>::value,
  //              "PageType must be derived from Page class.");

public:
  virtual ~Storage() {} //<- Virtual destructor

  /**
   * Open storage.
   */
  virtual void open() = 0;

  /**
   * Check if storage is open.
   */
  virtual bool is_open() = 0;

  /**
   * Close storage.
   */
  virtual void close() = 0;

  /**
   * Remove storage.
   */
  virtual void remove() = 0;

  /**
   * Read storage metadata information. In case no metadata information is
   * available a pointer to new metadata object is returned.
   *
   * @return pointer to MetaData object
   */
  virtual std::unique_ptr<MetaData> read() = 0;

  /**
   * Write MetaData object to storage.
   *
   * @param metadata reference to MetaData object to be written
   */
  virtual void write(MetaData &metadata) = 0;

  /**
   * Read Page with given identifier from storage.
   *
   * @param pageId page identifier
   * @returns pointer to Page object
   */
  virtual std::unique_ptr<PageType> read(PageId pageId) = 0;

  /**
   * Write Page object to storage.
   *
   * @param page reference to Page object to be written
   */
  virtual void write(PageType &page) = 0;

  /**
   * @brief Allocate a new page in storage. The identifier of the newly created
   * page is returned.
   *
   * @returns identifier of the newly allocated page
   */
  virtual PageId allocate() = 0;

  /**
   * @brief Deallocate page with given identifier.
   *
   * @param pageId identifier of the page to deallocate
   */
  virtual void deallocate(PageId pageId) = 0;
};

} // namespace persist

#endif /* STORAGE_BASE_HPP */
