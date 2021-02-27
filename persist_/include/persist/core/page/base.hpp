/**
 * page/base.hpp - Persist
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

#ifndef PERSIST_CORE_PAGE_BASE_HPP
#define PERSIST_CORE_PAGE_BASE_HPP

#include <persist/core/defs.hpp>

namespace persist {

/**
 * @brief Page Abstract Class
 *
 * A Page is a logical chunk of space on a backend storage. The base class
 * exposes interface common to all types of pages.
 *
 */
class PageBase {
public:
  /**
   * @brief Destroy the PageBase object
   *
   */
  virtual ~PageBase() {}

  /**
   * @brief Get the page type identifer.
   *
   * NOTES:
   *  1. The underlying implementation does not need to serialize/deserialize
   * this identifer as that is taken care by the polymorphic Load and Dump
   * methods.
   *  2. Each implementation should have a unique type ID.
   *
   * @returns The page type identifier
   */
  virtual PageTypeId GetPageTypeId() const = 0;

  /**
   * @brief Get the page identifier.
   *
   * @returns Constant reference to page identifier.
   */
  virtual const PageId &GetId() const = 0;

  /**
   * @brief Get the pagesize.
   *
   * @returns The page size.
   */
  virtual size_t GetSize() const = 0;

  /**
   * Get the free space size in bytes available in the page to perform the
   * specified operation.
   *
   * @param operation The type of operation for which free space is
   * requested.
   * @returns Free space available in the page
   */
  virtual size_t GetFreeSpaceSize(enum Operation operation) const = 0;

  /**
   * Load Block object from byte string.
   *
   * @param input input buffer span to load
   */
  virtual void Load(Span input) = 0;

  /**
   * Dump Block object as byte string.
   *
   * @param output output buffer span to dump
   */
  virtual void Dump(Span output) = 0;
};

} // namespace persist

#endif /* PERSIST_CORE_PAGE_BASE_HPP */
