/**
 * replacer/base.hpp - Persist
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

#ifndef PERSIST__CORE__BUFFER__REPLACER__BASE_HPP
#define PERSIST__CORE__BUFFER__REPLACER__BASE_HPP

#include <memory>

#include <persist/core/defs.hpp>

namespace persist {

/**
 * @brief Replacer Base Class
 *
 * The class exposes interface for page replacer. Replacer is responsible for
 * implementing page replacement policy. Each implementation must inherit this
 * class.
 */
class Replacer {
public:
  /**
   * @brief Destroy the Replacer object
   *
   */
  virtual ~Replacer() {}

  /**
   * @brief Track page ID for detecting victum page.
   *
   * @param pageId page identifer to remember
   */
  virtual void Track(PageId pageId) = 0;

  /**
   * @brief Forget page ID for detecting victum page.
   *
   * @param pageId page identifer to forget
   */
  virtual void Forget(PageId pageId) = 0;

  /**
   * @brief Get the Victum page Id. This is the page that can be replaced by the
   * buffer manager. In case no replacement page ID is found then 0 is returned.
   *
   * @return PageId identifier of the victum page
   */
  virtual PageId GetVictumId() = 0;

  /**
   * @brief Pin page ID. A pinned ID indicates the associated page is being
   * referenced by an external process and thus should be skipped while
   * detecting victum page ID.
   *
   * @param pageId page identifer to pin
   */
  virtual void Pin(PageId pageId) = 0;

  /**
   * @brief Check if page is pinned
   *
   * @param pageId page identifier
   * @returns true if pinned else false
   */
  virtual bool IsPinned(PageId pageId) = 0;

  /**
   * @brief Unpin page ID. This notifies the replacer that the page with given
   * ID is not being referenced by an external process anymore. Note that the
   * page can still be referenced by some other external process in
   * multi-threaded settings. The replacer is free to select the ID as victum
   * only when all the external processeses stop referencing the page.
   *
   * @param pageId page identifer to unpin
   */
  virtual void Unpin(PageId pageId) = 0;
};

} // namespace persist

#endif /* PERSIST__CORE__BUFFER__REPLACER__BASE_HPP */
