/**
 * base.hpp - Persist
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

#ifndef PERSIST_CORE_BUFFER_BASE_HPP
#define PERSIST_CORE_BUFFER_BASE_HPP

#include <persist/core/buffer/page_handle.hpp>
#include <persist/core/page/base.hpp>

namespace persist {

/**
 * @brief Buffer manager abstract base class.
 *
 * @tparam PageType The type of page managed.
 */
template <class PageType>
class BufferManagerBase : public virtual PageObserver {
  static_assert(std::is_base_of<Page, PageType>::value,
                "PageType must be derived from Page class.");

public:
  /**
   * @brief Destroy the Buffer Manager object
   *
   */
  virtual ~BufferManagerBase() = default;

  /**
   * @brief Start buffer manager.
   *
   * @thread_safe
   *
   */
  virtual void Start() = 0;

  /**
   * @brief Stop buffer manager.
   *
   * @thread_safe
   *
   */
  virtual void Stop() = 0;

  /**
   * Get page with given ID. The page is loaded from the backend storage if it
   * is not already found in the buffer. In case the page is not found in the
   * backend storage a PageNotFoundError exception is raised.
   *
   * @thread_safe
   *
   * @param page_id Page identifer.
   * @returns Page handle object
   */
  virtual PageHandle<PageType> Get(PageId page_id) = 0;

  /**
   * Get a new page. The method creates a new page by allocating space in
   * backend storage and loads it into buffer.
   *
   * @thread_safe
   *
   * @returns Page handle object
   */
  virtual PageHandle<PageType> GetNew() = 0;

  /**
   * Dump a single page to backend storage if modified and unpinned.
   *
   * @thread_safe
   *
   * @param page_id page identifer
   * @returns `true` if page is flushed else `false`
   */
  virtual bool Flush(PageId page_id) = 0;

  /**
   * @brief Dump all modified and unpinned pages to backend storage.
   *
   * @thread_safe
   */
  virtual void FlushAll() = 0;
};
} // namespace persist

#endif /* PERSIST_CORE_BUFFER_BASE_HPP */
