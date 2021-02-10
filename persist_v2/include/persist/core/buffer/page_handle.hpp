/**
 * page_handle.hpp - Persist
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

#ifndef PAGE_HANDLE_HPP
#define PAGE_HANDLE_HPP

#include <persist/core/buffer/replacer/base.hpp>
#include <persist/core/page/base.hpp>

namespace persist {

// TODO: Pass read-write lock for page from buffer manager.

/**
 * @brief Page Handle Class
 *
 * Page handles are objects used to safely access loaded pages in the buffer.
 * They are essentially pointers but with concurrent access control through a
 * read-write lock. Internally, a page handle object holds a raw pointer to
 * desired page. It performs the pinning and locking operation upon construction
 * while unpinning and unlocking operations upon destruction. The page can be
 * accessed using the standard -> operator.
 *
 * @tparam PageType type of page handled by the class
 */
template <class PageType> class PageHandle {
  static_assert(std::is_base_of<Page, PageType>::value,
                "PageType must be derived from Page class.");

  PERSIST_PRIVATE

  /**
   * @brief Pointer to loaded page in buffer
   *
   */
  PageType *page;

  /**
   * @brief Pointer to page replacer
   *
   */
  Replacer *replacer;

  /**
   * @brief Flag to indicate handle has ownership
   *
   */
  bool isOwner;

  /**
   * @brief Unset access ownserhip of the page.
   *
   */
  void unset() {
    isOwner = false;
    page = nullptr;
    replacer = nullptr;
  }

  /**
   * @brief Acquire access ownership of page.
   *
   */
  void acquire() {
    // Pin page
    replacer->pin(page->getId());
  }

  /**
   * @brief Release access ownership of page.
   *
   */
  void release() {
    if (isOwner) {
      // Unpin page
      replacer->unpin(page->getId());
    }
  }

public:
  /**
   * @brief Construct a new Page Handle object
   *
   */
  PageHandle(PageType *page, Replacer *replacer)
      : page(page), replacer(replacer), isOwner(true) {
    // Acquire access ownership of page
    acquire();
  }

  /**
   * @brief Move constructor for page handle
   */
  PageHandle(PageHandle &&other)
      : page(other.page), replacer(other.replacer), isOwner(other.isOwner) {
    // Unset ownership of the moved object
    other.unset();
  }

  /**
   * @brief Move assignment operator
   */
  PageHandle &operator=(PageHandle &&other) {
    // Check for moving same object
    if (this != &other) {
      // Release access ownership of the current page.
      if (isOwner) {
        // Unpin page
        replacer->unpin(page->getId());
      }

      // Copy members of moved object
      page = other.page;
      replacer = other.replacer;
      isOwner = other.isOwner;

      // Unset access ownership of the moved object
      other.unset();
    }

    return *this;
  }

  /**
   * @brief Page access operator
   */
  PageType *operator->() const { return page; }

  /**
   * @brief Destroy the Page Handle object
   */
  ~PageHandle() {
    // Release access ownership of page
    release();
  }
};

} // namespace persist

#endif /* PAGE_HANDLE_HPP */
