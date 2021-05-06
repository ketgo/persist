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

#ifndef PERSIST__CORE__BUFFER__PAGE_HANDLE_HPP
#define PERSIST__CORE__BUFFER__PAGE_HANDLE_HPP

#include <persist/core/buffer/replacer/base.hpp>
#include <persist/core/page/base.hpp>

namespace persist {

// TODO: Pass concurency manager instance to handle for managing concurrent
// access to page.

/**
 * @brief Page Handle Class
 *
 * Page handles are objects used to safely access loaded pages in the buffer.
 * They are essentially pointers but with concurrent access control through
 * concurency control manager. Internally, a page handle object holds a raw
 * pointer to desired page. They utilize RAII by performing the pinning,
 * unpinning and access control operations on construction and destruction.
 * The page can be accessed using the standard -> operator.
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
  bool is_owner;

  /**
   * @brief Unset access ownserhip of the page.
   *
   */
  void Unset() {
    is_owner = false;
    page = nullptr;
    replacer = nullptr;
  }

  /**
   * @brief Acquire access ownership of page.
   *
   */
  void Acquire() {
    // Pin page
    if (page) {
      replacer->Pin(page->GetId());
    }
  }

  /**
   * @brief Release access ownership of page.
   *
   */
  void Release() {
    if (page && is_owner) {
      // Unpin page
      replacer->Unpin(page->GetId());
    }
  }

public:
  /**
   * @brief Construct a new Page Handle object
   *
   */
  PageHandle(PageType *page, Replacer *replacer)
      : page(page), replacer(replacer), is_owner(true) {
    // Acquire access ownership of page
    Acquire();
  }

  /**
   * @brief Move constructor for page handle
   */
  PageHandle(PageHandle &&other)
      : page(other.page), replacer(other.replacer), is_owner(other.is_owner) {
    // Unset ownership of the moved object
    other.Unset();
  }

  /**
   * @brief Get handled pointer to page.
   *
   * @returns Pointer to page.
   */
  PageType *Get() const { return page; }

  /**
   * @brief Move assignment operator. This will relese access ownership of the
   * currently owned page.
   */
  PageHandle &operator=(PageHandle &&other) {
    // Check for moving same object
    if (this != &other) {
      // Release access ownership of the current page.
      Release();

      // Copy members of moved object
      page = other.page;
      replacer = other.replacer;
      is_owner = other.is_owner;

      // Unset access ownership of the moved object
      other.Unset();
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
    Release();
  }

  /**
   * @brief Check if handle is null.
   *
   */
  operator bool() const { return bool(page); }
};

} // namespace persist

#endif /* PERSIST__CORE__BUFFER__PAGE_HANDLE_HPP */
