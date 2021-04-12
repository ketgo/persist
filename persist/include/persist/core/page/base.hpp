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

#ifndef PERSIST_CORE_PAGE_BASE_HPP
#define PERSIST_CORE_PAGE_BASE_HPP

#include <set>

#include <persist/core/common.hpp>

namespace persist {

/**
 * @brief Forward declare Page class
 *
 */
class Page;

/**
 * @brief Page Observer
 *
 * Observes modification on page.
 */
class PageObserver {
public:
  /**
   * @brief Handle page modification.
   *
   * @param page Constant reference to the modified page.
   */
  virtual void HandleModifiedPage(const Page &page) = 0;
};

/**
 * @brief Page slot base class. A slot is a block of space inside a page
 * containing complete or part of a data record. Each page type comprises of its
 * own type of slot. However, any type of slot should expose the following
 * interface.
 *
 */
class PageSlot : public virtual Storable {
public:
  /**
   * @brief Get the fixied minimum storage size occupied the slot inside a page.
   *
   * @returns storage size in bytes.
   */
  virtual size_t GetFixedStorageSize() const = 0;
};

/**
 * @brief Page Base Class
 *
 * A Page is a logical chunk of space on a backend storage. The base class
 * exposes interface common to all types of pages.
 *
 */
class Page : public virtual Storable {
  PERSIST_PROTECTED
  /**
   * @brief Set of registered page modification observers.
   */
  std::set<PageObserver *> observers;

  /**
   * @brief Notify all registered observers of page modification.
   */
  void NotifyObservers() {
    for (auto observer : observers) {
      observer->HandleModifiedPage(*this);
    }
  }

public:
  /**
   * @brief Destroy the Page Base object
   *
   */
  virtual ~Page() = default;

  /**
   * @brief Register page modification observer
   *
   * @param observer pointer to page modication observer
   */
  void RegisterObserver(PageObserver *observer) { observers.insert(observer); }

  /**
   * Get page identifier.
   *
   * @returns Page identifier
   */
  virtual const PageId &GetId() const = 0;

  /**
   * @brief Get the storage free space size in the page for specified operation.
   *
   * @param operation Operaion to be performed
   * @returns Free space in bytes
   */
  virtual size_t GetFreeSpaceSize(Operation operation) const = 0;
};

} // namespace persist

#endif /* BASE_HPP */
