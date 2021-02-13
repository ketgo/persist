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

#ifndef PAGE_BASE_HPP
#define PAGE_BASE_HPP

#include <list>

#include <persist/core/defs.hpp>

namespace persist {

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
   * @param pageId ID of the page modified
   */
  virtual void handleModifiedPage(PageId pageId) = 0;
};

/**
 * @brief Page Base Class
 *
 * A Page is a logical chunk of space on a backend storage. The base class
 * exposes interface common to all types of pages.
 */
class Page {
  PERSIST_PROTECTED
  /**
   * @brief List of registered page modification observers
   */
  std::list<PageObserver *> observers;

  /**
   * @brief Notify all registered observers of page modification.
   */
  void notifyObservers() {
    for (auto observer : observers) {
      observer->handleModifiedPage(getId());
    }
  }

public:
  /**
   * @brief Enumerated list of operation that can be performed on page data.
   *
   */
  enum class Operation { INSERT, UPDATE, DELETE };

  /**
   * @brief Destroy the Page object
   *
   */
  virtual ~Page() {}

  /**
   * @brief Register page modification observer
   *
   * @param observer pointer to page modication observer
   */
  void registerObserver(PageObserver *observer) {
    observers.insert(observers.end(), observer);
  }

  /**
   * Get page ID.
   *
   * @returns page identifier
   */
  virtual const PageId &getId() const = 0;

  /**
   * Get free space in bytes available in the page.
   *
   * @param operation The type of page operation for which free space is
   * requested.
   * @returns free space available in page
   */
  virtual uint64_t freeSpace(Operation operation) = 0;

  /**
   * Load Block object from byte string.
   *
   * @param input input buffer span to load
   */
  virtual void load(Span input) = 0;

  /**
   * Dump Block object as byte string.
   *
   * @param output output buffer span to dump
   */
  virtual void dump(Span output) = 0;
};

} // namespace persist

#endif /* PAGE_BASE_HPP */
