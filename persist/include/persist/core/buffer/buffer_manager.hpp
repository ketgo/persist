/**
 * buffer_manager.hpp - Persist
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

#ifndef PERSIST_CORE_BUFFER_MANAGER_HPP
#define PERSIST_CORE_BUFFER_MANAGER_HPP

#include <persist/core/buffer/replacer/creator.hpp>
#include <persist/core/page/base.hpp>
#include <persist/utility/mutex.hpp>

namespace persist {
/**
 * @brief The buffer manager handles buffer of pages loaded in memory from a
 * backend storage. The reading of pages while wrting of modifed pages are
 * perfromed in compliance with the page repleacement policy.
 *
 * @tparam PageType The type of page managed.
 */
template <class PageType> class BufferManager : public PageObserver {
  static_assert(std::is_base_of<Page, PageType>::value,
                "PageType must be derived from Page class.");

  PERSIST_PRIVATE
  /**
   * @brief Recursive lock for thread safety
   *
   */
  // TODO: Need granular locking
  typedef typename persist::Mutex<std::recursive_mutex> Mutex;
  Mutex lock; //<- lock for achieving thread safety via mutual exclusion
  typedef typename persist::LockGuard<Mutex> LockGuard;

  /**
   * Frame Struct
   *
   * The data structure contains page pointer and status information. A
   * collection of frames make up the memory buffer.
   */
  struct Frame {
    std::unique_ptr<PageType> page;
    bool modified;

    /**
     * @brief Construct a new Frame object
     *
     */
    Frame() : page(nullptr), modified(false) {}
  };

  // Storage *storage;                   //<- opened backend storage
  std::unique_ptr<Replacer> replacer; //<- page replacer

  size_t max_size GUARDED_BY(lock); //<- maximum size of buffer
  typedef typename std::unordered_map<PageId, Frame> Buffer;
  Buffer buffer GUARDED_BY(lock); //<- buffer of page slots
  bool started GUARDED_BY(lock);  //<- flag indicating buffer manager started
};

} // namespace persist

#endif /* PERSIST_CORE_BUFFER_MANAGER_HPP */
