/**
 * lru_replacer.hpp - Persist
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

#ifndef PERSIST_CORE_BUFFER_LRU_REPLACER_HPP
#define PERSIST_CORE_BUFFER_LRU_REPLACER_HPP

#include <list>
#include <unordered_map>

#include <persist/core/buffer/replacer/base.hpp>
#include <persist/utility/mutex.hpp>

namespace persist {

/**
 * @brief LRU Replacer
 *
 * This replacer detects victum page ID using the LRU replacement algorithm.
 */
class LRUReplacer : public Replacer {
  PERSIST_PRIVATE
  // TODO: Need granular locking
  typedef typename persist::Mutex<std::recursive_mutex> Mutex;
  Mutex lock; //<- lock for achieving thread safety via mutual exclusion
  typedef typename persist::LockGuard<Mutex> LockGuard;

  /**
   * @brief Node
   *
   * The data structure stores the page ID and pin count. The value of pin count
   * determins the number of times the page associated with stored ID is pinned.
   */
  struct Frame {
    PageId page_id;
    uint64_t pin_count;
  };

  /**
   * @brief Cache of frames
   *
   */
  std::list<Frame> cache GUARDED_BY(lock);

  /**
   * @brief Maps page ID to postion of associated frame in the cache
   *
   */
  typedef std::list<Frame>::iterator Position;
  std::unordered_map<PageId, Position> position GUARDED_BY(lock);

public:
  /**
   * @brief Track page ID for detecting victum page.
   *
   * @param page_id page identifer to remember
   */
  void Track(PageId page_id) override {
    LockGuard guard(lock);

    // Check if page_id does not exist in cache
    if (position.find(page_id) == position.end()) {
      // Insert value in cache
      cache.push_front({page_id, 0});
      // Save position of value in cache
      position[page_id] = cache.begin();
    }
  }

  /**
   * @brief Forget page ID for detecting victum page.
   *
   * @param page_id page identifer to forget
   */
  void Forget(PageId page_id) override {
    LockGuard guard(lock);

    cache.erase(position.at(page_id));
    position.erase(page_id);
  }

  /**
   * @brief Get the Victum page Id. This is the page that can be replaced by the
   * buffer manager. In case no replacement page ID is found then 0 is returned.
   *
   * @return PageId identifier of the victum page
   */
  PageId GetVictumId() override {
    LockGuard guard(lock);

    // Looks for LRU page ID having 0 valued pin count
    for (auto i = cache.rbegin(); i != cache.rend(); ++i) {
      if (i->pin_count == 0) {
        return i->page_id;
      }
    }
    return 0;
  }

  /**
   * @brief Pin page ID. A pinned ID indicates the associated page is being
   * referenced by an external process and thus should be skipped while
   * detecting victum page ID.
   *
   * @param page_id page identifer to pin
   */
  void Pin(PageId page_id) override {
    LockGuard guard(lock);

    // Increase reference count for page ID
    position.at(page_id)->pin_count += 1;
    // Move the frame for given page ID to front in accordance with LRU strategy
    cache.splice(cache.begin(), cache, position.at(page_id));
  }

  bool IsPinned(PageId page_id) override {
    LockGuard guard(lock);

    return position.at(page_id)->pin_count > 0;
  }

  /**
   * @brief Unpin page ID. This notifies the replacer that the page with given
   * ID is not being referenced by an external process anymore. Note that the
   * page can still be referenced by some other external process in
   * multi-threaded settings. The replacer is free to select the ID as victum
   * only when all the external processeses stopped referencing the page.
   *
   * @param page_id page identifer to unpin
   */
  void Unpin(PageId page_id) override {
    LockGuard guard(lock);

    position.at(page_id)->pin_count -= 1;
  }
};

} // namespace persist

#endif /* PERSIST_CORE_BUFFER_LRU_REPLACER_HPP */
