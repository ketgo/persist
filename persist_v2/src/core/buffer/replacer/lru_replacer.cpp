/**
 * lru_replacer.cpp - Persist
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

#include <persist/core/buffer/replacer/lru_replacer.hpp>

namespace persist {

void LRUReplacer::track(PageId pageId) {
  // Check if pageId exists in cache
  if (position.find(pageId) == position.end()) {
    // Insert value in cache
    cache.push_front({pageId, 0});
    // Save position of value in cache
    position[pageId] = cache.begin();
  }
}

void LRUReplacer::forget(PageId pageId) {
  cache.erase(position.at(pageId));
  position.erase(pageId);
}

PageId LRUReplacer::getVictumId() {
  // Looks for LRU page ID having 0 valued pin count
  for (auto i = cache.rbegin(); i != cache.rend(); ++i) {
    if (i->pinCount == 0) {
      return i->pageId;
    }
  }
  return 0;
}

void LRUReplacer::pin(PageId pageId) {
  // Increase reference count for page ID
  position.at(pageId)->pinCount += 1;
  // Move the frame for given page ID to front in accordance with LRU strategy
  cache.splice(cache.begin(), cache, position.at(pageId));
};

void LRUReplacer::unpin(PageId pageId) { position.at(pageId)->pinCount -= 1; }

} // namespace persist