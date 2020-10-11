/**
 * record_manager.cpp - Persist
 *
 * Copyright 2020 Ketan Goyal
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

#include <persist/core/record_manager.hpp>

namespace persist {

static uint64_t cachesizeToBufferCount(uint64_t cacheSize) {
  // TODO: Calculate page table max size from given cache size
  return cacheSize;
}

RecordManager::RecordManager(std::string storageURL, uint64_t cacheSize)
    : storage(Storage::create(storageURL)),
      table(*storage, cachesizeToBufferCount(cacheSize)) {}

void RecordManager::start() {
  if (!storage->is_open()) {
    storage->open();
  }
}

void RecordManager::stop() {
  if (storage->is_open()) {
    storage->close();
  }
}

} // namespace persist
