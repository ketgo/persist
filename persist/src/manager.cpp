/**
 * manager.cpp - Persist
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

#include <persist/exceptions.hpp>
#include <persist/manager.hpp>

namespace persist {

/****************************
 * LRU Cache
 ****************************/

BlockManager::LRUCache::LRUCache(uint64_t cacheSize, BlockManager &manager)
    : cacheSize(cacheSize), manager(manager) {}

BlockManager::LRUCache::Entry &
BlockManager::LRUCache::get(DataBlockId blockId) {
  // Check if block present in cache
  if (map.find(blockId) == map.end()) {
    throw DataBlockNotFoundError(blockId);
  }
  // Move the entry for given blockId to front
  entries.splice(entries.begin(), entries, map[blockId]);

  return *map[blockId];
}

void BlockManager::LRUCache::put(DataBlockId blockId, Entry &entry) {
  // Check if cache is full
  if (entries.size() == cacheSize) {
    // TODO: Pop the last element from list and save it on storage if updated.
    // Also remove entry from map.
    Entry &last = entries.back();
    DataBlockId _blockId = last.block->getId();
    // Write to storage if block is updated
    if (last.updated) {
      manager.storage.write(*last.block);
    }
    entries.erase(map[_blockId]);
    map.erase(_blockId);
  }

  // Check if blockId present in cache
  if (map.find(blockId) == map.end()) {
    // Block ID not present in cache
    entries.push_front(std::move(entry));
    map[blockId] = entries.begin();
  } else {
    // Block ID present in cache
    map[blockId]->block = std::move(entry.block);
    map[blockId]->updated = entry.updated;
  }
}

/****************************
 * Block Manager
 ****************************/

BlockManager::BlockManager(Storage &storage, uint64_t blockSize)
    : storage(storage), blockSize(blockSize),
      cache(DEFAULT_MAX_CACHE_SIZE, *this) {}

BlockManager::BlockManager(Storage &storage, uint64_t blockSize,
                           uint64_t cacheSize)
    : storage(storage), blockSize(blockSize), cache(cacheSize, *this) {}

} // namespace persist
