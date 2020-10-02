/**
 * buffer_manager.cpp - Persist
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

#include <persist/buffer_manager.hpp>

namespace persist {

BufferManager::BufferManager(Storage &storage)
    : storage(storage), maxSize(DEFAULT_MAX_BUFFER_SIZE) {
  // Read metadata
  metadata = storage.read();
}

BufferManager::BufferManager(Storage &storage, uint64_t maxSize)
    : storage(storage), maxSize(maxSize) {
  // Read metadata
  metadata = storage.read();
}

void BufferManager::put(std::unique_ptr<DataBlock> &dataBlock) {
  // If buffer is full then remove the LRU data block
  if (buffer.size() == maxSize) {
    // Get last data block in buffer
    std::unique_ptr<DataBlock> &lruBlock = buffer.back();
    DataBlockId lruBlockId = lruBlock->getId();
    // Write to storage if block is updated
    if (lruBlock->isModified()) {
      storage.write(*lruBlock);
    }
    // Remove data block from buffer
    buffer.erase(map[lruBlockId]);
    map.erase(lruBlockId);
  }
  DataBlockId blockId = dataBlock->getId();
  // Check if blockId present in cache
  if (map.find(blockId) == map.end()) {
    // Block ID not present in cache
    buffer.push_front(std::move(dataBlock));
    map[blockId] = buffer.begin();
  } else {
    // Block ID present in cache
    *map[blockId] = std::move(dataBlock);
  }
}

DataBlock &BufferManager::get() {
  // TODO
}

DataBlock &BufferManager::get(DataBlockId blockId) {
  // Check if data block not present in buffer
  if (map.find(blockId) == map.end()) {
    // Load data block from storage
    std::unique_ptr<DataBlock> dataBlock = storage.read(blockId);
    // Insert data block in buffer in accordance with LRU strategy
    put(dataBlock);
  }
  // Move the entry for given blockId to front to accordance with LRU strategy
  buffer.splice(buffer.begin(), buffer, map[blockId]);

  return *(*map[blockId]);
}

void BufferManager::flush() {}

} // namespace persist
