/**
 * manager.hpp - Persist
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

/**
 * Block Manager Header
 */

#ifndef MANAGER_HPP
#define MANAGER_HPP

#include <list>
#include <memory>
#include <unordered_map>

#include <persist/data_block.hpp>
#include <persist/storage/base.hpp>

#define DEFAULT_MAX_CACHE_SIZE 10000

namespace persist {
/**
 * Block Manager Class
 */
class BlockManager {
public:
  /**
   * Least Recently Used Cache
   *
   * The cache is used by the manager for efficient handling of read write
   * operations on data blocks.
   */
  class LRUCache {
  public:
    /**
     * Cache Entry Struct
     */
    struct Entry {
      // Pointer to data block
      std::unique_ptr<DataBlock> block;
      // Flag indicating if the data block got updated
      bool updated;
    };

  private:
    // List of of cached entries
    std::list<Entry> entries;
    // Stores mapped references to cached entries
    std::unordered_map<DataBlockId, std::list<Entry>::iterator> map;
    // Maximum capacity of cache
    uint64_t cacheSize;
    // Reference to block manager
    BlockManager &manager;

  public:
    /**
     * Construct a new LRUCache object
     *
     * @param cacheSize maximum cache size
     * @param manager reference to block manager
     */
    LRUCache(uint64_t cacheSize, BlockManager &manager);

    /**
     * Get entry for given data block ID
     *
     * @param blockId data block ID
     * @returns referece to cached entry
     */
    Entry &get(DataBlockId blockId);

    /**
     * Put data block ID and entry into cache.
     *
     * @param blockId data block ID
     * @param entry cache entry
     */
    void put(DataBlockId blockId, Entry &entry);
  };

private:
  Storage &storage;   //<- backend storage
  uint64_t blockSize; //<- data block size
  LRUCache cache;     //<- data block cache

public:
  /**
   * Construct a new Block Manager object
   *
   * @param storage reference to backend storage
   * @param blockSize data block size
   * @param cacheSize maximum cache size
   */
  BlockManager(Storage &storage, uint64_t blockSize);
  BlockManager(Storage &storage, uint64_t blockSize, uint64_t cacheSize);
};

} // namespace persist

#endif /* MANAGER_HPP */
