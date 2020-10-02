/**
 * buffer_manager.hpp - Persist
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

#define DEFAULT_MAX_BUFFER_SIZE 10000

namespace persist {

/**
 * DataBlock Buffer Type
 */
typedef std::list<std::unique_ptr<DataBlock>> DataBlockBuffer;

/**
 * Buffer Manager Class
 *
 * The buffer manager is responsible for loading and writing modified data
 * blocks to backend storage. It implements LRU buffer-relplacement policy.
 *
 * - Get block with given identifer
 * - Get block with free space
 * - Create new block if no blocks with free space are available
 * - Track modified blocks and write them to storage
 */
class BufferManager {
private:
  Storage &storage;                            //<- backend storage
  std::unique_ptr<Storage::MetaData> metadata; //<- data block storage metadata
  DataBlockBuffer buffer;                      //<- buffer of data blocks
  std::unordered_map<DataBlockId, DataBlockBuffer::iterator>
      map;          //<- Stores mapped references to data blocks in the buffer
  uint64_t maxSize; //<- maximum size of buffer

  /**
   * Add data block to buffer.
   *
   * @param dataBlock pointer reference to data block
   */
  void put(std::unique_ptr<DataBlock> &dataBlock);

public:
  /**
   * Construct a new Buffer Manager object
   *
   * @param storage reference to backend storage
   * @param maxSize maximum buffer size
   */
  BufferManager(Storage &storage);
  BufferManager(Storage &storage, uint64_t maxSize);

  /**
   * Get a data block with free space. If no such data block is available a new
   * block is created and loaded in buffer.
   *
   * @returns referece to data block in buffer
   */
  DataBlock &get();

  /**
   * Get data block with given ID. The data block is loaded from the backend
   * storage if it is not already found in the buffer. In case the block is not
   * found in the backend then DataBlockNotFoundError exception is raised.
   *
   * @param blockId data block ID
   * @returns referece to data block in buffer
   */
  DataBlock &get(DataBlockId blockId);

  /**
   * Save all modifed data blocks in the buffer to backend storage.
   */
  void flush();
};

} // namespace persist

#endif /* MANAGER_HPP */
