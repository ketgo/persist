/**
 * base.hpp - Persist
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
 * Backend Storage Interface
 *
 * The header file exposes interface to implement non-volatile backend storage.
 */

#ifndef STORAGE_BASE_HPP
#define STORAGE_BASE_HPP

#include <list>
#include <memory>

#include <persist/block.hpp>
#include <persist/common.hpp>

namespace persist {
/**
 * Storage Abstract Class
 *
 * Exposes interface to open and close a backend storage.
 */
class Storage {
public:
  /**
   * Storage MetaData
   *
   * The metadata class contains the block size and free block Ids
   * in the backend storage. This information is utilized by the data
   * block manager for efficient handling of data block lifecycle.
   *
   */
  class MetaData : public Serializable {
  public:
    uint64_t blockSize;
    std::list<DataBlockId> freeBlocks;

    /**
     * Constructor
     */
    MetaData() {}

    /**
     * Load object from byte string
     *
     * @param input input buffer to load
     */
    void load(ByteBuffer &input);

    /**
     * Dump object as byte string
     *
     * @returns reference to the buffer with results
     */
    ByteBuffer &dump();
  };

  virtual ~Storage() {} //<- Virtual destructor

  /**
   * Read storage metadata information. In case no metadata
   * information is available a null pointer is returned.
   *
   * Note: Some storage types dont store metadata as it is not
   * needed by the underlying implementation.
   *
   * @return pointer to MetaData object
   */
  virtual std::unique_ptr<MetaData> read() = 0;

  /**
   * Read DataBlock with given identifier from storage.
   *
   * @param blockId block identifier
   * @returns pointer to requested DataBlock object
   */
  virtual std::unique_ptr<DataBlock> read(DataBlockId blockId) = 0;

  /**
   * Write MetaData object to storage.
   *
   * @param metadata reference to MetaData object to be written
   */
  virtual void write(MetaData &metadata) = 0;

  /**
   * Write DataBlock object to storage.
   *
   * @param block reference to DataBlock object to be written
   */
  virtual void write(DataBlock &block) = 0;
};

} // namespace persist

#endif /* STORAGE_BASE_HPP */
