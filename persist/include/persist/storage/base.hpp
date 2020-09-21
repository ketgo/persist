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

#include <memory>
#include <persist/block.hpp>

namespace persist {
/**
 * Storage Abstract Class
 *
 * Exposes interface to open and close a backend storage.
 */
class Storage {
public:
  virtual ~Storage() {} //<- Virtual destructor
  
  /**
   * Open storage.
   */
  virtual void open() = 0;

  /**
   * Close storage.
   */
  virtual void close() = 0;

  /**
   * Create a new Block.
   *
   * @returns pointer to new Block object
   */
  virtual std::unique_ptr<DataBlock> create() = 0;

  /**
   * Load Block with given identifier.
   *
   * @param blockId block identifier
   * @returns pointer to requested Block object
   */
  virtual std::unique_ptr<DataBlock> load(DataBlockId blockId) = 0;

  /**
   * Dump block to storage
   *
   * @param block reference to block object to be written
   */
  virtual void dump(DataBlock &block) = 0;
};
} // namespace persist

#endif /* STORAGE_BASE_HPP */
