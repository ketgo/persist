/**
 * record_manager.hpp - Persist
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

#ifndef RECORD_MANAGER_HPP
#define RECORD_MANAGER_HPP

#include <persist/core/defs.hpp>
#include <persist/core/page_table.hpp>
#include <persist/core/storage/base.hpp>

namespace persist {

/**
 * @brief Typedef for record location on backend storage
 */
typedef RecordBlock::Location RecordLocation;

/**
 * Record Manager Class
 *
 * The record manager interfaces with page table to GET, INSERT, UPDATE and
 * DELETE records.
 */
class RecordManager {
  PERSIST_PROTECTED
  /**
   * @brief Page table
   *
   */
  PageTable &pageTable;

  /**
   * @brief Flag indicating record manager started
   */
  bool started;

public:
  /**
   * @brief Construct a new Record Manager object.
   *
   * @param pageTable reference to an opened page table.
   */
  RecordManager(PageTable &pageTable) : pageTable(pageTable), started(false) {}
  virtual ~RecordManager() {}

  /**
   * @brief Start record manager. This opens the page table.
   *
   */
  void start() {
    if (!started) {
      pageTable.open();
      started = true;
    }
  }

  /**
   * @brief Stop record manager. This closes the page table.
   *
   */
  void stop() {
    if (started) {
      pageTable.close();
      started = false;
    }
  }

  /**
   * @brief Get record stored at given location.
   *
   * @param buffer byte buffer into which the record will be stored
   * @param location record starting location
   */
  virtual void get(ByteBuffer &buffer, RecordLocation location) = 0;

  /**
   * @brief Insert record stored in buffer to storage. The method returns the
   * insert location of the record.
   *
   * @param buffer byte buffer containing record data
   * @returns inserted location of the record
   */
  virtual RecordLocation insert(ByteBuffer &buffer) = 0;

  /**
   * @brief Update record stored at given location.
   *
   * @param buffer byte buffer containing updated record
   * @param location starting location of record
   */
  virtual void update(ByteBuffer &buffer, RecordLocation location) = 0;

  /**
   * @brief Remove record stored at given location.
   *
   * @param location starting location of record
   */
  virtual void remove(RecordLocation location) = 0;
};

} // namespace persist

#endif /* RECORD_MANAGER_HPP */
