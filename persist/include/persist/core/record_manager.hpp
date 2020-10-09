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

#include <memory>
#include <string>

#include <persist/core/page_table.hpp>

namespace persist {

/**
 * Record Location
 */
struct RecordLocation {
  PageId pageId;
  PageSlotId slotId;
};

/**
 * Record Manager Class
 *
 * The record manager interfaces with page table to GET, INSERT, UPDATE and
 * DELETE records.
 */
class RecordManager {
private:
  std::unique_ptr<Storage> storage;
  std::unique_ptr<PageTable> table;

public:
  /**
   * @brief Construct a new Record Manager object
   */
  RecordManager() {}

  /**
   * @brief Get record stored at given location.
   *
   * @param buffer string buffer into which the record will be stored
   * @param location record starting location
   */
  void get(std::string &buffer, RecordLocation location);

  /**
   * @brief Insert record stored in buffer to storage. The method returns the
   * insert location of the record.
   *
   * @param buffer string buffer containing record data
   * @return RecordLocation inserted location of the record
   */
  RecordLocation insert(std::string &buffer);

  /**
   * @brief Update record stored at given location.
   *
   * @param buffer buffer containing updated record
   * @param location starting location of record
   */
  void update(std::string &buffer, RecordLocation location);

  /**
   * @brief Remove record stored at given location.
   *
   * @param location starting location of record
   */
  void remove(RecordLocation location);
};

} // namespace persist

#endif /* RECORD_MANAGER_HPP */
