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
#include <persist/core/record_block.hpp>

namespace persist {

/**
 * Record Manager Class
 *
 * The record manager interfaces with page table to GET, INSERT, UPDATE and
 * DELETE records.
 */
class RecordManager {
  PERSIST_PRIVATE
  /**
   * @brief Pointer to backend storage
   */
  std::unique_ptr<Storage> storage;
  /**
   * @brief Page table
   */
  PageTable pageTable;
  /**
   * @brief Flag indicating record manager started
   */
  bool started;

  /**
   * @brief Insert doubly linked record blocks in storage. This method is used
   * for inserting and in-place updating of records stored in backend storage.
   *
   * @param session reference to the started page table session
   * @param span span pointing to the record buffer to store
   * @param location previous record block location. By default this is set to
   * the NULL location
   * @returns starting record block location of the record pointed by span
   */
  RecordBlock::Location
  insert(PageTable::Session &session, Span span,
         RecordBlock::Location location = RecordBlock::Location());

  /**
   * @brief Remove doubly linked record blocks in srorage. This method is used
   * for removing and in-place updating of records stored in backend storage.
   *
   * @param session reference to the started page table session
   * @param location location of the starting doubly linked record block to
   * remove
   */
  void remove(PageTable::Session &session, RecordBlock::Location location);

public:
  /**
   * @brief Construct a new Record Manager object.
   *
   * @param connectionString url containing the type of storage backend and its
   * arguments. The url schema is `<type>://<host>/<path>?<args>`. For example a
   * file storage url looks like `file:///myCollection.db` where the backend
   * uses the file `myCollection.db` in the root folder `/` to store data.
   * @param cacheSize the amount of memory in bytes to use for internal cache.
   */
  RecordManager(std::string connectionString, uint64_t cacheSize);

  /**
   * @brief Start record manager. This opens the backend storage if not already
   * opened.
   */
  void start();

  /**
   * @brief Stop record manager. This closes the backend storage if opened.
   *
   */
  void stop();

  /**
   * @brief Get record stored at given location.
   *
   * @param buffer byte buffer into which the record will be stored
   * @param location record starting location
   */
  void get(ByteBuffer &buffer, RecordBlock::Location location);

  /**
   * @brief Insert record stored in buffer to storage. The method returns the
   * insert location of the record.
   *
   * @param buffer byte buffer containing record data
   * @return RecordLocation inserted location of the record
   */
  RecordBlock::Location insert(ByteBuffer &buffer);

  /**
   * @brief Update record stored at given location.
   *
   * @param buffer byte buffer containing updated record
   * @param location starting location of record
   */
  void update(ByteBuffer &buffer, RecordBlock::Location location);

  /**
   * @brief Remove record stored at given location.
   *
   * @param location starting location of record
   */
  void remove(RecordBlock::Location location);
};

} // namespace persist

#endif /* RECORD_MANAGER_HPP */
