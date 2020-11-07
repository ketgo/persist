/**
 * ops_manager.hpp - Persist
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

#ifndef OPS_MANAGER_HPP
#define OPS_MANAGER_HPP

#include <string>
#include <type_traits>

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>
#include <persist/core/record_manager.hpp>

namespace persist {

/**
 * Operations Manager
 *
 * Operations manager is responsible for handling the operations on
 * collections. The operations are performed under transactions and
 * logging in order to support ACID requirements.
 *
 */
template <class RecordManagerType> class OpsManager {
  PERSIST_PRIVATE
  /**
   * @brief Collection specific record manager
   */
  static_assert(std::is_base_of<RecordManager, RecordManagerType>::value,
                "RecordManagerType must be derived from RecordManager.");
  RecordManagerType recordManager;

  /**
   * @brief Flag indicating ops manager started
   */
  bool started;

public:
  /**
   * @brief Construct a new Ops Manager object.
   *
   * @param pageTable reference to an opened page table
   */
  OpsManager(PageTable &pageTable) : recordManager(pageTable), started(false) {}

  /**
   * @brief Start ops manager.
   */
  void start() {
    if (!started) {
      recordManager.start();
      started = true;
    }
  }

  /**
   * @brief Stop ops manager.
   *
   */
  void stop() {
    if (!started) {
      recordManager.stop();
      started = false;
    }
  }

  /**
   * @brief Get record stored at given location.
   *
   * @param buffer byte buffer into which the record will be stored
   * @param location record starting location
   */
  void get(ByteBuffer &buffer, RecordLocation location) {
    // Check if ops manager has started
    if (!started) {
      throw OpsManagerNotStartedError();
    }
    recordManager.get(buffer, location);
  }

  /**
   * @brief Insert record stored in buffer to storage. The method returns the
   * insert location of the record.
   *
   * @param buffer byte buffer containing record data
   * @returns inserted location of the record
   */
  RecordLocation insert(ByteBuffer &buffer) {
    // Check if ops manager has started
    if (!started) {
      throw OpsManagerNotStartedError();
    }
    return recordManager.insert(buffer);
  }

  /**
   * @brief Update record stored at given location.
   *
   * @param buffer byte buffer containing updated record
   * @param location starting location of record
   */
  void update(ByteBuffer &buffer, RecordLocation location) {
    // Check if ops manager has started
    if (!started) {
      throw OpsManagerNotStartedError();
    }
    recordManager.update(buffer, location);
  }

  /**
   * @brief Remove record stored at given location.
   *
   * @param location starting location of record
   */
  void remove(RecordLocation location) {
    // Check if ops manager has started
    if (!started) {
      throw OpsManagerNotStartedError();
    }
    recordManager.remove(location);
  }
};

} // namespace persist

#endif /* OPS_MANAGER_HPP */
