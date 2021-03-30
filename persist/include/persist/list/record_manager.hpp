/**
 * list/record_manager.hpp - Persist
 *
 * Copyright 2021 Ketan Goyal
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

#ifndef PERSIST_LIST_RECORD_MANAGER_HPP
#define PERSIST_LIST_RECORD_MANAGER_HPP

#include <persist/core/common.hpp>
#include <persist/core/record_manager.hpp>

namespace persist {

/**
 * @brief List collection record manager.
 *
 * @tparam RecordType Data record type.
 * @tparam ReplacerType The type of page replacer to be used by buffer manager.
 * Default set to LRUReplacer.
 * @tparam FreeSpaceManagerType The type of free space manager. Default set to
 * FSLManager.
 */
template <class RecordType, class ReplacerType = LRUReplacer,
          class FreeSpaceManagerType = FSLManager>
class ListRecordManager
    : public RecordManager<RecordType, ReplacerType, FreeSpaceManagerType> {
public:
  /**
   * @brief Get record stored at given location.
   *
   * @param record Reference to the record to get.
   * @param location Location of the stored record.
   * @param txn Reference to an active transaction.
   */
  void Get(RecordType &record, RecordLocation location, Transaction &txn) {}

  /**
   * @brief Insert record stored in buffer to storage. The method returns the
   * inserted location of the record.
   *
   * @param record Reference to the record to insert.
   * @param txn Reference to an active transaction.
   * @returns Location of the inserted record.
   */
  RecordLocation Insert(RecordType &record, Transaction &txn) {}

  /**
   * @brief Update record stored at given location.
   *
   * @param record Reference to the updated record.
   * @param location Location of the record.
   * @param txn Reference to an active transaction.
   */
  void Update(RecordType &record, RecordLocation location, Transaction &txn) {}

  /**
   * @brief Delete record stored at given location.
   *
   * @param location Location of the record to delete.
   * @param txn Reference to an active transaction.
   */
  void Delete(RecordLocation location, Transaction &txn) {}
};

} // namespace persist

#endif /* PERSIST_LIST_RECORD_MANAGER_HPP */
