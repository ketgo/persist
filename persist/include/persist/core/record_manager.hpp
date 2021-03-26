/**
 * record_manager.hpp - Persist
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

#ifndef PERSIST_CORE_RECORDMANAGER_HPP
#define PERSIST_CORE_RECORDMANAGER_HPP

#include <persist/core/page/record_page/slot.hpp>
#include <persist/core/transaction/transaction.hpp>

namespace persist {
/**
 * @brief Record location type.
 *
 */
typedef RecordPageSlot::Location RecordLocation;

/**
 * @brief Record manager abstract base class. Each collection should implement
 * the interfaces defined in the base class.
 *
 * @tparam RecordType Data record type.
 */
template <class RecordType> class RecordManager {
  // Record should be storage
  static_assert(std::is_base_of<Storable, RecordType>::value,
                "RecordType must be derived from persist::Storable");

public:
  /**
   * @brief Destroy the RecordManager object.
   *
   */
  virtual ~RecordManager() = default;

  /**
   * @brief Start record manager.
   *
   */
  virtual void Start() = 0;

  /**
   * @brief Stop record manager.
   *
   */
  virtual void Stop() = 0;

  /**
   * @brief Get record stored at given location.
   *
   * @param record Reference to the record to get.
   * @param location Location of the stored record.
   * @param txn Reference to an active transaction.
   */
  virtual void Get(RecordType &record, RecordLocation location,
                   Transaction &txn) = 0;

  /**
   * @brief Insert record stored in buffer to storage. The method returns the
   * inserted location of the record.
   *
   * @param record Reference to the record to insert.
   * @param txn Reference to an active transaction.
   * @returns Location of the inserted record.
   */
  virtual RecordLocation Insert(RecordType &record, Transaction &txn) = 0;

  /**
   * @brief Update record stored at given location.
   *
   * @param record Reference to the updated record.
   * @param location Location of the record.
   * @param txn Reference to an active transaction.
   */
  virtual void Update(RecordType &record, RecordLocation location,
                      Transaction &txn) = 0;

  /**
   * @brief Delete record stored at given location.
   *
   * @param location Location of the record to delete.
   * @param txn Reference to an active transaction.
   */
  virtual void Delete(RecordLocation location, Transaction &txn) = 0;
};

} // namespace persist

#endif /* PERSIST_CORE_RECORDMANAGER_HPP */
