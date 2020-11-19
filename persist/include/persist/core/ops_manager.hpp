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
#include <persist/core/log_manager.hpp>
#include <persist/core/page_table.hpp>
#include <persist/core/record_manager.hpp>
#include <persist/core/transaction.hpp>
#include <persist/core/utility.hpp>

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
   * @brief Collection page table
   */
  PageTable pageTable;

  /**
   * @brief Log manager
   */
  LogManager logManager;

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
  // TODO: Convert cache size in MB to  page buffer size.
  OpsManager(Storage &storage, uint64_t cacheSize = DEFAULT_CACHE_SIZE)
      : pageTable(storage, cacheSize), recordManager(pageTable),
        started(false) {}

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
   */
  void stop() {
    if (!started) {
      recordManager.stop();
      started = false;
    }
  }

  /**
   * @brief Create a transaction.
   *
   * @returns a new transaction object
   */
  Transaction createTransaction() {
    return Transaction(pageTable, logManager, uuid::generate());
  }

  /**
   * @brief Get record stored at given location. An optional pointer to an
   * active transaction can also be provided. In case an active transaction is
   * provided then the commit cycle is left for the user to complete.
   *
   * @param buffer byte buffer into which the record will be stored
   * @param location record starting location
   * @param transaction currently active transaction to use. Defaults to null in
   * which case a new transaction is created for the operation.
   */
  void get(ByteBuffer &buffer, RecordLocation location,
           Transaction *transaction = nullptr) {
    // Check if ops manager has started
    if (!started) {
      throw OpsManagerNotStartedError();
    }
    if (transaction == nullptr) {
      Transaction txn = createTransaction();
      recordManager.get(txn, buffer, location);
      txn.commit();
    } else {
      recordManager.get(*transaction, buffer, location);
    }
  }

  /**
   * @brief Insert record stored in buffer to storage. The method returns the
   * insert location of the record. An optional pointer to an active transaction
   * can also be provided. In case an active transaction is provided then the
   * commit cycle is left for the user to complete.
   *
   * @param buffer byte buffer containing record data
   * @returns inserted location of the record
   * @param transaction currently active transaction to use. Defaults to null in
   * which case a new transaction is created for the operation.
   */
  RecordLocation insert(ByteBuffer &buffer,
                        Transaction *transaction = nullptr) {
    // Check if ops manager has started
    if (!started) {
      throw OpsManagerNotStartedError();
    }
    RecordLocation location;
    if (transaction == nullptr) {
      Transaction txn = createTransaction();
      location = recordManager.insert(txn, buffer);
      txn.commit();
    } else {
      location = recordManager.insert(*transaction, buffer);
    }

    return location;
  }

  /**
   * @brief Update record stored at given location. The method returns the
   * insert location of the record. An optional pointer to an active transaction
   * can also be provided. In case an active transaction is provided then the
   * commit cycle is left for the user to complete.
   *
   * @param buffer byte buffer containing updated record
   * @param location starting location of record
   * @param transaction currently active transaction to use. Defaults to null in
   * which case a new transaction is created for the operation.
   */
  void update(ByteBuffer &buffer, RecordLocation location,
              Transaction *transaction = nullptr) {
    // Check if ops manager has started
    if (!started) {
      throw OpsManagerNotStartedError();
    }
    if (transaction == nullptr) {
      Transaction txn = createTransaction();
      recordManager.update(txn, buffer, location);
      txn.commit();
    } else {
      recordManager.update(*transaction, buffer, location);
    }
  }

  /**
   * @brief Remove record stored at given location. The method returns the
   * insert location of the record. An optional pointer to an active transaction
   * can also be provided. In case an active transaction is provided then the
   * commit cycle is left for the user to complete.
   *
   * @param location starting location of record
   * @param transaction currently active transaction to use. Defaults to null in
   * which case a new transaction is created for the operation.
   */
  void remove(RecordLocation location, Transaction *transaction = nullptr) {
    // Check if ops manager has started
    if (!started) {
      throw OpsManagerNotStartedError();
    }
    if (transaction == nullptr) {
      Transaction txn = createTransaction();
      recordManager.remove(txn, location);
      txn.commit();
    } else {
      recordManager.remove(*transaction, location);
    }
  }
};

} // namespace persist

#endif /* OPS_MANAGER_HPP */
