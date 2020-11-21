/**
 * collection.hpp - Persist
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

#ifndef COLLECTION_HPP
#define COLLECTION_HPP

#include <memory>

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>
#include <persist/core/log_manager.hpp>
#include <persist/core/page_table.hpp>
#include <persist/core/record_manager.hpp>
#include <persist/core/transaction.hpp>
#include <persist/core/transaction_manager.hpp>
#include <persist/core/utility.hpp>

namespace persist {

/**
 * Collection Base Class
 *
 * The collection base class contains data members and methods common for all
 * types of collections.
 */
template <class RecordManagerType> class Collection {
  PERSIST_PRIVATE
  /**
   * @brief Pointer to backend storage
   */
  std::unique_ptr<Storage> storage;

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
   * @brief Flag indicating if the collection is open
   */
  bool opened;

  PERSIST_PROTECTED

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
    if (!opened) {
      throw CollectionNotOpenError();
    }
    if (transaction == nullptr) {
      Transaction txn = transactionManager.begin();
      recordManager.get(txn, buffer, location);
      transactionManager.commit(txn);
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
    if (!opened) {
      throw CollectionNotOpenError();
    }
    RecordLocation location;
    if (transaction == nullptr) {
      Transaction txn = transactionManager.begin();
      location = recordManager.insert(txn, buffer);
      transactionManager.commit(txn);
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
    // Check if collection is opened
    if (!opened) {
      throw CollectionNotOpenError();
    }
    if (transaction == nullptr) {
      Transaction txn = transactionManager.begin();
      recordManager.update(txn, buffer, location);
      transactionManager.commit(txn);
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
    // Check if collection is opened
    if (!opened) {
      throw CollectionNotOpenError();
    }
    if (transaction == nullptr) {
      Transaction txn = transactionManager.begin();
      recordManager.remove(txn, location);
      transactionManager.commit(txn);
    } else {
      recordManager.remove(*transaction, location);
    }
  }

public:
  /**
   * @brief Transaction manager
   */
  TransactionManager transactionManager;

  /**
   * @brief Construct a new Collection object
   *
   * @param connectionString url containing the type of storage backend and its
   * arguments. The url schema is `<type>://<host>/<path>?<args>`. For example a
   * file storage url looks like `file:///myCollection.db` where the backend
   * uses the file `myCollection.db` in the root folder `/` to store data.
   * @param cacheSize the amount of memory in bytes to use for internal cache.
   */
  // TODO: Convert cache size in MB to  page buffer size.
  Collection(std::string connectionString,
             uint64_t cacheSize = DEFAULT_CACHE_SIZE)
      : storage(Storage::create(connectionString)),
        pageTable(*storage, cacheSize), recordManager(pageTable),
        transactionManager(pageTable, logManager), opened(false) {}

  /**
   *Open the collection. This method starts the record manager which in turn
   *sets up the connection with backend storage, e.g. file.
   */
  void open() {
    if (!opened) {
      recordManager.start();
      opened = true;
    }
  }

  /**
   * Close the collection. This method stops the record manager which in turn
   * tears down the connection with backend storage.
   */
  void close() {
    if (opened) {
      recordManager.stop();
      opened = false;
    }
  }

  /**
   * Check if the collection is open.
   */
  bool is_open() { return opened; }
};

} // namespace persist

#endif /* COLLECTION_HPP */
