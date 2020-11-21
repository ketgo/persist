/**
 * transaction_manager.hpp - Persist
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

#ifndef TRANSACTION_MANAGER_HPP
#define TRANSACTION_MANAGER_HPP

#include <persist/core/defs.hpp>
#include <persist/core/log_manager.hpp>
#include <persist/core/transaction.hpp>

namespace persist {
/**
 * @brief Transaction Manager
 *
 * The transaction manager handles Transaction objects and detects any
 * operational conflicts during concurrent transations using lock manager.
 */
class TransactionManager {
  PERSIST_PRIVATE
  /**
   * @brief Reference to PageTable
   */
  PageTable &pageTable;

  /**
   * @brief Reference to log manager
   */
  LogManager &logManager;

public:
  /**
   * @brief Construct a new Transaction Manager object
   *
   * @param pageTable reference to the page table
   * @param logManager reference to the log manager
   */
  TransactionManager(PageTable &pageTable, LogManager &logManager)
      : pageTable(pageTable), logManager(logManager) {}

  /**
   * @brief Begin a new or existing transaction.
   *
   * @param txn pointer to existing transaction to begin. A new transaction is
   * created if set to null.
   * @returns transaction object
   */
  Transaction begin(Transaction *txn = nullptr);

  /**
   * @brief Abort given transaction. This operation rollsback all changes
   * performed during the transaction.
   *
   * @param txn pointer to the transaction to abort. Nop is performed if null is
   * passed.
   */
  void abort(Transaction *txn);

  /**
   * @brief Commit given transaction. Persists all modified pages and metadata
   * to backend storage.
   *
   * @param txn pointer to the transaction to commit. Nop is performed if null
   * is passed.
   */
  void commit(Transaction *txn);
};

} // namespace persist

#endif /* TRANSACTION_MANAGER_HPP */
