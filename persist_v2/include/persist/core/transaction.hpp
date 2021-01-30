/**
 * transaction.hpp - Persist
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

#ifndef TRANSACTION_HPP
#define TRANSACTION_HPP

#include <set>

#include <persist/core/defs.hpp>
#include <persist/core/recovery/log_manager.hpp>

namespace persist {

/**
 * Transaction Class
 *
 * A transaction is a group of operations performed on a collection in atomic
 * manner. It can have the following states:
 *
 * - ACTIVE: The state represents an active transaction. A transaction is active
 * if one or more operations are being performed as part of the transaction.
 * - FAILED: The state represents a failed transaction. A transaction is failed
 * if further operations can not be performed as part of the transaction.
 * - PARTIALLY_COMMITED: The state represents a partially comitted transaction.
 * A partially comitted transaction has all its operations logged and executed.
 * - COMMITED: The state represent successfull completetion of a transaction.
 * - ABORTED: The state represents successfull rollback of a failed transaction
 * where the collection has been restored to its state prior to the start of the
 * transaction.
 *
 * The transition diagram between states is as shown below:
 *
 *  ACTIVE +--> PARTIALLY_COMMITTED ---> COMMITTED
 *         |           |
 *         |           v
 *         +-------> FAILED --------> ABORTED
 *
 */
class Transaction {
  friend class VLSSlottedPage;
  friend class TransactionManager;

public:
  /**
   * @brief Enumerated set of transaction states.
   */
  enum class State { ACTIVE, FAILED, PARTIALLY_COMMITED, COMMITED, ABORTED };

  PERSIST_PRIVATE

  /**
   * @brief Reference to log manager.
   */
  LogManager &logManager;

  /**
   * @brief Transaction ID
   */
  TransactionId id;

  /**
   * @brief Transaction state.
   */
  State state;

  /**
   * @brief Set of staged page ID.
   */
  std::set<PageId> staged;

  /**
   * @brief Sequence number of the latest log record in the transaction. This
   * used to set the previous sequence number in the next log record.
   */
  SeqNumber prevSeqNumber;

  /**
   * @brief Log INSERT operation.
   *
   * @param location location where record is inserted
   * @param pageSlot page slot inserted
   */
  void logInsertOp(PageSlot::Location &location, PageSlot &pageSlot) {
    // Log record for insert operation
    LogRecord logRecord(id, prevSeqNumber, LogRecord::Type::INSERT, location,
                        pageSlot);
    prevSeqNumber = logManager.add(logRecord);
  }

  /**
   * @brief Log UPDATE operation.
   *
   * @param location location where record is located
   * @param oldPageSlot old page slot
   * @param newPageSlot new page slot
   */
  void logUpdateOp(PageSlot::Location &location, PageSlot &oldPageSlot,
                   PageSlot &newPageSlot) {
    // Log record for update operation
    LogRecord logRecord(id, prevSeqNumber, LogRecord::Type::UPDATE, location,
                        oldPageSlot, newPageSlot);
    prevSeqNumber = logManager.add(logRecord);
  }

  /**
   * @brief Log DELETE operation.
   *
   * @param location location where record is located
   * @param pageSlot page slot deleted
   */
  void logDeleteOp(PageSlot::Location &location, PageSlot &pageSlot) {
    // Log record for delete operation
    LogRecord logRecord(id, prevSeqNumber, LogRecord::Type::DELETE, location,
                        pageSlot);
    prevSeqNumber = logManager.add(logRecord);
  }

  /**
   * Stage the page with given ID for commit. This adds the page ID to the
   * stage list and marks the corresponding page as modified.
   *
   * @param pageId page identifier
   */
  void stage(PageId pageId) { staged.insert(pageId); }

public:
  /**
   * Construct a new Transaction object
   *
   * @param logManager reference to log manager
   * @param id Transaction ID
   * @param state transaction state
   */
  Transaction(LogManager &logManager, uint64_t id, State state = State::ACTIVE)
      : logManager(logManager), id(id), state(state), prevSeqNumber(0) {}

  /**
   * @brief Get the transaction ID
   *
   * returns transaction unique identifier
   */
  TransactionId getId() { return id; }

  /**
   * Get the state of transaction.
   *
   * @returns transaction state
   */
  State getState() { return state; }

  /**
   * @brief Equality comparision operator.
   */
  bool operator==(const Transaction &other) const {
    return staged == other.staged && state == other.state;
  }
};

} // namespace persist

#endif /* TRANSACTION_HPP */
