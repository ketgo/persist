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
#include <persist/core/log_manager.hpp>

namespace persist {

/**
 * Transaction Class
 *
 * A transaction represents group of operations performed on collection while
 * mentaining atomicity. A transaction can have the following states:
 *
 * - GROWING: Adding operations to the transaction
 * - SHRINKING: Removing operations during rollback
 * - COMMITED: Successful completion of operations
 * - ABORTED: The transaction has been rolled back and the database  has
 * beenrestored to its state prior to the start of the transaction.
 */
class Transaction {
  friend class Page;
  friend class SlottedPage;
  friend class TransactionManager;

public:
  /**
   * @brief Transaction states.
   */
  enum class State { GROWING, SHRINKING, COMMITED, ABORTED };

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
   * @param recordBlock record block inserted
   */
  void logInsertOp(RecordBlock::Location &location, RecordBlock &recordBlock) {
    // Log record for insert operation
    LogRecord logRecord(id, prevSeqNumber, LogRecord::Type::INSERT, location,
                        recordBlock);
    prevSeqNumber = logManager.add(logRecord);
  }

  /**
   * @brief Log UPDATE operation.
   *
   * @param location location where record is located
   * @param oldRecordBlock old record block
   * @param newRecordBlock new record block
   */
  void logUpdateOp(RecordBlock::Location &location, RecordBlock &oldRecordBlock,
                   RecordBlock &newRecordBlock) {
    // Log record for update operation
    LogRecord logRecord(id, prevSeqNumber, LogRecord::Type::UPDATE, location,
                        oldRecordBlock, newRecordBlock);
    prevSeqNumber = logManager.add(logRecord);
  }

  /**
   * @brief Log DELETE operation.
   *
   * @param location location where record is located
   * @param recordBlock record block deleted
   */
  void logDeleteOp(RecordBlock::Location &location, RecordBlock &recordBlock) {
    // Log record for delete operation
    LogRecord logRecord(id, prevSeqNumber, LogRecord::Type::DELETE, location,
                        recordBlock);
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
  Transaction(LogManager &logManager, uint64_t id, State state = State::GROWING)
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
