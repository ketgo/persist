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

#ifndef PERSIST_CORE_TRANSACTION_HPP
#define PERSIST_CORE_TRANSACTION_HPP

#include <set>

#include <persist/core/defs.hpp>
#include <persist/core/wal/log_manager.hpp>

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
public:
  /**
   * @brief Enumerated set of transaction states.
   */
  enum class State { ACTIVE, FAILED, PARTIALLY_COMMITED, COMMITED, ABORTED };

  PERSIST_PRIVATE

  /**
   * @brief Pointer to log manager.
   */
  LogManager *log_manager;

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
   * @brief Location of the latest log record in the transaction. This is used
   * to link the the next log record.
   */
  LogRecord::Location log_location;

public:
  /**
   * Construct a new Transaction object
   *
   * @param log_manager pointer to log manager
   * @param id Transaction ID
   * @param state transaction state
   */
  Transaction(LogManager *log_manager, TransactionId id,
              State state = State::ACTIVE)
      : log_manager(log_manager), id(id), state(state), log_location(0, 0) {}

  /**
   * @brief Log INSERT operation.
   *
   * @param location location where record is inserted
   * @param page_slot page slot inserted
   */
  void LogInsertOp(RecordPageSlot::Location &location,
                   RecordPageSlot &page_slot) {
    // Stage Page ID
    staged.insert(location.page_id);
    // Log record for insert operation
    LogRecord log_record(id, log_location, LogRecord::Type::INSERT, location,
                         page_slot);
    log_location = log_manager->Add(log_record);
  }

  /**
   * @brief Log UPDATE operation.
   *
   * @param location location where record is located
   * @param old_page_slot old page slot
   * @param new_page_slot new page slot
   */
  void LogUpdateOp(RecordPageSlot::Location &location,
                   RecordPageSlot &old_page_slot,
                   RecordPageSlot &new_page_slot) {
    // Stage Page ID
    staged.insert(location.page_id);
    // Log record for update operation
    LogRecord log_record(id, log_location, LogRecord::Type::UPDATE, location,
                         old_page_slot, new_page_slot);
    log_location = log_manager->Add(log_record);
  }

  /**
   * @brief Log DELETE operation.
   *
   * @param location location where record is located
   * @param page_slot page slot deleted
   */
  void LogDeleteOp(RecordPageSlot::Location &location,
                   RecordPageSlot &page_slot) {
    // Stage Page ID
    staged.insert(location.page_id);
    // Log record for delete operation
    LogRecord log_record(id, log_location, LogRecord::Type::DELETE, location,
                         page_slot);
    log_location = log_manager->Add(log_record);
  }

  /**
   * @brief Get the staged page IDs in the transaction.
   *
   * @returns constant reference to set of staged page IDs
   */
  const std::set<PageId> &GetStaged() const { return staged; }

  /**
   * @brief Get the transaction ID
   *
   * @returns transaction unique identifier
   */
  TransactionId GetId() const { return id; }

  /**
   * Get the state of transaction.
   *
   * @returns transaction state
   */
  const State GetState() const { return state; }

  /**
   * Set the state of transaction.
   *
   * @param state transaction state to set
   */
  void SetState(State state) { this->state = state; }

  /**
   * @brief Get the location of the lastest log record in the transaction.
   *
   * @returns sequence number of the lastest log record in the transaction
   */
  const LogRecord::Location GetLogLocation() const { return log_location; }

  /**
   * @brief Set the location of the lastest log record in the transaction.
   *
   * @param location location of the lastest log record in the transaction
   */
  void SetLogLocation(LogRecord::Location location) { log_location = location; }

  /**
   * @brief Equality comparision operator.
   */
  bool operator==(const Transaction &other) const {
    return staged == other.staged && state == other.state;
  }
};

} // namespace persist

#endif /* TRANSACTION_HPP */
