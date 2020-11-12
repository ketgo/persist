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
#include <persist/core/page_table.hpp>
#include <persist/core/record_manager.hpp>

namespace persist {

/**
 * Transaction Class
 *
 * A transaction represents group of operations performed on collection while
 * mentaining atomicity. A transaction can have the following states:
 *
 *  - GROWING: Adding operations to the transaction
 *  - SHRINKING: Removing operations during rollback
 *  - COMMITED: Successful completion of operations
 *  - ABORTED: The transaction has been rolled back and the database  has
 * beenrestored to its state prior to the start of the transaction.
 */
class Transaction {
public:
  /**
   * @brief Transaction states.
   */
  enum class State { GROWING, SHRINKING, COMMITED, ABORTED };

  /**
   * @brief Operation Class
   */
  struct Operation {
    enum class Type { INSERT, UPDATE, DELETE };
    Type type;
    RecordLocation location;
    ByteBuffer record;
  };

  PERSIST_PRIVATE

  /**
   * @brief Reference to page table.
   */
  PageTable &pageTable;

  /**
   * @brief Transaction ID
   */
  uint64_t id;

  /**
   * @brief Transaction state.
   */
  State state;

  /**
   * @brief Set of staged page ID.
   */
  std::set<PageId> staged;

public:
  /**
   * Construct a new Transaction object
   *
   * @param pageTable reference to page table object
   * @param id Transaction ID
   * @param state transaction state
   */
  Transaction(PageTable &pageTable, uint64_t id, State state = State::GROWING)
      : pageTable(pageTable), id(id), state(state) {}

  /**
   * Stage the page with given ID for commit. This adds the page ID to the
   * stage list and marks the corresponding page as modified.
   *
   * @param pageId page identifier
   */
  void stage(PageId pageId);

  /**
   * Persist all modified pages and metadata to backend storage.
   */
  void commit();

  /**
   * Abort transaction. This operation rollsback all changes performed during
   * the transaction.
   */
  void abort();

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
    return &pageTable == &other.pageTable && staged == other.staged &&
           state == other.state;
  }
};

} // namespace persist

#endif /* TRANSACTION_HPP */
