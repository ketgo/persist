/**
 * transaction_manager.hpp - Persist
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

#ifndef TRANSACTION_MANAGER_HPP
#define TRANSACTION_MANAGER_HPP

#include <persist/core/buffer/buffer_manager.hpp>
#include <persist/core/buffer/replacer/base.hpp>
#include <persist/core/page/slotted_page/vls_slotted_page.hpp>
#include <persist/core/recovery/log_manager.hpp>
#include <persist/core/transaction/transaction.hpp>

#include <persist/utility/uid.hpp>

namespace persist {

/**
 * @brief Transaction Manager Class
 *
 * The transaction manager manages slotted page transactions of a collection.
 *
 * @tparam PageType type of page
 */
// TODO: This is brute force approach. Need to use polymorphic pages.
template <class PageType> class TransactionManager {
  static_assert(std::is_base_of<SlottedPage, PageType>::value,
                "PageType must be derived from SlottedPage class.");

  PERSIST_PRIVATE
  /**
   * @brief Pointer to Buffer Manager
   *
   */
  BufferManager<PageType> *bufferManager;

  /**
   * @brief Pointer to Log Manager
   *
   */
  LogManager *logManager;

  /**
   * @brief Log transaction begin.
   *
   * @param txn reference to the new transaction
   */
  void logBegin(Transaction &txn) {
    // Log record for starting transaction
    LogRecord logRecord(txn.getId(), txn.getLogLocation(),
                        LogRecord::Type::BEGIN);
    // Add log record and update the location in the transaction
    txn.setLogLocation(logManager->add(logRecord));
  }

  /**
   * @brief Log transaction abort.
   *
   * @param txn reference to the aborted transaction
   */
  void logAbort(Transaction &txn) {
    // Log record for transaction abortion
    LogRecord logRecord(txn.getId(), txn.getLogLocation(),
                        LogRecord::Type::ABORT);
    // Add log record and update the location in the transaction
    txn.setLogLocation(logManager->add(logRecord));
  }

  /**
   * @brief Log transaction commit operation.
   *
   * @param txn reference to the committed transaction
   */
  void logCommit(Transaction &txn) {
    // Log record for commit operation
    LogRecord logRecord(txn.getId(), txn.getLogLocation(),
                        LogRecord::Type::COMMIT);
    // Add log record and update the location in the transaction
    txn.setLogLocation(logManager->add(logRecord));
  }

  /**
   * @brief Undo a given operation performed during a transaction.
   *
   * @param txn reference to the transaction for which to perfrom undo
   * @param logRecord log record of the operation to undo
   */
  void undo(Transaction &txn, LogRecord &logRecord) {
    switch (logRecord.getLogType()) {
    case LogRecord::Type::INSERT: {
      auto page = bufferManager->get(logRecord.getLocation().pageId);
      page->removePageSlot(logRecord.getLocation().slotId, txn);
      break;
    }
    case LogRecord::Type::DELETE: {
      auto page = bufferManager->get(logRecord.getLocation().pageId);
      page->undoRemovePageSlot(logRecord.getLocation().slotId,
                               logRecord.getPageSlotA(), txn);
      break;
    }
    case LogRecord::Type::UPDATE: {
      auto page = bufferManager->get(logRecord.getLocation().pageId);
      // NOTE: The value from log record object is being moved
      page->updatePageSlot(logRecord.getLocation().slotId,
                           logRecord.getPageSlotA(), txn);
      break;
    }
    default:
      break;
    }
  }

public:
  /**
   * @brief Construct a new Transaction Manager object
   *
   * @param bufferManager slotted page buffer manager for which transactions are
   * to be managed
   * @param logManager transaction log manager
   */
  TransactionManager(BufferManager<PageType> *bufferManager,
                     LogManager *logManager)
      : bufferManager(bufferManager), logManager(logManager) {}

  /**
   * @brief Begin a new transaction.
   *
   * @returns transaction object
   */
  Transaction begin() {
    // Create a new transaction
    Transaction txn(logManager, utility::generateUID(),
                    Transaction::State::ACTIVE);
    // Log transaction begin record
    logBegin(txn);

    return txn;
  }

  /**
   * @brief Abort given transaction. This operation rollsback all changes
   * performed during the transaction.
   *
   * @param txn reference to the transaction to abort.
   */
  void abort(Transaction &txn) {
    // Abort transaction if not in completed state, i.e. COMMITED or
    // ABORTED.
    if (txn.getState() != Transaction::State::COMMITED &&
        txn.getState() != Transaction::State::ABORTED) {
      // Undo all operations performed as part of the transaction
      std::unique_ptr<LogRecord> logRecord =
          logManager->get(txn.getLogLocation());
      undo(txn, *logRecord);
      while (!logRecord->getPrevLogRecordLocation().isNull()) {
        logRecord = logManager->get(logRecord->getPrevLogRecordLocation());
        undo(txn, *logRecord);
      }

      // Log transaction abort record
      logAbort(txn);

      // NOTE: No need to flush log records and staged pages since the recovery
      // manager will always abort any unfinished transaction.

      // Set transaction to aborted state as all operations performed by the
      // transaction have been rolled backed.
      txn.setState(Transaction::State::ABORTED);
    }
  }

  /**
   * @brief Commit given transaction. Persists all modified pages and metadata
   * to backend storage.
   *
   * @param txn reference to the transaction to commit.
   * @param force force commit all modified pages to backend storage. If set
   * to `false`, the modified pages are automatically written upon page
   * replacement process of the buffer manager. Default value is set to
   * `false`.
   */
  void commit(Transaction &txn, bool force = false) {
    // Commit staged pages if transaction not in completed state, i.e.
    // COMMITED or ABORTED.
    if (txn.getState() != Transaction::State::COMMITED &&
        txn.getState() != Transaction::State::ABORTED) {
      // Log transaction commit record
      logCommit(txn);
      // Flush all log records to stable storage
      logManager->flush();
      // Set transaction to partially commited state. This is in compliance
      // with the requirement that all log records are flushed to backend
      // storage on transaction commit.
      txn.setState(Transaction::State::PARTIALLY_COMMITED);

      // Flush all staged pages if force mode commit
      if (force) {

        // TODO: Use page IDs in log records instead of a staged list?

        // Flush all staged pages
        for (auto pageId : txn.getStaged()) {
          bufferManager->flush(pageId);
        }
        // Set transaction to commited state as all modified pages by the
        // transaction have been flushed to disk.
        txn.setState(Transaction::State::COMMITED);

        // TODO: Transaction state change to COMMITED for non-force commit
      }
    }
  }
};

} // namespace persist

#endif /* TRANSACTION_MANAGER_HPP */
