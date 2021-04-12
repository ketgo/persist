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

#ifndef PERSIST_CORE_TRANSACTION_MANAGER_HPP
#define PERSIST_CORE_TRANSACTION_MANAGER_HPP

#include <persist/core/buffer/base.hpp>
#include <persist/core/page/record_page/page.hpp>
#include <persist/core/transaction/transaction.hpp>
#include <persist/core/wal/log_manager.hpp>

#include <persist/utility/uid.hpp>

namespace persist {

// TODO: Refactor template class to use page traits. The buffer manager does not
// need PageType anymore due to polymorphic serialization.

/**
 * @brief Transaction Manager Class
 *
 * The transaction manager manages slotted page transactions of a collection.
 *
 */
class TransactionManager {
  PERSIST_PRIVATE
  /**
   * @brief Reference to Buffer Manager base class.
   *
   * NOTE: The reference to the base class enables referring to buffer managers
   * with different replacer types.
   *
   */
  BufferManagerBase<RecordPage> &buffer_manager;

  /**
   * @brief Unique pointer to log manager.
   *
   */
  std::unique_ptr<LogManager> log_manager;

  /**
   * @brief Flag indicating transaction manager started.
   *
   */
  bool started;

  /**
   * @brief Log transaction begin.
   *
   * @param txn reference to the new transaction
   */
  void LogBegin(Transaction &txn) {
    // Log record for starting transaction
    LogRecord log_record(txn.GetId(), txn.GetLogLocation(),
                         LogRecord::Type::BEGIN);
    // Add log record and update the location in the transaction
    txn.SetLogLocation(log_manager->Add(log_record));
  }

  /**
   * @brief Log transaction abort.
   *
   * @param txn reference to the aborted transaction
   */
  void LogAbort(Transaction &txn) {
    // Log record for transaction abortion
    LogRecord log_record(txn.GetId(), txn.GetLogLocation(),
                         LogRecord::Type::ABORT);
    // Add log record and update the location in the transaction
    txn.SetLogLocation(log_manager->Add(log_record));
  }

  /**
   * @brief Log transaction commit operation.
   *
   * @param txn reference to the committed transaction
   */
  void LogCommit(Transaction &txn) {
    // Log record for commit operation
    LogRecord log_record(txn.GetId(), txn.GetLogLocation(),
                         LogRecord::Type::COMMIT);
    // Add log record and update the location in the transaction
    txn.SetLogLocation(log_manager->Add(log_record));
  }

  /**
   * @brief Undo a given operation performed during a transaction.
   *
   * @param txn reference to the transaction for which to perfrom undo
   * @param log_record log record of the operation to undo
   */
  void Undo(Transaction &txn, LogRecord &log_record) {
    switch (log_record.GetLogType()) {
    case LogRecord::Type::INSERT: {
      auto page = buffer_manager.Get(log_record.GetLocation().page_id);
      page->RemovePageSlot(log_record.GetLocation().slot_id, txn);
      break;
    }
    case LogRecord::Type::DELETE: {
      auto page = buffer_manager.Get(log_record.GetLocation().page_id);
      page->UndoRemovePageSlot(log_record.GetLocation().slot_id,
                               log_record.GetPageSlotA(), txn);
      break;
    }
    case LogRecord::Type::UPDATE: {
      auto page = buffer_manager.Get(log_record.GetLocation().page_id);
      // NOTE: The value from log record object is being moved
      page->UpdatePageSlot(log_record.GetLocation().slot_id,
                           log_record.GetPageSlotA(), txn);
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
   * @param buffer_manager Reference to record page buffer manager.
   * @param connection_string Constant reference to connection string for log
   * storage.
   * @param cache_size Cache size to be used for WAL.
   */
  TransactionManager(BufferManager<RecordPage> &buffer_manager,
                     const std::string &connection_string,
                     size_t cache_size = DEFAULT_LOG_BUFFER_SIZE)
      : buffer_manager(buffer_manager),
        log_manager(
            std::make_unique<LogManager>(connection_string, cache_size)),
        started(false) {}

  /**
   * @brief Start transaction manager.
   *
   */
  void Start() {
    if (!started) {
      // Start log manager.
      log_manager->Start();
    }
  }

  /**
   * @brief Stop transaction manager.
   *
   */
  void Stop() {
    if (started) {
      // Stop log manager.
      log_manager->Stop();
    }
  }

  /**
   * @brief Begin a new transaction.
   *
   * @returns transaction object
   */
  Transaction Begin() {
    // Create a new transaction
    Transaction txn(*log_manager, persist::uid(), Transaction::State::ACTIVE);
    // Log transaction begin record
    LogBegin(txn);

    return txn;
  }

  /**
   * @brief Abort given transaction. This operation rollsback all changes
   * performed during the transaction.
   *
   * @param txn reference to the transaction to abort.
   */
  void Abort(Transaction &txn) {
    // Abort transaction if not in completed state, i.e. COMMITED or
    // ABORTED.
    if (txn.GetState() != Transaction::State::COMMITED &&
        txn.GetState() != Transaction::State::ABORTED) {
      // Undo all operations performed as part of the transaction
      auto log_record = log_manager->Get(txn.GetLogLocation());
      Undo(txn, *log_record);
      while (!log_record->GetPrevLocation().IsNull()) {
        log_record = log_manager->Get(log_record->GetPrevLocation());
        Undo(txn, *log_record);
      }

      // Log transaction abort record
      LogAbort(txn);

      // NOTE: No need to flush log records and staged pages since the recovery
      // manager will always abort any unfinished transaction.

      // Set transaction to aborted state as all operations performed by the
      // transaction have been rolled backed.
      txn.SetState(Transaction::State::ABORTED);
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
  void Commit(Transaction &txn, bool force = false) {
    // Commit staged pages if transaction not in completed state, i.e.
    // COMMITED or ABORTED.
    if (txn.GetState() != Transaction::State::COMMITED &&
        txn.GetState() != Transaction::State::ABORTED) {
      // Log transaction commit record
      LogCommit(txn);
      // Flush all log records to stable storage
      log_manager->Flush();
      // Set transaction to partially commited state. This is in compliance
      // with the requirement that all log records are flushed to backend
      // storage on transaction commit.
      txn.SetState(Transaction::State::PARTIALLY_COMMITED);

      // Flush all staged pages if force mode commit
      if (force) {

        // TODO: Use page IDs in log records instead of a staged list?
        // TODO: Flush free space manager pages.

        // Flush all staged pages
        for (auto page_id : txn.GetStaged()) {
          buffer_manager.Flush(page_id);
        }
        // Set transaction to commited state as all modified pages by the
        // transaction have been flushed to disk.
        txn.SetState(Transaction::State::COMMITED);

        // TODO: Transaction state change to COMMITED for non-force commit
      }
    }
  }
};

} // namespace persist

#endif /* PERSIST_CORE_TRANSACTION_MANAGER_HPP */
