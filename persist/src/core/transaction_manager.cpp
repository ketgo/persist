/**
 * transaction_manager.cpp - Persist
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

#include <persist/core/exceptions.hpp>
#include <persist/core/transaction_manager.hpp>
#include <persist/core/utility.hpp>

namespace persist {

Transaction TransactionManager::begin() {
  return Transaction(logManager, uuid::generate(), Transaction::State::GROWING);
}

void TransactionManager::undo(Transaction &txn, LogRecord &logRecord) {
  Page *page;
  switch (logRecord.type) {
  case LogRecord::Type::INSERT:
    page = &pageTable.get(logRecord.location.pageId);
    page->removeRecordBlock(txn, logRecord.location.slotId);
    break;
  case LogRecord::Type::DELETE:
    page = &pageTable.get(logRecord.location.pageId);
    page->undoRemoveRecordBlock(txn, logRecord.location.slotId,
                                logRecord.recordBlockA);
    break;
  case LogRecord::Type::UPDATE:
    page = &pageTable.get(logRecord.location.pageId);
    page->updateRecordBlock(txn, logRecord.location.slotId,
                            logRecord.recordBlockB);
    break;
  default:
    break;
  }
}

void TransactionManager::abort(Transaction &txn) {
  // Abort transaction if not in completed state, i.e. COMMITED or
  // ABORTED.
  if (txn.state != Transaction::State::COMMITED &&
      txn.state != Transaction::State::ABORTED) {
    LogRecord *logRecord = &logManager.get(txn.getSeqNumber());
    while (logRecord->header.prevSeqNumber) {
      logRecord = &logManager.get(logRecord->header.prevSeqNumber);
      undo(txn, *logRecord);
    }
  }
}

void TransactionManager::commit(Transaction &txn) {
  // Commit pages if transaction not in completed state, i.e. COMMITED or
  // ABORTED.
  if (txn.state != Transaction::State::COMMITED &&
      txn.state != Transaction::State::ABORTED) {
    txn.state = Transaction::State::COMMITED;
    // Flush all staged pages
    for (auto pageId : txn.staged) {
      pageTable.flush(pageId);
    }
  }
}

} // namespace persist
