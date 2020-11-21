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

Transaction TransactionManager::begin(Transaction *txn) {
  if (txn == nullptr) {
    return Transaction(logManager, uuid::generate(),
                       Transaction::State::GROWING);
  }
  // Begin transaction if not started else perform NOP
  if (txn->state == Transaction::State::NOT_STARTED) {
    txn->state = Transaction::State::GROWING;
  }

  return *txn;
}

void TransactionManager::abort(Transaction *txn) {
  if (txn != nullptr) {
    // Abort transaction if not in completed state, i.e. COMMITED or
    // ABORTED.
    if (txn->state != Transaction::State::COMMITED &&
        txn->state != Transaction::State::ABORTED) {
      // TODO: Rollback transition
    }
  }
}

void TransactionManager::commit(Transaction *txn) {
  if (txn != nullptr) {
    // Commit pages if transaction not in completed state, i.e. COMMITED or
    // ABORTED.
    if (txn->state != Transaction::State::COMMITED &&
        txn->state != Transaction::State::ABORTED) {
      txn->state = Transaction::State::COMMITED;
      // Flush all staged pages
      for (auto pageId : txn->staged) {
        pageTable.flush(pageId);
      }
    }
  }
}

} // namespace persist
