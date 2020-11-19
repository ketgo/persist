/**
 * transaction.cpp - Persist
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

#include <persist/core/transaction.hpp>

namespace persist {

void Transaction::stage(PageId pageId) {
  // Stage page ID
  staged.insert(pageId);
  // Mark page for commit
  pageTable.mark(pageId);
}

void Transaction::commit() {
  if (state == State::COMMITED || state == State::ABORTED) {
    throw;
  }

  state = State::COMMITED;
  // Flush all staged pages
  for (auto pageId : staged) {
    pageTable.flush(pageId);
  }
}

void Transaction::abort() {
  if (state == State::COMMITED || state == State::ABORTED) {
    throw;
  }

  state = State::SHRINKING;
  // TODO: Rollback transition
  state = State::ABORTED;
}

} // namespace persist
