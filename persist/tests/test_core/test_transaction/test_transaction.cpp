/**
 * test_transaction.cpp - Persist
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

/**
 * @brief Transaction Manager Unit Test
 *
 */

#include <gtest/gtest.h>

#include <memory>

#include <persist/core/storage/creator.hpp>
#include <persist/core/transaction/transaction.hpp>

#include "persist/test/simple_page.hpp"

using namespace persist;
using namespace persist::test;

class TransactionTestFixture : public ::testing::Test {
protected:
  const uint64_t page_size = DEFAULT_PAGE_SIZE;
  const uint64_t max_size = 2;
  const TransactionId txn_id = 10;
  const std::string path = "test_transaction_log";
  RecordPageSlot::Location location;
  std::unique_ptr<Storage<LogPage>> storage;
  std::unique_ptr<LogManager> log_manager;
  std::unique_ptr<Transaction> txn;

  void SetUp() override {
    // Setting up storage
    storage = persist::CreateStorage<LogPage>("file://" + path);

    // Setting up log manager
    log_manager = std::make_unique<LogManager>(storage.get(), max_size);
    log_manager->Start();

    // Setup transaction
    txn = std::make_unique<Transaction>(log_manager.get(), txn_id);
  }

  void TearDown() override {
    storage->Remove();
    log_manager->Stop();
  }
};

TEST_F(TransactionTestFixture, TestGetId) { ASSERT_EQ(txn->GetId(), txn_id); }

TEST_F(TransactionTestFixture, TestGetSetState) {
  txn->SetState(Transaction::State::PARTIALLY_COMMITED);
  ASSERT_EQ(txn->GetState(), Transaction::State::PARTIALLY_COMMITED);
}
