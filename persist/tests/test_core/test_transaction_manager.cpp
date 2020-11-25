/**
 * test_transaction_manager.cpp - Persist
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

/**
 * Transaction Manager Unit Tests
 */

#include <gtest/gtest.h>

#include <memory>

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>
#include <persist/core/page_table.hpp>
#include <persist/core/storage/file_storage.hpp>
#include <persist/core/transaction_manager.hpp>

using namespace persist;

class TransactionManagerTestFixture : public ::testing::Test {
protected:
  const uint64_t pageSize = DEFAULT_PAGE_SIZE;
  const uint64_t maxSize = 2;
  const char *file = "test_transaction_manager.storage";
  std::unique_ptr<PageTable> pageTable;
  std::unique_ptr<FileStorage> storage;
  std::unique_ptr<LogManager> logManager;
  std::unique_ptr<TransactionManager> txnManager;

  void SetUp() override {
    // setting up storage
    storage = std::make_unique<FileStorage>(file, pageSize);
    pageTable = std::make_unique<PageTable>(*storage, maxSize);
    pageTable->open();

    // Setup transaction manager
    logManager = std::make_unique<LogManager>();
    txnManager = std::make_unique<TransactionManager>(*pageTable, *logManager);
  }

  void TearDown() override {
    storage->remove();
    pageTable->close();
  }
};

TEST_F(TransactionManagerTestFixture, TestBegin) {}
