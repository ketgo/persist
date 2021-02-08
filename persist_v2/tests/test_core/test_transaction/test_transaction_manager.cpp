/**
 * test_transaction_manager.cpp - Persist
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

/**
 * Enabled intrusive testing
 */
#define PERSIST_INTRUSIVE_TESTING

#include <persist/core/buffer/buffer_manager.hpp>
#include <persist/core/buffer/replacer/lru_replacer.hpp>
#include <persist/core/page/log_page/log_page.hpp>
#include <persist/core/page/slotted_page/vls_slotted_page.hpp>
#include <persist/core/storage/factory.hpp>
#include <persist/core/transaction/transaction_manager.hpp>

using namespace persist;

class TransactionManagerTestFixture : public ::testing::Test {
protected:
  const uint64_t pageSize = DEFAULT_PAGE_SIZE;
  const uint64_t maxSize = 2;
  const std::string path = "test_transaction_manager";
  PageId pageId;
  PageSlotId pageSlotId;
  typedef BufferManager<VLSSlottedPage> BufferManager;
  std::unique_ptr<BufferManager> bufferManager;
  std::unique_ptr<Storage<VLSSlottedPage>> dataStorage;
  std::unique_ptr<Storage<LogPage>> logStorage;
  std::unique_ptr<LogManager> logManager;
  std::unique_ptr<TransactionManager<VLSSlottedPage>> txnManager;

  void SetUp() override {
    // Setting up storage
    dataStorage = createStorage<VLSSlottedPage>("file://" + path);
    logStorage = createStorage<LogPage>("file://" + path + "_log");

    // Setting up buffer manager
    bufferManager = std::make_unique<BufferManager>(dataStorage.get(), maxSize);
    bufferManager->start();

    // Setting up log manager
    logManager = std::make_unique<LogManager>(logStorage.get(), maxSize);
    logManager->start();

    // Setup transaction manager
    txnManager = std::make_unique<TransactionManager<VLSSlottedPage>>(
        bufferManager.get(), logManager.get());

    // Setup data for test
    insert();
  }

  void TearDown() override {
    dataStorage->remove();
    bufferManager->stop();
    logStorage->remove();
    logManager->stop();
  }

  /**
   * @brief Helper function to retrive all log records for a given transaction.
   * The retrived records are inserted into a vector passed by reference.
   *
   * @param txn reference to the transaction
   * @param logRecords reference to vector of log records type
   */
  void retriveLogRecord(Transaction &txn, std::vector<LogRecord> &logRecords) {
    std::unique_ptr<LogRecord> logRecord =
        logManager->get(txn.getLogLocation());
    logRecords.push_back(*logRecord);
    while (!logRecord->getPrevLogRecordLocation().isNull()) {
      logRecords.push_back(*logRecord);
      logRecord = logManager->get(logRecord->getPrevLogRecordLocation());
    }
  }

private:
  /**
   * @brief Insert test data
   *
   */
  void insert() {}
};

TEST_F(TransactionManagerTestFixture, TestBegin) {
  Transaction txn = txnManager->begin();

  // Retrive all log records doe transaction
  std::vector<LogRecord> logRecords;
  retriveLogRecord(txn, logRecords);

  // Assert BEGIN log record
  ASSERT_EQ(logRecords.size(), 1);
  ASSERT_EQ(logRecords.back().getSeqNumber(), txn.getLogLocation().seqNumber);
  ASSERT_EQ(logRecords.back().getLogType(), LogRecord::Type::BEGIN);
}
