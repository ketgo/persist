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
// #define PERSIST_INTRUSIVE_TESTING

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
  PageSlot::Location location;
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
      logRecord = logManager->get(logRecord->getPrevLogRecordLocation());
      logRecords.push_back(*logRecord);
    }
  }

  /**
   * @brief Helper function to insert page slot as part of a transaction.
   *
   * @param txn reference to the transaction
   * @param slot reference to the page lost to insert
   * @returns location where page slot is inserted
   */
  PageSlot::Location insert(Transaction &txn, PageSlot &slot) {
    PageSlot::Location _location;
    auto page = bufferManager->getNew();
    auto inserted = page->insertPageSlot(slot, txn);
    _location.pageId = page->getId();
    _location.slotId = inserted.first;

    return _location;
  }

private:
  /**
   * @brief Insert test data
   *
   */
  void insert() {
    PageSlot slot("testing"_bb);
    Transaction txn = txnManager->begin();
    location = insert(txn, slot);
    txnManager->commit(txn, true);
  }
};

TEST_F(TransactionManagerTestFixture, TestBegin) {
  // Begin transaction
  Transaction txn = txnManager->begin();

  // Retrive all log records for transaction
  std::vector<LogRecord> logRecords;
  retriveLogRecord(txn, logRecords);

  // Assert BEGIN log record
  ASSERT_EQ(logRecords.size(), 1);
  ASSERT_EQ(logRecords[0].getLogType(), LogRecord::Type::BEGIN);
  ASSERT_EQ(logRecords[0].getSeqNumber(), txn.getLogLocation().seqNumber);
}

TEST_F(TransactionManagerTestFixture, TestCommitForce) {
  PageSlot::Location _location;
  PageSlot slot("testing-commit-force"_bb);

  // Insert page slot as part of transaction
  Transaction txn = txnManager->begin();
  _location = insert(txn, slot);
  txnManager->commit(txn, true);

  Transaction _txn = txnManager->begin();
  // Assert page flushed
  ASSERT_EQ(
      dataStorage->read(_location.pageId)->getPageSlot(_location.slotId, _txn),
      slot);
  txnManager->commit(_txn);

  // Retrive all log records for transaction
  std::vector<LogRecord> logRecords;
  retriveLogRecord(txn, logRecords);

  // Assert BEGIN and INSERT log records
  ASSERT_EQ(logRecords.size(), 3);
  ASSERT_EQ(logRecords[2].getLogType(), LogRecord::Type::BEGIN);
  ASSERT_EQ(logRecords[1].getLogType(), LogRecord::Type::INSERT);
  ASSERT_EQ(logRecords[1].getLocation(), _location);
  ASSERT_EQ(logRecords[1].getPageSlotA(), slot);
  ASSERT_EQ(logRecords[0].getLogType(), LogRecord::Type::COMMIT);
}

TEST_F(TransactionManagerTestFixture, TestCommitNoForce) {
  PageSlot::Location _location;
  PageSlot slot("testing-commit-no-force"_bb);

  // Insert page slot as part of transaction
  Transaction txn = txnManager->begin();
  _location = insert(txn, slot);
  txnManager->commit(txn);

  // Assert page not flushed
  ASSERT_THROW(dataStorage->read(_location.pageId), PageNotFoundError);

  // Retrive all log records for transaction
  std::vector<LogRecord> logRecords;
  retriveLogRecord(txn, logRecords);

  // Assert BEGIN and INSERT log records
  ASSERT_EQ(logRecords.size(), 3);
  ASSERT_EQ(logRecords[2].getLogType(), LogRecord::Type::BEGIN);
  ASSERT_EQ(logRecords[1].getLogType(), LogRecord::Type::INSERT);
  ASSERT_EQ(logRecords[1].getLocation(), _location);
  ASSERT_EQ(logRecords[1].getPageSlotA(), slot);
  ASSERT_EQ(logRecords[0].getLogType(), LogRecord::Type::COMMIT);
}

TEST_F(TransactionManagerTestFixture, TestInsertAbort) {
  PageSlot::Location _location;
  PageSlot slot("testing-insert-abort"_bb);

  // Insert page slot as part of transaction
  Transaction txn = txnManager->begin();
  _location = insert(txn, slot);
  txnManager->abort(txn);

  Transaction _txn = txnManager->begin();
  // Assert slot not inserted due to aborted transaction
  ASSERT_THROW(
      bufferManager->get(_location.pageId)->getPageSlot(_location.slotId, _txn),
      PageSlotNotFoundError);
  txnManager->commit(_txn);

  // Retrive all log records for transaction
  std::vector<LogRecord> logRecords;
  retriveLogRecord(txn, logRecords);

  // Assert Rollback log records
  ASSERT_EQ(logRecords.size(), 4);
  ASSERT_EQ(logRecords[3].getLogType(), LogRecord::Type::BEGIN);
  ASSERT_EQ(logRecords[2].getLogType(), LogRecord::Type::INSERT);
  ASSERT_EQ(logRecords[2].getLocation(), _location);
  ASSERT_EQ(logRecords[2].getPageSlotA(), slot);
  ASSERT_EQ(logRecords[1].getLogType(), LogRecord::Type::DELETE);
  ASSERT_EQ(logRecords[1].getLocation(), _location);
  ASSERT_EQ(logRecords[1].getPageSlotA(), slot);
  ASSERT_EQ(logRecords[0].getLogType(), LogRecord::Type::ABORT);
}

/*
TEST_F(TransactionManagerTestFixture, TestUpdateAbort) {
  Transaction txn = txnManager->begin();
  Page &page = pageTable->get(pageId);
  RecordBlock recordBlock;
  recordBlock.data = "testing-update"_bb;
  page.updateRecordBlock(txn, pageSlotId, recordBlock);
  ASSERT_EQ(pageTable->get(pageId).getRecordBlock(txn, pageSlotId).data,
            "testing-update"_bb);
  txnManager->abort(txn);
  ASSERT_EQ(pageTable->get(pageId).getRecordBlock(txn, pageSlotId).data,
            "testing"_bb);

  std::vector<LogRecord> logRecords;
  SeqNumber seqNumber = txn.prevSeqNumber;
  while (seqNumber) {
    logRecords.push_back(logManager->get(seqNumber));
    ASSERT_EQ(logRecords.back().header.seqNumber, seqNumber);
    seqNumber = logRecords.back().header.prevSeqNumber;
  }
  ASSERT_EQ(logRecords.size(), 5);
  ASSERT_EQ(logRecords[4].type, LogRecord::Type::BEGIN);
  ASSERT_EQ(logRecords[3].type, LogRecord::Type::UPDATE);
  ASSERT_EQ(logRecords[3].location, RecordBlock::Location(pageId, pageSlotId));
  ASSERT_EQ(logRecords[3].recordBlockA.data, "testing"_bb);
  ASSERT_EQ(logRecords[3].recordBlockB.data, "testing-update"_bb);
  ASSERT_EQ(logRecords[2].type, LogRecord::Type::ABORT);
  ASSERT_EQ(logRecords[1].type, LogRecord::Type::UPDATE);
  ASSERT_EQ(logRecords[1].location, RecordBlock::Location(pageId, pageSlotId));
  ASSERT_EQ(logRecords[1].recordBlockA.data, "testing-update"_bb);
  ASSERT_EQ(logRecords[1].recordBlockB.data, "testing"_bb);
  ASSERT_EQ(logRecords[0].type, LogRecord::Type::DONE);
}

TEST_F(TransactionManagerTestFixture, TestRemoveAbort) {
  Transaction txn = txnManager->begin();
  Page &page = pageTable->get(pageId);
  page.removeRecordBlock(txn, pageSlotId);
  EXPECT_THROW(pageTable->get(pageId).getRecordBlock(txn, pageSlotId),
               RecordBlockNotFoundError);
  txnManager->abort(txn);
  ASSERT_EQ(pageTable->get(pageId).getRecordBlock(txn, pageSlotId).data,
            "testing"_bb);

  std::vector<LogRecord> logRecords;
  SeqNumber seqNumber = txn.prevSeqNumber;
  while (seqNumber) {
    logRecords.push_back(logManager->get(seqNumber));
    ASSERT_EQ(logRecords.back().header.seqNumber, seqNumber);
    seqNumber = logRecords.back().header.prevSeqNumber;
  }
  ASSERT_EQ(logRecords.size(), 5);
  ASSERT_EQ(logRecords[4].type, LogRecord::Type::BEGIN);
  ASSERT_EQ(logRecords[3].type, LogRecord::Type::DELETE);
  ASSERT_EQ(logRecords[3].location, RecordBlock::Location(pageId, pageSlotId));
  ASSERT_EQ(logRecords[3].recordBlockA.data, "testing"_bb);
  ASSERT_EQ(logRecords[3].recordBlockB.data, ""_bb);
  ASSERT_EQ(logRecords[2].type, LogRecord::Type::ABORT);
  ASSERT_EQ(logRecords[1].type, LogRecord::Type::INSERT);
  ASSERT_EQ(logRecords[1].location, RecordBlock::Location(pageId, pageSlotId));
  ASSERT_EQ(logRecords[1].recordBlockA.data, "testing"_bb);
  ASSERT_EQ(logRecords[1].recordBlockB.data, ""_bb);
  ASSERT_EQ(logRecords[0].type, LogRecord::Type::DONE);
}
*/