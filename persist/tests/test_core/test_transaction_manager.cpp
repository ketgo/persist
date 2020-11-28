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

/**
 * Enabled intrusive testing
 */
#define PERSIST_INTRUSIVE_TESTING

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>
#include <persist/core/page.hpp>
#include <persist/core/page_table.hpp>
#include <persist/core/storage/file_storage.hpp>
#include <persist/core/transaction_manager.hpp>

using namespace persist;

class TransactionManagerTestFixture : public ::testing::Test {
protected:
  const uint64_t pageSize = DEFAULT_PAGE_SIZE;
  const uint64_t maxSize = 2;
  const char *file = "test_transaction_manager.storage";
  PageId pageId;
  PageSlotId pageSlotId;
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

    // Setup data for test
    insert();
  }

  void TearDown() override {
    storage->remove();
    pageTable->close();
  }

private:
  void insert() {
    Transaction txn = txnManager->begin();
    Page &page = pageTable->getNew();
    pageId = page.getId();

    RecordBlock recordBlock;
    recordBlock.data = "testing"_bb;
    auto inserted = page.addRecordBlock(txn, recordBlock);
    txnManager->commit(txn);
    pageSlotId = inserted.first;
  }
};

TEST_F(TransactionManagerTestFixture, TestBegin) {
  Transaction txn = txnManager->begin();
  std::vector<LogRecord> logRecords;
  SeqNumber seqNumber = txn.prevSeqNumber;
  while (seqNumber) {
    logRecords.push_back(logManager->get(seqNumber));
    ASSERT_EQ(logRecords.back().header.seqNumber, seqNumber);
    seqNumber = logRecords.back().header.prevSeqNumber;
  }
  ASSERT_EQ(logRecords.size(), 1);
  ASSERT_EQ(logRecords.back().type, LogRecord::Type::BEGIN);
}

TEST_F(TransactionManagerTestFixture, TestCommit) {
  Transaction txn = txnManager->begin();
  Page &page = pageTable->getNew();
  RecordBlock recordBlock;
  recordBlock.data = "testing-commit"_bb;
  auto inserted = page.addRecordBlock(txn, recordBlock);
  txnManager->commit(txn);

  ASSERT_EQ(pageTable->get(page.getId()).getRecordBlock(txn, inserted.first),
            recordBlock);

  std::vector<LogRecord> logRecords;
  SeqNumber seqNumber = txn.prevSeqNumber;
  while (seqNumber) {
    logRecords.push_back(logManager->get(seqNumber));
    ASSERT_EQ(logRecords.back().header.seqNumber, seqNumber);
    seqNumber = logRecords.back().header.prevSeqNumber;
  }
  ASSERT_EQ(logRecords.size(), 4);
  ASSERT_EQ(logRecords[3].type, LogRecord::Type::BEGIN);
  ASSERT_EQ(logRecords[2].type, LogRecord::Type::INSERT);
  ASSERT_EQ(logRecords[2].location,
            RecordBlock::Location(page.getId(), inserted.first));
  ASSERT_EQ(logRecords[2].recordBlockA, recordBlock);
  ASSERT_EQ(logRecords[2].recordBlockB, RecordBlock());
  ASSERT_EQ(logRecords[1].type, LogRecord::Type::COMMIT);
  ASSERT_EQ(logRecords[0].type, LogRecord::Type::DONE);
}

TEST_F(TransactionManagerTestFixture, TestInsertAbort) {
  Transaction txn = txnManager->begin();
  Page &page = pageTable->getNew();
  RecordBlock recordBlock;
  recordBlock.data = "testing-insert"_bb;
  auto inserted = page.addRecordBlock(txn, recordBlock);
  ASSERT_EQ(pageTable->get(page.getId()).getRecordBlock(txn, inserted.first),
            recordBlock);
  txnManager->abort(txn);
  EXPECT_THROW(pageTable->get(page.getId()).getRecordBlock(txn, inserted.first),
               RecordBlockNotFoundError);

  std::vector<LogRecord> logRecords;
  SeqNumber seqNumber = txn.prevSeqNumber;
  while (seqNumber) {
    logRecords.push_back(logManager->get(seqNumber));
    ASSERT_EQ(logRecords.back().header.seqNumber, seqNumber);
    seqNumber = logRecords.back().header.prevSeqNumber;
  }
  ASSERT_EQ(logRecords.size(), 5);
  ASSERT_EQ(logRecords[4].type, LogRecord::Type::BEGIN);
  ASSERT_EQ(logRecords[3].type, LogRecord::Type::INSERT);
  ASSERT_EQ(logRecords[3].location,
            RecordBlock::Location(page.getId(), inserted.first));
  ASSERT_EQ(logRecords[3].recordBlockA, recordBlock);
  ASSERT_EQ(logRecords[3].recordBlockB, RecordBlock());
  ASSERT_EQ(logRecords[2].type, LogRecord::Type::ABORT);
  ASSERT_EQ(logRecords[1].type, LogRecord::Type::DELETE);
  ASSERT_EQ(logRecords[1].location,
            RecordBlock::Location(page.getId(), inserted.first));
  ASSERT_EQ(logRecords[1].recordBlockA, recordBlock);
  ASSERT_EQ(logRecords[1].recordBlockB, RecordBlock());
  ASSERT_EQ(logRecords[0].type, LogRecord::Type::DONE);
}

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
