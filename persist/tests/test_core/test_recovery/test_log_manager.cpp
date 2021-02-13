/**
 * recovery/test_log_manager.cpp - Persist
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
 * @brief LogManager unit test
 */

#include <gtest/gtest.h>

#include <memory>

/**
 * Enabled intrusive testing
 */
#define PERSIST_INTRUSIVE_TESTING

#include <persist/core/recovery/log_manager.hpp>
#include <persist/core/storage/factory.hpp>

using namespace persist;

class LogManagerTestFixture : public ::testing::Test {
protected:
  const TransactionId txnId = 10;
  const PageId pageId = 1;
  const SeqNumber seqNumber = 1;
  const LogRecord::Location location = {pageId, seqNumber};
  const uint64_t maxSize = 2;
  const std::string path = "test_log_manager";
  std::unique_ptr<LogPage> page;
  std::unique_ptr<LogRecord> logRecord;
  std::unique_ptr<FSL> fsl;
  std::unique_ptr<LogManager> logManager;
  std::unique_ptr<Storage<LogPage>> storage;

  void SetUp() override {
    // Setting log record
    logRecord =
        std::make_unique<LogRecord>(txnId); //<- Transaction BEGIN log record
    logRecord->setSeqNumber(seqNumber);

    // Setting up page
    page = std::make_unique<LogPage>(pageId);
    LogPageSlot slot(seqNumber);
    slot.data.resize(logRecord->size());
    logRecord->dump(slot.data);
    page->insertPageSlot(slot);
    page->setLastSeqNumber(seqNumber);

    // setting up free space list
    fsl = std::make_unique<FSL>();
    fsl->freePages = {pageId};

    // setting up storage
    storage = createStorage<LogPage>("file://" + path);
    insert();

    // Setup log manager
    logManager = std::make_unique<LogManager>(storage.get(), maxSize);
    logManager->start();

    auto _page = storage->read(pageId);
  }

  void TearDown() override {
    storage->remove();
    logManager->stop();
  }

private:
  /**
   * @brief Insert test data
   */
  void insert() {
    storage->open();
    storage->write(*page);
    storage->write(*fsl);
    storage->close();
  }
};

TEST_F(LogManagerTestFixture, TestOpen) { ASSERT_EQ(logManager->seqNumber, 1); }

TEST_F(LogManagerTestFixture, TestGet) {
  auto _logRecord = logManager->get(location);

  ASSERT_EQ(*_logRecord, *logRecord);
}

TEST_F(LogManagerTestFixture, TestAdd) {
  // Creating log record which should span multiple page slots
  PageSlot pageSlotA, pageSlotB;
  pageSlotA.data = ByteBuffer(storage->getPageSize(), 'A');
  pageSlotB.data = ByteBuffer(storage->getPageSize(), 'B');
  PageSlot::Location slotLocation = {10, 1};
  LogRecord::Location prevLogRecordLocation = {0, 0};
  LogRecord logRecord(11, prevLogRecordLocation, LogRecord::Type::UPDATE,
                      slotLocation, pageSlotA, pageSlotB);
  logRecord.setSeqNumber(2);

  LogRecord::Location location = logManager->add(logRecord);
  auto _logRecord = logManager->get(location);

  ASSERT_EQ(*_logRecord, logRecord);
}

TEST_F(LogManagerTestFixture, TestFlush) {
  // Creating log record which should span multiple page slots
  PageSlot pageSlotA, pageSlotB;
  pageSlotA.data = ByteBuffer(storage->getPageSize(), 'A');
  pageSlotB.data = ByteBuffer(storage->getPageSize(), 'B');
  PageSlot::Location slotLocation = {10, 1};
  LogRecord::Location prevLogRecordLocation = {0, 0};
  LogRecord logRecord(11, prevLogRecordLocation, LogRecord::Type::UPDATE,
                      slotLocation, pageSlotA, pageSlotB);
  logRecord.setSeqNumber(2);

  LogRecord::Location location = logManager->add(logRecord);
  logManager->flush();

  // Create storage to test page flush
  auto _storage = createStorage<LogPage>("file://" + path);
  _storage->open();
  ASSERT_TRUE(_storage->getPageCount() > 1);
  _storage->close();
}
