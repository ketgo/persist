/**
 * log/test_log_manager.cpp - Persist
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
 * @brief Enable debug mode if not already enabled
 *
 */
#ifndef __PERSIST_DEBUG__
#define __PERSIST_DEBUG__
#endif

#include <persist/core/log/log_manager.hpp>
#include <persist/core/storage/factory.hpp>
#include <persist/core/page/factory.hpp>

using namespace persist;

class LogManagerTestFixture : public ::testing::Test {
protected:
  const TransactionId txn_id = 10;
  const PageId page_id = 1;
  const SeqNumber seq_number = 1;
  const LogRecord::Location location = {page_id, seq_number};
  const uint64_t max_size = 2;
  const std::string path = "test_log_manager";
  std::unique_ptr<LogPage> page;
  std::unique_ptr<LogRecord> log_record;
  std::unique_ptr<FSL> fsl;
  std::unique_ptr<LogManager> log_manager;
  std::unique_ptr<Storage> storage;

  void SetUp() override {
    // Setting log record
    log_record =
        std::make_unique<LogRecord>(txn_id); //<- Transaction BEGIN log record
    log_record->SetSeqNumber(seq_number);

    // Setting up page
    page = persist::CreatePage<LogPage>(page_id, DEFAULT_LOG_PAGE_SIZE);
    LogPageSlot slot(seq_number);
    slot.data.resize(log_record->GetSize());
    log_record->Dump(slot.data);
    page->InsertPageSlot(slot);
    page->SetLastSeqNumber(seq_number);

    // setting up free space list
    fsl = std::make_unique<FSL>();
    fsl->freePages = {page_id};

    // setting up storage
    storage = persist::CreateStorage("file://" + path);
    Insert();

    // Setup log manager
    log_manager = std::make_unique<LogManager>(storage.get(), max_size);
    log_manager->Start();
  }

  void TearDown() override {
    storage->Remove();
    log_manager->Stop();
  }

private:
  /**
   * @brief Insert test data
   */
  void Insert() {
    storage->Open();
    storage->Write(*page);
    storage->Write(*fsl);
    storage->Close();
  }
};

TEST_F(LogManagerTestFixture, TestOpen) {
  ASSERT_EQ(log_manager->GetSeqNumber(), 1);
}

TEST_F(LogManagerTestFixture, TestGet) {
  auto _log_record = log_manager->Get(location);

  ASSERT_EQ(*_log_record, *log_record);
}

TEST_F(LogManagerTestFixture, TestAdd) {
  // Creating log record which should span multiple page slots
  SlottedPageSlot page_slot_a, page_slot_b;
  page_slot_a.data = ByteBuffer(storage->GetPageSize(), 'A');
  page_slot_b.data = ByteBuffer(storage->GetPageSize(), 'B');
  SlottedPageSlot::Location slot_location = {10, 1};
  LogRecord::Location prev_log_record_location = {0, 0};
  LogRecord log_record(11, prev_log_record_location, LogRecord::Type::UPDATE,
                       slot_location, page_slot_a, page_slot_b);
  log_record.SetSeqNumber(2);

  LogRecord::Location location = log_manager->Add(log_record);
  auto _log_record = log_manager->Get(location);

  ASSERT_EQ(*_log_record, log_record);
}

TEST_F(LogManagerTestFixture, TestFlush) {
  // Creating log record which should span multiple page slots
  SlottedPageSlot page_slot_a, page_slot_b;
  page_slot_a.data = ByteBuffer(storage->GetPageSize(), 'A');
  page_slot_b.data = ByteBuffer(storage->GetPageSize(), 'B');
  SlottedPageSlot::Location slot_location = {10, 1};
  LogRecord::Location prev_log_record_location = {0, 0};
  LogRecord log_record(11, prev_log_record_location, LogRecord::Type::UPDATE,
                       slot_location, page_slot_a, page_slot_b);
  log_record.SetSeqNumber(2);

  LogRecord::Location location = log_manager->Add(log_record);
  log_manager->Flush();

  // Create storage to test page flush
  auto _storage = CreateStorage("file://" + path);
  _storage->Open();
  ASSERT_TRUE(_storage->GetPageCount() > 1);
  _storage->Close();
}
