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

#include <persist/core/storage/creator.hpp>
#include <persist/core/transaction/transaction_manager.hpp>

using namespace persist;

class TransactionManagerTestFixture : public ::testing::Test {
protected:
  const uint64_t page_size = DEFAULT_PAGE_SIZE;
  const uint64_t max_size = 2;
  const std::string data_connection_string = "file://test_txn_manager";
  const std::string log_connection_string = "file://test_txn_manager_log";
  RecordPageSlot::Location location;
  std::unique_ptr<BufferManager<RecordPage>> buffer_manager;
  std::unique_ptr<Storage<RecordPage>> data_storage;
  std::unique_ptr<Storage<LogPage>> log_storage;
  std::unique_ptr<LogManager> log_manager;
  std::unique_ptr<TransactionManager> txn_manager;

  void SetUp() override {
    // Setting up storage
    data_storage = persist::CreateStorage<RecordPage>(data_connection_string);
    log_storage = persist::CreateStorage<LogPage>(log_connection_string);

    // Setting up buffer manager
    buffer_manager =
        std::make_unique<BufferManager<RecordPage>>(*data_storage, max_size);
    buffer_manager->Start();

    // Setting up log manager
    log_manager = std::make_unique<LogManager>(*log_storage, max_size);
    log_manager->Start();

    // Setup transaction manager
    txn_manager =
        std::make_unique<TransactionManager>(*buffer_manager, *log_manager);
    txn_manager->Start();

    // Setup data for test
    Insert();
  }

  void TearDown() override {
    data_storage->Remove();
    buffer_manager->Stop();
    log_storage->Remove();
    log_manager->Stop();
    txn_manager->Stop();
  }

  /**
   * @brief Helper function to retrive all log records for a given transaction.
   * The retrived records are inserted into a vector passed by reference.
   *
   * @param txn reference to the transaction
   * @param log_records reference to vector of log records type
   */
  void RetriveLogRecord(Transaction &txn, std::vector<LogRecord> &log_records) {
    std::unique_ptr<LogRecord> log_record =
        log_manager->Get(txn.GetLogLocation());
    log_records.push_back(*log_record);
    while (!log_record->GetPrevLocation().IsNull()) {
      log_record = log_manager->Get(log_record->GetPrevLocation());
      log_records.push_back(*log_record);
    }
  }

  /**
   * @brief Helper function to insert page slot as part of a transaction.
   *
   * @param txn reference to the transaction
   * @param slot reference to the page lost to insert
   * @returns location where page slot is inserted
   */
  RecordPageSlot::Location Insert(Transaction &txn, RecordPageSlot &slot) {
    RecordPageSlot::Location _location;
    auto page = buffer_manager->GetNew();
    auto inserted = page->InsertPageSlot(slot, txn);
    _location.page_id = page->GetId();
    _location.slot_id = inserted.first;

    return _location;
  }

  /**
   * @brief Helper function to update page slot as part of a transaction.
   *
   * @param txn reference to the transaction
   * @param location reference to the location to update
   * @param slot reference to the page lost to insert
   */
  void Update(Transaction &txn, RecordPageSlot::Location &location,
              RecordPageSlot &slot) {
    auto page = buffer_manager->Get(location.page_id);
    page->UpdatePageSlot(location.slot_id, slot, txn);
  }

  /**
   * @brief Helper function to update page slot as part of a transaction.
   *
   * @param txn reference to the transaction
   * @param location reference to the location to remove
   */
  void Remove(Transaction &txn, RecordPageSlot::Location &location) {
    auto page = buffer_manager->Get(location.page_id);
    page->RemovePageSlot(location.slot_id, txn);
  }

private:
  /**
   * @brief Insert test data
   *
   */
  void Insert() {
    RecordPageSlot slot("testing"_bb);
    Transaction txn = txn_manager->Begin();
    location = Insert(txn, slot);
    txn_manager->Commit(txn, true);
  }
};

TEST_F(TransactionManagerTestFixture, TestBegin) {
  // Begin transaction
  Transaction txn = txn_manager->Begin();

  // Retrive all log records for transaction
  std::vector<LogRecord> log_records;
  RetriveLogRecord(txn, log_records);

  // Assert BEGIN log record
  ASSERT_EQ(log_records.size(), 1);
  ASSERT_EQ(log_records[0].GetLogType(), LogRecord::Type::BEGIN);
  ASSERT_EQ(log_records[0].GetSeqNumber(), txn.GetLogLocation().seq_number);
}

TEST_F(TransactionManagerTestFixture, TestCommitForce) {
  RecordPageSlot::Location _location;
  RecordPageSlot slot("testing-commit-force"_bb);

  // Insert page slot as part of transaction
  Transaction txn = txn_manager->Begin();
  _location = Insert(txn, slot);
  txn_manager->Commit(txn, true);

  Transaction _txn = txn_manager->Begin();
  // Assert page flushed
  auto page = data_storage->Read(_location.page_id);
  ASSERT_EQ(page->GetPageSlot(_location.slot_id, _txn), slot);
  txn_manager->Commit(_txn);

  // Retrive all log records for transaction
  std::vector<LogRecord> log_records;
  RetriveLogRecord(txn, log_records);

  // Assert BEGIN and INSERT log records
  ASSERT_EQ(log_records.size(), 3);
  ASSERT_EQ(log_records[2].GetLogType(), LogRecord::Type::BEGIN);
  ASSERT_EQ(log_records[1].GetLogType(), LogRecord::Type::INSERT);
  ASSERT_EQ(log_records[1].GetLocation(), _location);
  ASSERT_EQ(log_records[1].GetPageSlotA(), slot);
  ASSERT_EQ(log_records[0].GetLogType(), LogRecord::Type::COMMIT);
}

TEST_F(TransactionManagerTestFixture, TestCommitNoForce) {
  RecordPageSlot::Location _location;
  RecordPageSlot slot("testing-commit-no-force"_bb);

  // Insert page slot as part of transaction
  Transaction txn = txn_manager->Begin();
  _location = Insert(txn, slot);
  txn_manager->Commit(txn);

  // Assert page not flushed
  ASSERT_THROW(data_storage->Read(_location.page_id), PageNotFoundError);

  // Retrive all log records for transaction
  std::vector<LogRecord> log_records;
  RetriveLogRecord(txn, log_records);

  // Assert BEGIN and INSERT log records
  ASSERT_EQ(log_records.size(), 3);
  ASSERT_EQ(log_records[2].GetLogType(), LogRecord::Type::BEGIN);
  ASSERT_EQ(log_records[1].GetLogType(), LogRecord::Type::INSERT);
  ASSERT_EQ(log_records[1].GetLocation(), _location);
  ASSERT_EQ(log_records[1].GetPageSlotA(), slot);
  ASSERT_EQ(log_records[0].GetLogType(), LogRecord::Type::COMMIT);
}

TEST_F(TransactionManagerTestFixture, TestInsertAbort) {
  RecordPageSlot::Location _location;
  RecordPageSlot slot("testing-insert-abort"_bb);

  // Insert page slot as part of transaction
  Transaction txn = txn_manager->Begin();
  _location = Insert(txn, slot);
  txn_manager->Abort(txn);

  Transaction _txn = txn_manager->Begin();
  // Assert slot not inserted due to aborted transaction
  auto page = buffer_manager->Get(_location.page_id);
  ASSERT_THROW(page->GetPageSlot(_location.slot_id, _txn),
               PageSlotNotFoundError);
  txn_manager->Commit(_txn);

  // Retrive all log records for transaction
  std::vector<LogRecord> log_records;
  RetriveLogRecord(txn, log_records);

  // Assert Rollback log records
  ASSERT_EQ(log_records.size(), 4);
  ASSERT_EQ(log_records[3].GetLogType(), LogRecord::Type::BEGIN);
  ASSERT_EQ(log_records[2].GetLogType(), LogRecord::Type::INSERT);
  ASSERT_EQ(log_records[2].GetLocation(), _location);
  ASSERT_EQ(log_records[2].GetPageSlotA(), slot);
  ASSERT_EQ(log_records[1].GetLogType(), LogRecord::Type::DELETE);
  ASSERT_EQ(log_records[1].GetLocation(), _location);
  ASSERT_EQ(log_records[1].GetPageSlotA(), slot);
  ASSERT_EQ(log_records[0].GetLogType(), LogRecord::Type::ABORT);
}

TEST_F(TransactionManagerTestFixture, TestUpdateAbort) {
  RecordPageSlot slot("testing-update-abort"_bb);

  // Update page slot as part of transaction
  Transaction txn = txn_manager->Begin();
  Update(txn, location, slot);
  txn_manager->Abort(txn);

  Transaction _txn = txn_manager->Begin();
  // Assert slot not updated due to aborted transaction
  auto page = buffer_manager->Get(location.page_id);
  ASSERT_EQ(page->GetPageSlot(location.slot_id, _txn).data, "testing"_bb);
  txn_manager->Commit(_txn);

  // Retrive all log records for transaction
  std::vector<LogRecord> log_records;
  RetriveLogRecord(txn, log_records);

  // Assert Rollback log records
  ASSERT_EQ(log_records.size(), 4);
  ASSERT_EQ(log_records[3].GetLogType(), LogRecord::Type::BEGIN);
  ASSERT_EQ(log_records[2].GetLogType(), LogRecord::Type::UPDATE);
  ASSERT_EQ(log_records[2].GetLocation(), location);
  ASSERT_EQ(log_records[2].GetPageSlotA().data, "testing"_bb);
  ASSERT_EQ(log_records[2].GetPageSlotB().data, "testing-update-abort"_bb);
  ASSERT_EQ(log_records[1].GetLogType(), LogRecord::Type::UPDATE);
  ASSERT_EQ(log_records[1].GetLocation(), location);
  ASSERT_EQ(log_records[1].GetPageSlotA().data, "testing-update-abort"_bb);
  ASSERT_EQ(log_records[1].GetPageSlotB().data, "testing"_bb);
  ASSERT_EQ(log_records[0].GetLogType(), LogRecord::Type::ABORT);
}

TEST_F(TransactionManagerTestFixture, TestRemoveAbort) {
  // Remove page slot as part of transaction
  Transaction txn = txn_manager->Begin();
  Remove(txn, location);
  txn_manager->Abort(txn);

  Transaction _txn = txn_manager->Begin();
  // Assert slot not removed due to aborted transaction
  auto page = buffer_manager->Get(location.page_id);
  ASSERT_EQ(page->GetPageSlot(location.slot_id, _txn).data, "testing"_bb);
  txn_manager->Commit(_txn);

  // Retrive all log records for transaction
  std::vector<LogRecord> log_records;
  RetriveLogRecord(txn, log_records);

  // Assert Rollback log records
  ASSERT_EQ(log_records.size(), 4);
  ASSERT_EQ(log_records[3].GetLogType(), LogRecord::Type::BEGIN);
  ASSERT_EQ(log_records[2].GetLogType(), LogRecord::Type::DELETE);
  ASSERT_EQ(log_records[2].GetLocation(), location);
  ASSERT_EQ(log_records[2].GetPageSlotA().data, "testing"_bb);
  ASSERT_EQ(log_records[1].GetLogType(), LogRecord::Type::INSERT);
  ASSERT_EQ(log_records[1].GetLocation(), location);
  ASSERT_EQ(log_records[1].GetPageSlotA().data, "testing"_bb);
  ASSERT_EQ(log_records[0].GetLogType(), LogRecord::Type::ABORT);
}
