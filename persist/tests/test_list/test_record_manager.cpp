/**
 * test_record_manager.cpp - Persist
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
 * @brief Unit Test ListRecordManager
 *
 */

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include <persist/core/fsm/fsl.hpp>
#include <persist/core/transaction/transaction_manager.hpp>
#include <persist/list/record_manager.hpp>
#include <persist/utility/serializer.hpp>

using namespace persist;

class ListRecordManagerTestFixture : public ::testing::Test {
protected:
  const uint64_t page_size = DEFAULT_PAGE_SIZE;
  const uint64_t max_size = 2;
  const std::string data_connection_string = "file://test_list_record_manager";
  const std::string log_connection_string =
      "file://test_list_record_manager_log";
  std::unique_ptr<FSLPage> fsl_page;
  std::unique_ptr<Storage<RecordPage>> data_storage;
  std::unique_ptr<Storage<FSLPage>> fsl_storage;
  std::unique_ptr<Storage<LogPage>> log_storage;
  std::unique_ptr<BufferManager<RecordPage, LRUReplacer>> buffer_manager;
  std::unique_ptr<FSLManager> fsl_manager;
  std::unique_ptr<TransactionManager> txn_manager;

  /**
   * @brief Record stored by the collection.
   *
   */
  struct Record : public Storable {
    std::string data;

    Record() = default;
    Record(const std::string &record) : data(record) {}
    size_t GetStorageSize() const override {
      return data.size() + sizeof(size_t);
    }
    void Load(Span input) override { load(input, data); }
    void Dump(Span output) override { dump(output, data); }
    bool operator==(const Record &other) const { return data == other.data; }
  };
  std::unique_ptr<ListRecordManager<Record, LRUReplacer, FSLManager>>
      record_manager;

  void SetUp() override {
    // Setup backend storage
    data_storage = persist::CreateStorage<RecordPage>(
        ConnectionString(data_connection_string, DATA_STORAGE_EXTENTION));
    data_storage->Open();
    fsl_storage = persist::CreateStorage<FSLPage>(
        ConnectionString(data_connection_string, FSM_STORAGE_EXTENTION));
    fsl_storage->Open();
    log_storage = persist::CreateStorage<LogPage>(log_connection_string);
    log_storage->Open();

    // Setup buffer manager
    buffer_manager = std::make_unique<BufferManager<RecordPage>>(
        data_storage.get(), max_size);
    buffer_manager->Start();

    // Setup free space manager
    fsl_manager =
        std::make_unique<FSLManager>(data_connection_string, max_size);
    fsl_manager->Start();

    // Setup transaction manager
    txn_manager = std::make_unique<TransactionManager>(
        *buffer_manager, log_connection_string, max_size);
    txn_manager->Start();

    // Setup record manager
    record_manager =
        std::make_unique<ListRecordManager<Record, LRUReplacer, FSLManager>>(
            *buffer_manager, *fsl_manager);
    record_manager->Start();
  }

  void TearDown() override {
    buffer_manager->Stop();
    fsl_manager->Stop();
    txn_manager->Stop();
    record_manager->Stop();

    data_storage->Remove();
    fsl_storage->Remove();
    log_storage->Remove();
  }
};

TEST_F(ListRecordManagerTestFixture, TestInsert) {
  // Insert record
  Record record("test");
  RecordLocation location;
  {
    auto txn = txn_manager->Begin();
    location = record_manager->Insert(record, txn);
    txn_manager->Commit(txn);
  }

  // Get record
  Record _record;
  {
    auto txn = txn_manager->Begin();
    record_manager->Get(_record, location, txn);
    txn_manager->Commit(txn);
  }

  ASSERT_EQ(record, _record);
}
