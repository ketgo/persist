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

#include <list>
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
  const uint64_t max_size = 20;
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

TEST_F(ListRecordManagerTestFixture, TestGetNullLocation) {
  Record record;
  RecordLocation location;

  auto txn = txn_manager->Begin();
  ASSERT_THROW(record_manager->Get(record, location, txn), RecordNotFoundError);
  txn_manager->Commit(txn);
}

TEST_F(ListRecordManagerTestFixture, TestInsertGet) {
  std::list<std::pair<RecordLocation, std::string>> records;
  const size_t num_records = 2;

  // Insert records
  for (size_t i = 0; i < num_records; ++i) {
    std::string data((i + 3) * page_size, 'A');
    Record record(data);
    auto txn = txn_manager->Begin();
    RecordLocation location = record_manager->Insert(record, txn);
    txn_manager->Commit(txn);

    records.push_back({location, data});
  }

  // Get and assert records
  for (auto &element : records) {
    Record record;
    auto txn = txn_manager->Begin();
    record_manager->Get(record, element.first, txn);
    txn_manager->Commit(txn);

    ASSERT_EQ(element.second, record.data);
  }
}

TEST_F(ListRecordManagerTestFixture, TestUpdateNullLocation) {
  RecordLocation location;
  Record record;

  auto txn = txn_manager->Begin();
  ASSERT_THROW(record_manager->Update(record, location, txn),
               RecordNotFoundError);
  txn_manager->Commit(txn);
}

/**
 * Test update of record with new value length greater than that of old value,
 */
TEST_F(ListRecordManagerTestFixture, TestInsertUpdateGtGet) {
  RecordLocation location;
  std::string old_data(3 * page_size, 'A');
  Record old_record(old_data);

  // Insert record
  {
    auto txn = txn_manager->Begin();
    location = record_manager->Insert(old_record, txn);
    txn_manager->Commit(txn);
  }
  // Update record
  std::cout << "\n";
  std::string new_data(4 * page_size, 'B');
  Record new_record(new_data);
  {
    auto txn = txn_manager->Begin();
    record_manager->Update(new_record, location, txn);
    txn_manager->Commit(txn);
  }
  // Assert updated record
  std::cout << "\n";
  {
    Record record;
    auto txn = txn_manager->Begin();
    record_manager->Get(record, location, txn);
    txn_manager->Commit(txn);

    std::cout << new_data.size() << ", " << record.data.size() << "\n";
    ASSERT_EQ(new_data, record.data);
  }
}

/**
 * Test update of record with new value length less than that of old value,
 */
TEST_F(ListRecordManagerTestFixture, TestInsertUpdateLtGet) {
  RecordLocation location;
  std::string old_data(3 * page_size, 'A');
  Record old_record(old_data);

  // Insert record
  {
    auto txn = txn_manager->Begin();
    location = record_manager->Insert(old_record, txn);
    txn_manager->Commit(txn);
  }
  // Update record
  std::string new_data(2 * page_size, 'B');
  Record new_record(new_data);
  {
    auto txn = txn_manager->Begin();
    record_manager->Update(new_record, location, txn);
    txn_manager->Commit(txn);
  }
  // Assert updated record
  {
    Record record;
    auto txn = txn_manager->Begin();
    record_manager->Get(record, location, txn);
    txn_manager->Commit(txn);

    ASSERT_EQ(new_data, record.data);
  }
}

/**
 * Test update of record with new value length equal to that of old value,
 */
TEST_F(ListRecordManagerTestFixture, TestInsertUpdateEqGet) {
  RecordLocation location;
  std::string old_data(3 * page_size, 'A');
  Record old_record(old_data);

  // Insert record
  {
    auto txn = txn_manager->Begin();
    location = record_manager->Insert(old_record, txn);
    txn_manager->Commit(txn);
  }
  // Update record
  std::string new_data(3 * page_size, 'B');
  Record new_record(new_data);
  {
    auto txn = txn_manager->Begin();
    record_manager->Update(new_record, location, txn);
    txn_manager->Commit(txn);
  }
  // Assert updated record
  {
    Record record;
    auto txn = txn_manager->Begin();
    record_manager->Get(record, location, txn);
    txn_manager->Commit(txn);

    ASSERT_EQ(new_data, record.data);
  }
}

TEST_F(ListRecordManagerTestFixture, TestDeleteNullLocation) {
  RecordLocation location;

  auto txn = txn_manager->Begin();
  ASSERT_THROW(record_manager->Delete(location, txn), RecordNotFoundError);
  txn_manager->Commit(txn);
}

TEST_F(ListRecordManagerTestFixture, TestInsertDeleteGet) {
  RecordLocation location;
  std::string data(3 * page_size, 'A');
  Record record(data);

  // Insert record
  {
    auto txn = txn_manager->Begin();
    location = record_manager->Insert(record, txn);
    txn_manager->Commit(txn);
  }
  // Delete record
  {
    auto txn = txn_manager->Begin();
    record_manager->Delete(location, txn);
    txn_manager->Commit(txn);
  }
  // Assert record not found
  {
    Record _record;
    auto txn = txn_manager->Begin();
    ASSERT_THROW(record_manager->Get(_record, location, txn),
                 RecordNotFoundError);
    txn_manager->Commit(txn);
  }
}
