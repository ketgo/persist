/**
 * test_metadata_manager.cpp - Persist
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
 * @brief Unit Test MetadataManager
 *
 */

#include <gtest/gtest.h>

#include <list>
#include <memory>
#include <string>

#include <persist/core/fsm/fsl.hpp>
#include <persist/core/metadata_manager.hpp>
#include <persist/core/transaction/transaction_manager.hpp>
#include <persist/utility/serializer.hpp>

using namespace persist;

class MetadataManagerTestFixture : public ::testing::Test {
protected:
  const uint64_t page_size = DEFAULT_PAGE_SIZE;
  // TODO: Check why minimum cache size of 3 and not 2 works?
  const uint64_t max_size = 3;
  const std::string data_connection_string = "file://test_metadata_manager";
  const std::string log_connection_string = "file://test_metadata_manager_log";
  std::unique_ptr<FSLPage> fsl_page;
  std::unique_ptr<Storage<RecordPage>> data_storage;
  std::unique_ptr<Storage<FSLPage>> fsl_storage;
  std::unique_ptr<Storage<LogPage>> log_storage;
  std::unique_ptr<BufferManager<RecordPage, LRUReplacer>> buffer_manager;
  std::unique_ptr<FSLManager> fsl_manager;
  std::unique_ptr<PageManager<RecordPage, LRUReplacer, FSLManager>>
      page_manager;
  std::unique_ptr<MetadataManager<LRUReplacer, FSLManager>> metadata_manager;
  std::unique_ptr<TransactionManager> txn_manager;

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

    // Setup page manager
    page_manager =
        std::make_unique<PageManager<RecordPage, LRUReplacer, FSLManager>>(
            *buffer_manager, *fsl_manager);

    // Setup transaction manager
    txn_manager = std::make_unique<TransactionManager>(
        *buffer_manager, log_connection_string, max_size);
    txn_manager->Start();

    // Setup metadata manager
    metadata_manager =
        std::make_unique<MetadataManager<LRUReplacer, FSLManager>>(
            *page_manager);
    metadata_manager->Start();
  }

  void TearDown() override {
    buffer_manager->Stop();
    fsl_manager->Stop();
    page_manager->Stop();
    txn_manager->Stop();
    metadata_manager->Stop();

    data_storage->Remove();
    fsl_storage->Remove();
    log_storage->Remove();
  }
};

TEST_F(MetadataManagerTestFixture, TestReadError) {
  Metadata metadata;

  auto txn = txn_manager->Begin();
  ASSERT_THROW(metadata_manager->Read(metadata, txn), MetadataNotFoundError);
  txn_manager->Commit(txn);
}

TEST_F(MetadataManagerTestFixture, TestInsertUpdateRead) {
  Metadata insert_metadata, update_metadata, read_metadata;
  insert_metadata.count = 1;
  insert_metadata.first = {1, 10};
  insert_metadata.last = {5, 1};

  {
    auto txn = txn_manager->Begin();
    metadata_manager->Insert(insert_metadata, txn);
    txn_manager->Commit(txn);
  }

  {
    auto txn = txn_manager->Begin();
    metadata_manager->Read(read_metadata, txn);
    txn_manager->Commit(txn);
  }

  ASSERT_EQ(insert_metadata, read_metadata);

  update_metadata.count = 10;
  update_metadata.first = {1, 10};
  update_metadata.last = {15, 5};

  {
    auto txn = txn_manager->Begin();
    metadata_manager->Update(update_metadata, txn);
    txn_manager->Commit(txn);
  }

  {
    auto txn = txn_manager->Begin();
    metadata_manager->Read(read_metadata, txn);
    txn_manager->Commit(txn);
  }

  ASSERT_EQ(update_metadata, read_metadata);
}
