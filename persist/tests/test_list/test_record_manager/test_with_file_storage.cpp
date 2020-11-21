/**
 * test_record_manager.cpp - Persist
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
 * @brief Record Manager Unit Tests
 */

#include <gtest/gtest.h>

#include <memory>

/**
 * Enabled intrusive testing
 */
#define PERSIST_INTRUSIVE_TESTING

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>
#include <persist/core/log_manager.hpp>
#include <persist/core/page_table.hpp>
#include <persist/core/storage/base.hpp>
#include <persist/core/transaction_manager.hpp>
#include <persist/list/record_manager.hpp>

using namespace persist;

class ListRecordManagerWithFileStorageTestFixture : public ::testing::Test {
private:
  class WrappedListRecordManager {
  public:
    ListRecordManager &manager;
    LogManager &logManager;
    TransactionManager txnManager;

    WrappedListRecordManager(ListRecordManager &manager, LogManager &logManager)
        : manager(manager), logManager(logManager),
          txnManager(manager.pageTable, logManager) {}

    void get(ByteBuffer &buffer, RecordLocation location) {
      Transaction txn = txnManager.begin();
      manager.get(txn, buffer, location);
      txnManager.commit(&txn);
    }

    RecordLocation insert(ByteBuffer &buffer) {
      Transaction txn = txnManager.begin();
      RecordLocation location = manager.insert(txn, buffer);
      txnManager.commit(&txn);

      return location;
    }

    void update(ByteBuffer &buffer, RecordLocation location) {
      Transaction txn = txnManager.begin();
      manager.update(txn, buffer, location);
      txnManager.commit(&txn);
    }

    void remove(RecordLocation location) {
      Transaction txn = txnManager.begin();
      manager.remove(txn, location);
      txnManager.commit(&txn);
    }
  };

protected:
  const uint64_t pageSize = DEFAULT_PAGE_SIZE;
  const uint64_t maxSize = 2;
  const ByteBuffer records[2] = {"testing_1"_bb, "testing_2"_bb};
  const std::string connetionString = "file://test_record_manager.storage";
  RecordBlock::Location locations[2];
  std::unique_ptr<Storage> storage;
  std::unique_ptr<PageTable> pageTable;
  std::unique_ptr<LogManager> logManager;
  std::unique_ptr<ListRecordManager> listRecordManager;
  std::unique_ptr<WrappedListRecordManager> manager;

  void SetUp() override {
    storage = Storage::create(connetionString);
    pageTable = std::make_unique<PageTable>(*storage, maxSize);
    logManager = std::make_unique<LogManager>();
    listRecordManager = std::make_unique<ListRecordManager>(*pageTable);
    manager = std::make_unique<WrappedListRecordManager>(*listRecordManager,
                                                         *logManager);
    insert();
    listRecordManager->start();
  }

  void TearDown() override {
    storage->remove();
    listRecordManager->stop();
  }

private:
  /**
   * @brief Insert data in backend storage for testing.
   */
  void insert() {
    storage->open();

    // Insert data
    MetaData metadata;
    metadata.pageSize = pageSize;
    metadata.numPages = 2;
    for (int i = 0; i < metadata.numPages; i++) {
      PageId pageId = i + 1;
      Page page(pageId, pageSize);
      RecordBlock recordBlock;
      recordBlock.data = records[i];
      locations[i].pageId = pageId;
      locations[i].slotId = page.addRecordBlock(recordBlock);
      metadata.freePages.insert(pageId);
      storage->write(page);
    }
    storage->write(metadata);

    storage->close();
  }
};

TEST_F(ListRecordManagerWithFileStorageTestFixture, TestGetSingleRecordBlock) {
  ByteBuffer record;

  manager->get(record, locations[0]);
  ASSERT_EQ(record, records[0]);
  record.clear();

  manager->get(record, locations[1]);
  ASSERT_EQ(record, records[1]);
}

TEST_F(ListRecordManagerWithFileStorageTestFixture, TestGetErrorNullLocation) {
  try {
    ByteBuffer record;
    RecordBlock::Location location;
    manager->get(record, location);
    FAIL() << "Expected RecordNotFoundError Exception.";
  } catch (RecordNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordNotFoundError Exception.";
  }
}
TEST_F(ListRecordManagerWithFileStorageTestFixture,
       TestGetErrorNonExistingLocation) {
  try {
    ByteBuffer record;
    RecordBlock::Location location;
    location.pageId = 10;
    manager->get(record, location);
    FAIL() << "Expected RecordNotFoundError Exception.";
  } catch (RecordNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordNotFoundError Exception.";
  }
}

TEST_F(ListRecordManagerWithFileStorageTestFixture,
       TestRemoveSingleRecordBlock) {
  ByteBuffer record;

  // Testing first record deleted
  manager->remove(locations[0]);
  ASSERT_THROW(manager->get(record, locations[0]), RecordNotFoundError);

  // Testing second record not touched
  record.clear();
  manager->get(record, locations[1]);
  ASSERT_EQ(record, records[1]);
}

TEST_F(ListRecordManagerWithFileStorageTestFixture,
       TestRemoveErrorNullLocation) {
  try {
    RecordBlock::Location location;
    manager->remove(location);
    FAIL() << "Expected RecordNotFoundError Exception.";
  } catch (RecordNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordNotFoundError Exception.";
  }
}
TEST_F(ListRecordManagerWithFileStorageTestFixture,
       TestRemoveErrorNonExistingLocation) {
  try {
    RecordBlock::Location location;
    location.pageId = 10;
    manager->remove(location);
    FAIL() << "Expected RecordNotFoundError Exception.";
  } catch (RecordNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordNotFoundError Exception.";
  }
}

TEST_F(ListRecordManagerWithFileStorageTestFixture,
       TestUpdateSingleRecordBlock) {
  ByteBuffer update = "testing_1-update"_bb;
  ByteBuffer record;

  // Testing first record update
  manager->update(update, locations[0]);
  manager->get(record, locations[0]);
  ASSERT_EQ(record, update);

  // Testing second record not touched
  record.clear();
  manager->get(record, locations[1]);
  ASSERT_EQ(record, records[1]);
}

TEST_F(ListRecordManagerWithFileStorageTestFixture,
       TestInsertAndGetMultiRecordBlock) {
  ByteBuffer input(2 * pageSize + 100, 'A');
  RecordBlock::Location location = manager->insert(input);

  ByteBuffer output;
  manager->get(output, location);

  ASSERT_EQ(output, input);
}

TEST_F(ListRecordManagerWithFileStorageTestFixture,
       TestInsertAndRemoveMultiRecordBlock) {
  ByteBuffer input(2 * pageSize + 100, 'A');
  RecordBlock::Location location = manager->insert(input);
  ByteBuffer record;

  // Testing record deleted
  manager->remove(location);
  ASSERT_THROW(manager->get(record, location), RecordNotFoundError);

  // Testing first record not touched
  record.clear();
  manager->get(record, locations[0]);
  ASSERT_EQ(record, records[0]);

  // Testing second record not touched
  record.clear();
  manager->get(record, locations[1]);
  ASSERT_EQ(record, records[1]);
}

TEST_F(ListRecordManagerWithFileStorageTestFixture,
       TestInsertAndUpdateMultiRecordBlock) {
  ByteBuffer input(2 * pageSize + 100, 'A'),
      updateIncrease(3 * pageSize + 100, 'A'),
      updateDecrease(1 * pageSize + 100, 'A');
  RecordBlock::Location location = manager->insert(input);
  ByteBuffer record;

  // Testing record update with increase in data length
  manager->update(updateIncrease, location);
  manager->get(record, location);
  ASSERT_EQ(record, updateIncrease);

  // Testing record update with decrease in data length
  manager->update(updateDecrease, location);
  record.clear();
  manager->get(record, location);
  ASSERT_EQ(record, updateDecrease);

  // Testing first record not touched
  record.clear();
  manager->get(record, locations[0]);
  ASSERT_EQ(record, records[0]);

  // Testing second record not touched
  record.clear();
  manager->get(record, locations[1]);
  ASSERT_EQ(record, records[1]);
}