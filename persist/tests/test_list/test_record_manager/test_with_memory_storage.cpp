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
#include <persist/list/record_manager.hpp>

using namespace persist;

class ListRecordManagerWithMemoryStorageTestFixture : public ::testing::Test {
protected:
  const uint64_t pageSize = DEFAULT_PAGE_SIZE;
  const uint64_t maxSize = 2;
  const ByteBuffer records[2] = {"testing_1"_bb, "testing_2"_bb};
  RecordBlock::Location locations[2];
  std::unique_ptr<Storage> storage;
  std::unique_ptr<PageTable> pageTable;
  std::unique_ptr<RecordManager> manager;

  void SetUp() override {
    storage = Storage::create("memory://");
    pageTable = std::make_unique<PageTable>(*storage, maxSize);
    manager = std::make_unique<ListRecordManager>(*pageTable);
    insert();
    manager->start();
  }

  void TearDown() override {
    storage->remove();
    manager->stop();
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

TEST_F(ListRecordManagerWithMemoryStorageTestFixture,
       TestGetSingleRecordBlock) {
  ByteBuffer record;

  manager->get(record, locations[0]);
  ASSERT_EQ(record, records[0]);
  record.clear();

  manager->get(record, locations[1]);
  ASSERT_EQ(record, records[1]);
}

TEST_F(ListRecordManagerWithMemoryStorageTestFixture,
       TestGetErrorNullLocation) {
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
TEST_F(ListRecordManagerWithMemoryStorageTestFixture,
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

TEST_F(ListRecordManagerWithMemoryStorageTestFixture,
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

TEST_F(ListRecordManagerWithMemoryStorageTestFixture,
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
TEST_F(ListRecordManagerWithMemoryStorageTestFixture,
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

TEST_F(ListRecordManagerWithMemoryStorageTestFixture,
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

TEST_F(ListRecordManagerWithMemoryStorageTestFixture,
       TestInsertAndGetMultiRecordBlock) {
  ByteBuffer input(2 * pageSize + 100, 'A');
  RecordBlock::Location location = manager->insert(input);

  ByteBuffer output;
  manager->get(output, location);

  ASSERT_EQ(output, input);
}

TEST_F(ListRecordManagerWithMemoryStorageTestFixture,
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

TEST_F(ListRecordManagerWithMemoryStorageTestFixture,
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