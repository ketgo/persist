/**
 * test_ops_manager.cpp - Persist
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
 * OpsManager Integration Tests
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

/**
 * Enabled intrusive testing
 */
#define PERSIST_INTRUSIVE_TESTING

#include <persist/core/ops_manager.hpp>
#include <persist/core/page_table.hpp>
#include <persist/core/record_manager.hpp>
#include <persist/core/storage/memory_storage.hpp>

using namespace persist;
using ::testing::AtLeast;
using ::testing::Return;

class MockRecordManager : public RecordManager {
public:
  MockRecordManager(PageTable &pageTable) : RecordManager(pageTable) {}

  MOCK_METHOD(void, get, (ByteBuffer & buffer, RecordLocation location),
              (override));
  MOCK_METHOD(RecordLocation, insert, (ByteBuffer & buffer), (override));
  MOCK_METHOD(void, update, (ByteBuffer & buffer, RecordLocation location),
              (override));
  MOCK_METHOD(void, remove, (RecordLocation location), (override));
};

class OpsManagerTestFixture : public ::testing::Test {
protected:
  std::unique_ptr<OpsManager<MockRecordManager>> manager;
  std::unique_ptr<PageTable> pageTable;
  std::unique_ptr<MemoryStorage> storage;

  void SetUp() override {
    storage = std::make_unique<MemoryStorage>();
    pageTable = std::make_unique<PageTable>(*storage, 10);
    manager = std::make_unique<OpsManager<MockRecordManager>>(*pageTable);
    manager->start();
  }

  void TearDown() override {
    storage->remove();
    manager->stop();
  }
};

TEST_F(OpsManagerTestFixture, TestGet) {
  ByteBuffer buffer;
  RecordLocation location;

  EXPECT_CALL(manager->recordManager, get(buffer, location)).Times(AtLeast(1));
  manager->get(buffer, location);
}

TEST_F(OpsManagerTestFixture, TestInsert) {
  ByteBuffer buffer;
  RecordLocation location;

  EXPECT_CALL(manager->recordManager, insert(buffer))
      .Times(AtLeast(1))
      .WillRepeatedly(Return(location));
  ASSERT_EQ(manager->insert(buffer), location);
}

TEST_F(OpsManagerTestFixture, TestUpdate) {
  ByteBuffer buffer;
  RecordLocation location;

  EXPECT_CALL(manager->recordManager, update(buffer, location))
      .Times(AtLeast(1));
  manager->update(buffer, location);
}

TEST_F(OpsManagerTestFixture, TestRemove) {
  RecordLocation location;

  EXPECT_CALL(manager->recordManager, remove(location)).Times(AtLeast(1));
  manager->remove(location);
}
