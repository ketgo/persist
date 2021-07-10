/**
 * test_page.cpp - Persist
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
 * @brief Log Page Unit Test
 *
 */

#include <gtest/gtest.h>

#include <memory>

#include <persist/core/page/log_page/page.hpp>

#include "persist/test/mocks/page_observer.hpp"

using namespace persist;
using namespace persist::test;

using ::testing::AtLeast;
using ::testing::Return;

/***********************************************
 * Log Page Header Unit Tests
 **********************************************/

class LogPageHeaderTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  ByteBuffer extra;
  const PageId page_id = 12;
  const SeqNumber last_seq_number = 1;
  const uint64_t slot_count = 10;
  std::unique_ptr<LogPage::Header> header;

  void SetUp() override {
    header = std::make_unique<LogPage::Header>(page_id);
    header->last_seq_number = last_seq_number;
    header->slot_count = slot_count;

    input = {12, 0, 0, 0, 0,  0, 0, 0, 1, 0, 0, 0,
             0,  0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0};
    extra = {42, 0, 0, 0, 21, 48, 4};
  }
};

TEST_F(LogPageHeaderTestFixture, TestLoad) {
  LogPage::Header _header;
  ByteBuffer _input;
  _input.insert(_input.end(), input.begin(), input.end());
  _input.insert(_input.end(), extra.begin(), extra.end());
  _header.Load(_input);

  ASSERT_EQ(_header.page_id, header->page_id);
  ASSERT_EQ(_header.last_seq_number, header->last_seq_number);
  ASSERT_EQ(_header.slot_count, header->slot_count);
}

TEST_F(LogPageHeaderTestFixture, TestLoadError) {
  try {
    ByteBuffer _input;
    LogPage::Header _header;
    _header.Load(_input);
    FAIL() << "Expected PageParseError Exception.";
  } catch (PageParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageParseError Exception.";
  }
}

TEST_F(LogPageHeaderTestFixture, TestDump) {
  ByteBuffer output(header->GetStorageSize());
  header->Dump(output);

  ASSERT_EQ(input, output);
}

TEST_F(LogPageHeaderTestFixture, TestSize) {
  ASSERT_EQ(header->GetStorageSize(), input.size());
}

/***********************************************
 * LogPage Unit Tests
 ***********************************************/

class LogPageTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  const PageId page_id = 12;
  const uint64_t page_size = DEFAULT_PAGE_SIZE;
  std::unique_ptr<LogPage> page;
  SeqNumber seq_number_1 = 1, seq_number_2 = 2;
  LogPageSlot::Location next_location_1 = {10, 4}, next_location_2 = {0, 0};
  std::unique_ptr<LogPageSlot> page_slot_1, page_slot_2;
  const ByteBuffer page_slot_data_1 = "testing_1"_bb,
                   page_slot_data_2 = "testing_2"_bb;
  MockPageObserver observer;

  void SetUp() override {
    // Setup valid page
    page = std::make_unique<LogPage>(page_id, page_size);
    // Add record blocks
    page_slot_1 = std::make_unique<LogPageSlot>(seq_number_1, next_location_1);
    page_slot_1->data = page_slot_data_1;
    page->InsertPageSlot(*page_slot_1);
    page_slot_2 = std::make_unique<LogPageSlot>(seq_number_2, next_location_2);
    page_slot_2->data = page_slot_data_2;
    page->InsertPageSlot(*page_slot_2);

    input = {
        12,  0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   2,
        0,   0,   0,   0,  0,  0, 0, 2, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  9, 0, 0, 0, 0, 0, 0,   0,   116, 101, 115, 116,
        105, 110, 103, 95, 50, 1, 0, 0, 0, 0, 0, 0,   0,   10,  0,   0,   0,
        0,   0,   0,   0,  4,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   9,  0,  0, 0, 0, 0, 0, 0, 116, 101, 115, 116, 105, 110,
        103, 95,  49,  0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,  0,  0, 0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,
        0,   0,   0,   0};
  }
};

TEST_F(LogPageTestFixture, TestGetId) { ASSERT_EQ(page->GetId(), page_id); }

TEST_F(LogPageTestFixture, TestFreeSpace) {
  LogPage::Header header(page_id, page_size);
  LogPage _page(page_id, page_size);

  ASSERT_EQ(_page.GetFreeSpaceSize(Operation::UPDATE),
            page_size - header.GetStorageSize() -
                LogPageSlot::GetFixedStorageSize());
  ASSERT_EQ(_page.GetFreeSpaceSize(Operation::INSERT),
            page_size - header.GetStorageSize() -
                LogPageSlot::GetFixedStorageSize());
}

TEST_F(LogPageTestFixture, TestGetPageSlot) {
  auto _page_slot = page->GetPageSlot(seq_number_1);

  ASSERT_EQ(_page_slot.data, page_slot_data_1);
  ASSERT_EQ(_page_slot.GetNextLocation(), next_location_1);
}

TEST_F(LogPageTestFixture, TestGetPageSlotError) {
  ASSERT_THROW(page->GetPageSlot(20), PageSlotNotFoundError);
}

TEST_F(LogPageTestFixture, TestInsertPageSlot) {
  SeqNumber seq_number = 100;
  LogPageSlot page_slot(seq_number);
  page_slot.data = "testing_3"_bb;

  // Current free space in block
  page->RegisterObserver(&observer);
  EXPECT_CALL(observer, HandleModifiedPage(testing::Ref(*page)))
      .Times(AtLeast(1));
  size_t old_free_space = page->GetFreeSpaceSize(Operation::INSERT);
  page->InsertPageSlot(page_slot);
  size_t new_free_size = page->GetFreeSpaceSize(Operation::INSERT);
  ASSERT_EQ(old_free_space - new_free_size, page_slot.GetStorageSize());
  ASSERT_EQ(page->GetPageSlot(seq_number), page_slot);
}

TEST_F(LogPageTestFixture, TestLoad) {
  LogPage _page;
  _page.Load(input);

  ASSERT_EQ(_page.GetId(), page->GetId());

  LogPageSlot _page_slot_1 = _page.GetPageSlot(seq_number_1);
  ASSERT_EQ(_page_slot_1.data, page_slot_data_1);
  ASSERT_EQ(_page_slot_1.GetNextLocation(), next_location_1);

  LogPageSlot _page_slot_2 = _page.GetPageSlot(seq_number_2);
  ASSERT_EQ(_page_slot_2.data, page_slot_data_2);
  ASSERT_EQ(_page_slot_2.GetNextLocation(), next_location_2);
}

TEST_F(LogPageTestFixture, TestLoadError) {
  ByteBuffer _input;
  LogPage _page;
  ASSERT_THROW(_page.Load(_input), PageParseError);
}

TEST_F(LogPageTestFixture, TestDump) {
  ByteBuffer output(page_size);
  page->Dump(output);

  ASSERT_EQ(input, output);
}
