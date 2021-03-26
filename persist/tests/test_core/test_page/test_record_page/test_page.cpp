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
 * @brief RecordPage unit tests
 *
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

/**
 * Enabled intrusive testing
 */
#define PERSIST_INTRUSIVE_TESTING

#include <persist/core/page/record_page/page.hpp>
#include <persist/core/storage/creator.hpp>

#include "persist/test/mocks/page_observer.hpp"

using namespace persist;
using namespace persist::test;

using ::testing::AtLeast;
using ::testing::Return;

/***********************************************
 * Slotted Page Header Unit Tests
 **********************************************/

class RecordPageHeaderTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  ByteBuffer extra;
  const PageId page_id = 12;
  const PageId next_page_id = 15;
  const PageId prev_page_id = 1;
  std::unique_ptr<RecordPage::Header> header;

  void SetUp() override {
    header = std::make_unique<RecordPage::Header>(page_id);
    // setup valid header
    header->next_page_id = next_page_id;
    header->prev_page_id = prev_page_id;
    header->slots[1] =
        RecordPage::Header::SlotSpan({DEFAULT_PAGE_SIZE - 10, 10});
    header->slots[2] =
        RecordPage::Header::SlotSpan({DEFAULT_PAGE_SIZE - 15, 5});
    header->slots[3] =
        RecordPage::Header::SlotSpan({DEFAULT_PAGE_SIZE - 18, 3});

    input = {12, 0, 0, 0, 0,   0, 0, 0, 15, 0, 0,   0, 0,  0, 0, 0, 1,   0,
             0,  0, 0, 0, 0,   0, 3, 0, 0,  0, 0,   0, 0,  0, 1, 0, 0,   0,
             0,  0, 0, 0, 246, 3, 0, 0, 0,  0, 0,   0, 10, 0, 0, 0, 0,   0,
             0,  0, 2, 0, 0,   0, 0, 0, 0,  0, 241, 3, 0,  0, 0, 0, 0,   0,
             5,  0, 0, 0, 0,   0, 0, 0, 3,  0, 0,   0, 0,  0, 0, 0, 238, 3,
             0,  0, 0, 0, 0,   0, 3, 0, 0,  0, 0,   0, 0,  0};
    extra = {42, 0, 0, 0, 21, 48, 4};
  }
};

TEST_F(RecordPageHeaderTestFixture, TestLoad) {
  RecordPage::Header _header;
  ByteBuffer _input;
  _input.insert(_input.end(), input.begin(), input.end());
  _input.insert(_input.end(), extra.begin(), extra.end());
  _header.Load(_input);

  ASSERT_EQ(_header.page_id, header->page_id);
  ASSERT_EQ(_header.slots.size(), header->slots.size());
  RecordPage::Header::SlotSpanMap::iterator _it = _header.slots.begin();
  RecordPage::Header::SlotSpanMap::iterator it = header->slots.begin();
  while (_it != _header.slots.end() && it != header->slots.end()) {
    ASSERT_EQ(_it->first, it->first);
    ASSERT_EQ(_it->second.offset, it->second.offset);
    ASSERT_EQ(_it->second.size, it->second.size);
    ++_it;
    ++it;
  }
}

TEST_F(RecordPageHeaderTestFixture, TestLoadError) {
  ByteBuffer _input;
  RecordPage::Header _header;
  ASSERT_THROW(_header.Load(_input), PageParseError);
}

TEST_F(RecordPageHeaderTestFixture, TestDump) {
  ByteBuffer output(header->GetStorageSize());
  header->Dump(output);

  ASSERT_EQ(input, output);
}

TEST_F(RecordPageHeaderTestFixture, TestSize) {
  ASSERT_EQ(header->GetStorageSize(), input.size());
}

TEST_F(RecordPageHeaderTestFixture, TestCreateSlot) {
  size_t size = 100;
  size_t tail = header->GetTail();
  PageSlotId slot_id = header->CreateSlot(size);
  ASSERT_EQ(header->GetTail(), tail - size);
  ASSERT_EQ(slot_id, 4);
  ASSERT_EQ(header->slots.rbegin()->first, slot_id);
  ASSERT_EQ(header->slots.rbegin()->second.offset, DEFAULT_PAGE_SIZE - 118);
  ASSERT_EQ(header->slots.rbegin()->second.size, size);
}

TEST_F(RecordPageHeaderTestFixture, TestUpdateSlot) {
  size_t old_size = header->slots.at(2).size;
  size_t new_size = 100;
  size_t tail = header->GetTail();

  header->UpdateSlot(2, new_size);

  // Test update in tail
  ASSERT_EQ(header->GetTail(), tail + (old_size - new_size));

  // Test no change in first slot
  ASSERT_EQ(header->slots.at(1).offset, DEFAULT_PAGE_SIZE - 10);
  ASSERT_EQ(header->slots.at(1).size, 10);

  // Test change in 2nd slot
  ASSERT_EQ(header->slots.at(2).offset, DEFAULT_PAGE_SIZE - 110);
  ASSERT_EQ(header->slots.at(2).size, new_size);

  // Test change in 3rd slot
  ASSERT_EQ(header->slots.at(3).offset, DEFAULT_PAGE_SIZE - 113);
  ASSERT_EQ(header->slots.at(3).size, 3);
}

TEST_F(RecordPageHeaderTestFixture, TestFreeSlot) {
  uint64_t tail = header->GetTail();
  RecordPage::Header::SlotSpanMap::iterator it = header->slots.begin();
  ++it;
  size_t entry_size = it->second.size;

  header->FreeSlot(it->first);
  ASSERT_EQ(header->GetTail(), tail + entry_size);
}

/***********************************************
 * Slotted Page Unit Tests
 ***********************************************/

class RecordPageTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  const PageId page_id = 12;
  const PageId next_page_id = 15;
  const PageId prev_page_id = 1;
  const uint64_t page_size = DEFAULT_PAGE_SIZE;
  const size_t single_slot_span_size =
      sizeof(RecordPage::Header::SlotSpan) + sizeof(PageSlotId);
  std::unique_ptr<RecordPage> page;
  PageSlotId slot_id_1, slot_id_2;
  std::unique_ptr<RecordPageSlot> page_slot_1, page_slot_2;
  const ByteBuffer page_slot_date_1 = "testing_1"_bb,
                   page_slot_date_2 = "testing_2"_bb;
  const std::string connection_string = "file://test_slotted_page_log";
  std::unique_ptr<Storage<LogPage>> storage;
  // TODO: Use Mock LogManager
  std::unique_ptr<LogManager> log_manager;
  MockPageObserver observer;

  void SetUp() override {
    // Setup log manager
    storage = persist::CreateStorage<LogPage>(connection_string);
    log_manager = std::make_unique<LogManager>(*storage, 2);
    log_manager->Start();

    // Setup valid page
    page = std::make_unique<RecordPage>(page_id, page_size);
    page->SetNextPageId(next_page_id);
    page->SetPrevPageId(prev_page_id);
    // Add record blocks
    Transaction txn(*log_manager, 0);
    page_slot_1 = std::make_unique<RecordPageSlot>();
    page_slot_1->data = page_slot_date_1;
    slot_id_1 = page->InsertPageSlot(*page_slot_1, txn).first;
    page_slot_2 = std::make_unique<RecordPageSlot>();
    page_slot_2->data = page_slot_date_2;
    slot_id_2 = page->InsertPageSlot(*page_slot_2, txn).first;

    input = {12,  0,   0,   0,   0,   0,  0,  0, 15, 0, 0,   0,   0,   0,   0,
             0,   1,   0,   0,   0,   0,  0,  0, 0,  2, 0,   0,   0,   0,   0,
             0,   0,   1,   0,   0,   0,  0,  0, 0,  0, 199, 3,   0,   0,   0,
             0,   0,   0,   57,  0,   0,  0,  0, 0,  0, 0,   2,   0,   0,   0,
             0,   0,   0,   0,   142, 3,  0,  0, 0,  0, 0,   0,   57,  0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   9,  0,  0, 0,  0, 0,   0,   0,   116, 101,
             115, 116, 105, 110, 103, 95, 50, 0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   0,   0,   0,   0,  0,  0, 0,  0, 0,   0,   0,   0,   0,
             0,   0,   9,   0,   0,   0,  0,  0, 0,  0, 116, 101, 115, 116, 105,
             110, 103, 95,  49};
  }

  void TearDown() override {
    storage->Remove();
    log_manager->Stop();
  }
};

TEST_F(RecordPageTestFixture, TestGetId) { ASSERT_EQ(page->GetId(), page_id); }

TEST_F(RecordPageTestFixture, TestGetNextBlockId) {
  ASSERT_EQ(page->GetNextPageId(), next_page_id);
}

TEST_F(RecordPageTestFixture, TestSetNextBlockId) {
  PageId blockId = 99;
  page->RegisterObserver(&observer);
  EXPECT_CALL(observer, HandleModifiedPage(testing::Ref(*page)))
      .Times(AtLeast(1));
  page->SetNextPageId(blockId);
  ASSERT_EQ(page->GetNextPageId(), blockId);
}

TEST_F(RecordPageTestFixture, TestGetPrevBlockId) {
  ASSERT_EQ(page->GetPrevPageId(), prev_page_id);
}

TEST_F(RecordPageTestFixture, TestSetPrevBlockId) {
  PageId blockId = 99;
  page->RegisterObserver(&observer);
  EXPECT_CALL(observer, HandleModifiedPage(testing::Ref(*page)))
      .Times(AtLeast(1));
  page->SetPrevPageId(blockId);
  ASSERT_EQ(page->GetPrevPageId(), blockId);
}

TEST_F(RecordPageTestFixture, TestFreeSpace) {
  RecordPage::Header header(page_id, page_size);
  header.next_page_id = next_page_id;
  header.prev_page_id = prev_page_id;
  RecordPage _page(page_id, page_size);
  _page.SetNextPageId(next_page_id);
  _page.SetPrevPageId(prev_page_id);

  ASSERT_EQ(_page.GetFreeSpaceSize(Operation::UPDATE),
            page_size - header.GetStorageSize());
  ASSERT_EQ(_page.GetFreeSpaceSize(Operation::INSERT),
            page_size - header.GetStorageSize() - single_slot_span_size);
}

TEST_F(RecordPageTestFixture, TestGetPageSlot) {
  Transaction txn(*log_manager, 0);
  RecordPageSlot _page_slot = page->GetPageSlot(slot_id_1, txn);

  ASSERT_EQ(_page_slot.data, page_slot_date_1);
  ASSERT_TRUE(_page_slot.GetNextLocation().IsNull());
  ASSERT_TRUE(_page_slot.GetPrevLocation().IsNull());
}

TEST_F(RecordPageTestFixture, TestGetPageSlotError) {
  Transaction txn(*log_manager, 0);
  ASSERT_THROW(page->GetPageSlot(10, txn), PageSlotNotFoundError);
}

TEST_F(RecordPageTestFixture, TestAddPageSlot) {
  RecordPageSlot page_slot;
  page_slot.data = "testing_3"_bb;

  // Current free space in block
  page->RegisterObserver(&observer);
  EXPECT_CALL(observer, HandleModifiedPage(testing::Ref(*page)))
      .Times(AtLeast(1));
  size_t old_free_space = page->GetFreeSpaceSize(Operation::INSERT);
  Transaction txn(*log_manager, 0);
  PageSlotId slot_id = page->InsertPageSlot(page_slot, txn).first;

  auto log_record = log_manager->Get(txn.log_location);
  ASSERT_EQ(log_record->header.transaction_id, txn.GetId());
  ASSERT_EQ(log_record->header.seq_number, txn.log_location.seq_number);
  ASSERT_EQ(log_record->header.prev_log_record_location.seq_number, 0);
  ASSERT_EQ(log_record->type, LogRecord::Type::INSERT);
  ASSERT_EQ(log_record->location,
            RecordPageSlot::Location(page->GetId(), slot_id));
  ASSERT_EQ(log_record->page_slot_a, page_slot);
  ASSERT_EQ(log_record->page_slot_b, RecordPageSlot());

  size_t new_free_size = page->header.GetTail() - page->header.GetStorageSize();
  ASSERT_EQ(old_free_space - new_free_size, page_slot.GetStorageSize());
  ASSERT_EQ(page->GetPageSlot(slot_id, txn), page_slot);
}

TEST_F(RecordPageTestFixture, TestUpdatePageSlot) {
  RecordPageSlot page_slot;
  page_slot.data = "testing_1-update"_bb;
  RecordPageSlot page_slot_copy = page_slot;

  // Current free space in block
  page->RegisterObserver(&observer);
  EXPECT_CALL(observer, HandleModifiedPage(testing::Ref(*page)))
      .Times(AtLeast(1));
  size_t old_free_space = page->GetFreeSpaceSize(Operation::UPDATE);
  Transaction txn(*log_manager, 0);
  page->UpdatePageSlot(slot_id_1, page_slot, txn);

  auto log_record = log_manager->Get(txn.log_location);
  ASSERT_EQ(log_record->header.transaction_id, txn.GetId());
  ASSERT_EQ(log_record->header.seq_number, txn.log_location.seq_number);
  ASSERT_EQ(log_record->header.prev_log_record_location.seq_number, 0);
  ASSERT_EQ(log_record->type, LogRecord::Type::UPDATE);
  ASSERT_EQ(log_record->location,
            RecordPageSlot::Location(page->GetId(), slot_id_1));
  ASSERT_EQ(log_record->page_slot_a, *page_slot_1);
  ASSERT_EQ(log_record->page_slot_b, page_slot_copy);

  size_t new_free_size = page->header.GetTail() - page->header.GetStorageSize();
  RecordPageSlot page_slot_;
  page_slot_.data = "testing_1-update"_bb;
  ASSERT_EQ(old_free_space - new_free_size,
            page_slot_.GetStorageSize() - page_slot_1->GetStorageSize());
  ASSERT_EQ(page->GetPageSlot(slot_id_1, txn), page_slot_copy);
}

TEST_F(RecordPageTestFixture, TestRemovePageSlot) {
  page->RegisterObserver(&observer);
  EXPECT_CALL(observer, HandleModifiedPage(testing::Ref(*page)))
      .Times(AtLeast(1));
  size_t old_free_space = page->GetFreeSpaceSize(Operation::UPDATE);
  Transaction txn(*log_manager, 0);
  page->RemovePageSlot(slot_id_2, txn);

  auto log_record = log_manager->Get(txn.log_location);
  ASSERT_EQ(log_record->header.transaction_id, txn.GetId());
  ASSERT_EQ(log_record->header.seq_number, txn.log_location.seq_number);
  ASSERT_EQ(log_record->header.prev_log_record_location.seq_number, 0);
  ASSERT_EQ(log_record->type, LogRecord::Type::DELETE);
  ASSERT_EQ(log_record->location,
            RecordPageSlot::Location(page->GetId(), slot_id_2));
  ASSERT_EQ(log_record->page_slot_a, *page_slot_2);
  ASSERT_EQ(log_record->page_slot_b, RecordPageSlot());

  size_t new_free_size = page->header.GetTail() - page->header.GetStorageSize();
  ASSERT_THROW(page->GetPageSlot(slot_id_2, txn), PageSlotNotFoundError);
  ASSERT_EQ(new_free_size - old_free_space,
            page_slot_2->GetStorageSize() + single_slot_span_size);
}

TEST_F(RecordPageTestFixture, TestRemovePageSlotError) {
  Transaction txn(*log_manager, 0);
  ASSERT_THROW(page->RemovePageSlot(20, txn), PageSlotNotFoundError);
}

TEST_F(RecordPageTestFixture, TestLoad) {
  RecordPage _page;
  _page.Load(input);

  ASSERT_EQ(_page.GetId(), page->GetId());

  Transaction txn(*log_manager, 0);

  RecordPageSlot _page_slot_1 = _page.GetPageSlot(slot_id_1, txn);
  ASSERT_EQ(_page_slot_1.data, page_slot_date_1);
  ASSERT_TRUE(_page_slot_1.GetNextLocation().IsNull());
  ASSERT_TRUE(_page_slot_1.GetPrevLocation().IsNull());

  RecordPageSlot _page_slot_2 = _page.GetPageSlot(slot_id_2, txn);
  ASSERT_EQ(_page_slot_2.data, page_slot_date_2);
  ASSERT_TRUE(_page_slot_2.GetNextLocation().IsNull());
  ASSERT_TRUE(_page_slot_2.GetPrevLocation().IsNull());
}

TEST_F(RecordPageTestFixture, TestLoadError) {
  ByteBuffer _input;
  RecordPage _page;
  ASSERT_THROW(_page.Load(_input), PageParseError);
}

TEST_F(RecordPageTestFixture, TestDump) {
  ByteBuffer output(page_size);
  page->Dump(output);

  ASSERT_EQ(input, output);
}
