/**
 * test_vls_slotted_page.cpp - Persist
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
 * @brief SlottedPage unit tests
 *
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

/**
 * Enabled intrusive testing
 */
#define PERSIST_INTRUSIVE_TESTING

#include <persist/core/page/slotted_page/slotted_page.hpp>
#include <persist/core/storage/factory.hpp>

#include "persist/test/mocks.hpp"

using namespace persist;
using namespace persist::test;

using ::testing::AtLeast;
using ::testing::Return;

/***********************************************
 * Slotted Page Header Unit Tests
 **********************************************/

class SlottedPageHeaderTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  ByteBuffer extra;
  const PageId pageId = 12;
  const PageId nextPageId = 15;
  const PageId prevPageId = 1;
  std::unique_ptr<SlottedPage::Header> header;

  void SetUp() override {
    header = std::make_unique<SlottedPage::Header>(pageId);
    // setup valid header
    header->nextPageId = nextPageId;
    header->prevPageId = prevPageId;
    header->slots[1] =
        SlottedPage::Header::HeaderSlot({1, DEFAULT_PAGE_SIZE - 10, 10});
    header->slots[2] =
        SlottedPage::Header::HeaderSlot({2, DEFAULT_PAGE_SIZE - 15, 5});
    header->slots[3] =
        SlottedPage::Header::HeaderSlot({3, DEFAULT_PAGE_SIZE - 18, 3});

    input = {12,  0, 0, 0, 0, 0, 0, 0, 15,  0,   0,   0,  0,  0,   0,  0,
             1,   0, 0, 0, 0, 0, 0, 0, 3,   0,   0,   0,  0,  0,   0,  0,
             1,   0, 0, 0, 0, 0, 0, 0, 246, 3,   0,   0,  0,  0,   0,  0,
             10,  0, 0, 0, 0, 0, 0, 0, 2,   0,   0,   0,  0,  0,   0,  0,
             241, 3, 0, 0, 0, 0, 0, 0, 5,   0,   0,   0,  0,  0,   0,  0,
             3,   0, 0, 0, 0, 0, 0, 0, 238, 3,   0,   0,  0,  0,   0,  0,
             3,   0, 0, 0, 0, 0, 0, 0, 142, 141, 188, 43, 11, 216, 12, 107};
    extra = {42, 0, 0, 0, 21, 48, 4};
  }
};

TEST_F(SlottedPageHeaderTestFixture, TestLoad) {
  SlottedPage::Header _header;
  ByteBuffer _input;
  _input.insert(_input.end(), input.begin(), input.end());
  _input.insert(_input.end(), extra.begin(), extra.end());
  _header.load(Span(_input));

  ASSERT_EQ(_header.pageId, header->pageId);
  ASSERT_EQ(_header.slots.size(), header->slots.size());
  SlottedPage::Header::HeaderSlotMap::iterator _it = _header.slots.begin();
  SlottedPage::Header::HeaderSlotMap::iterator it = header->slots.begin();
  while (_it != _header.slots.end() && it != header->slots.end()) {
    ASSERT_EQ(_it->second.id, it->second.id);
    ASSERT_EQ(_it->second.offset, it->second.offset);
    ASSERT_EQ(_it->second.size, it->second.size);
    ++_it;
    ++it;
  }
}

TEST_F(SlottedPageHeaderTestFixture, TestLoadError) {
  try {
    ByteBuffer _input;
    SlottedPage::Header _header;
    _header.load(Span(_input));
    FAIL() << "Expected PageParseError Exception.";
  } catch (PageParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageParseError Exception.";
  }
}

TEST_F(SlottedPageHeaderTestFixture, TestLoadCorruptErrorInvalidChecksum) {
  try {
    ByteBuffer _input = input;
    _input.back() = 0;
    SlottedPage::Header _header;
    _header.load(Span(_input));
    FAIL() << "Expected PageCorruptError Exception.";
  } catch (PageCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageCorruptError Exception.";
  }
}

TEST_F(SlottedPageHeaderTestFixture, TestLoadCorruptErrorInvalidSlotsCount) {
  try {
    ByteBuffer _input = input;
    _input[24] = 9; //<- sets the slot count located at 24th byte to 9
    SlottedPage::Header _header;
    _header.load(Span(_input));
    FAIL() << "Expected PageCorruptError Exception.";
  } catch (PageCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageCorruptError Exception.";
  }
}

TEST_F(SlottedPageHeaderTestFixture, TestDump) {
  ByteBuffer output(header->size());
  header->dump(Span(output));

  ASSERT_EQ(input, output);
}

TEST_F(SlottedPageHeaderTestFixture, TestSize) {
  ASSERT_EQ(header->size(), input.size());
}

TEST_F(SlottedPageHeaderTestFixture, TestCreateSlot) {
  uint64_t size = 100;
  uint64_t tail = header->tail();
  PageSlotId slotId = header->createSlot(size);
  ASSERT_EQ(header->tail(), tail - size);
  ASSERT_EQ(slotId, 4);
  ASSERT_EQ(header->slots.rbegin()->second.id, slotId);
  ASSERT_EQ(header->slots.rbegin()->second.offset, DEFAULT_PAGE_SIZE - 118);
  ASSERT_EQ(header->slots.rbegin()->second.size, size);
}

TEST_F(SlottedPageHeaderTestFixture, TestUpdateSlot) {
  uint64_t oldSize = header->slots.at(2).size;
  uint64_t newSize = 100;
  uint64_t tail = header->tail();

  header->updateSlot(2, newSize);

  // Test update in tail
  ASSERT_EQ(header->tail(), tail + (oldSize - newSize));

  // Test no change in first slot
  ASSERT_EQ(header->slots.at(1).id, 1);
  ASSERT_EQ(header->slots.at(1).offset, DEFAULT_PAGE_SIZE - 10);
  ASSERT_EQ(header->slots.at(1).size, 10);

  // Test change in 2nd slot
  ASSERT_EQ(header->slots.at(2).id, 2);
  ASSERT_EQ(header->slots.at(2).offset, DEFAULT_PAGE_SIZE - 110);
  ASSERT_EQ(header->slots.at(2).size, newSize);

  // Test change in 3rd slot
  ASSERT_EQ(header->slots.at(3).id, 3);
  ASSERT_EQ(header->slots.at(3).offset, DEFAULT_PAGE_SIZE - 113);
  ASSERT_EQ(header->slots.at(3).size, 3);
}

TEST_F(SlottedPageHeaderTestFixture, TestFreeSlot) {
  uint64_t tail = header->tail();
  SlottedPage::Header::HeaderSlotMap::iterator it = header->slots.begin();
  ++it;
  uint64_t entrySize = it->second.size;

  header->freeSlot(it->second.id);
  ASSERT_EQ(header->tail(), tail + entrySize);
}

/***********************************************
 * Slotted Page Unit Tests
 ***********************************************/

TEST(SlottedPageTest, PageSizeError) {
  try {
    SlottedPage page(1, 64);
    FAIL() << "Expected PageSizeError Exception.";
  } catch (PageSizeError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageSizeError Exception.";
  }
}

class SlottedPageTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  const PageId pageId = 12;
  const PageId nextPageId = 15;
  const PageId prevPageId = 1;
  const uint64_t pageSize = DEFAULT_PAGE_SIZE;
  std::unique_ptr<SlottedPage> page;
  PageSlotId slotId_1, slotId_2;
  std::unique_ptr<SlottedPageSlot> pageSlot_1, pageSlot_2;
  const ByteBuffer pageSlotData_1 = "testing_1"_bb,
                   pageSlotData_2 = "testing_2"_bb;
  std::unique_ptr<Storage<LogPage>> storage;
  // TODO: Use Mock LogManager
  std::unique_ptr<LogManager> logManager;
  MockPageObserver observer;

  void SetUp() override {
    // Setup log manager
    storage = createStorage<LogPage>("file://test_vls_storage_log");
    logManager = std::make_unique<LogManager>(storage.get(), 2);
    logManager->start();

    // Setup valid page
    page = std::make_unique<SlottedPage>(pageId, pageSize);
    page->setNextPageId(nextPageId);
    page->setPrevPageId(prevPageId);
    // Add record blocks
    Transaction txn(logManager.get(), 0);
    pageSlot_1 = std::make_unique<SlottedPageSlot>();
    pageSlot_1->data = pageSlotData_1;
    slotId_1 = page->insertPageSlot(*pageSlot_1, txn).first;
    pageSlot_2 = std::make_unique<SlottedPageSlot>();
    pageSlot_2->data = pageSlotData_2;
    slotId_2 = page->insertPageSlot(*pageSlot_2, txn).first;

    input = {
        12,  0,   0,   0,   0,   0,   0,   0,   15,  0,   0,   0,   0,   0,
        0,   0,   1,   0,   0,   0,   0,   0,   0,   0,   2,   0,   0,   0,
        0,   0,   0,   0,   1,   0,   0,   0,   0,   0,   0,   0,   207, 3,
        0,   0,   0,   0,   0,   0,   49,  0,   0,   0,   0,   0,   0,   0,
        2,   0,   0,   0,   0,   0,   0,   0,   158, 3,   0,   0,   0,   0,
        0,   0,   49,  0,   0,   0,   0,   0,   0,   0,   143, 202, 183, 195,
        60,  97,  29,  198, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   31,  137, 223, 200, 40,  173, 239, 136,
        116, 101, 115, 116, 105, 110, 103, 95,  50,  0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   16,
        137, 223, 200, 40,  173, 239, 136, 116, 101, 115, 116, 105, 110, 103,
        95,  49};
  }

  void TearDown() override {
    storage->remove();
    logManager->stop();
  }
};

TEST_F(SlottedPageTestFixture, TestGetId) { ASSERT_EQ(page->getId(), pageId); }

TEST_F(SlottedPageTestFixture, TestGetNextBlockId) {
  ASSERT_EQ(page->getNextPageId(), nextPageId);
}

TEST_F(SlottedPageTestFixture, TestSetNextBlockId) {
  PageId blockId = 99;
  page->registerObserver(&observer);
  EXPECT_CALL(observer, handleModifiedPage(page->getId())).Times(AtLeast(1));
  page->setNextPageId(blockId);
  ASSERT_EQ(page->getNextPageId(), blockId);
}

TEST_F(SlottedPageTestFixture, TestGetPrevBlockId) {
  ASSERT_EQ(page->getPrevPageId(), prevPageId);
}

TEST_F(SlottedPageTestFixture, TestSetPrevBlockId) {
  PageId blockId = 99;
  page->registerObserver(&observer);
  EXPECT_CALL(observer, handleModifiedPage(page->getId())).Times(AtLeast(1));
  page->setPrevPageId(blockId);
  ASSERT_EQ(page->getPrevPageId(), blockId);
}

TEST_F(SlottedPageTestFixture, TestFreeSpace) {
  SlottedPage::Header header(pageId, pageSize);
  header.nextPageId = nextPageId;
  header.prevPageId = prevPageId;
  SlottedPage _page(pageId, pageSize);
  _page.setNextPageId(nextPageId);
  _page.setPrevPageId(prevPageId);

  ASSERT_EQ(_page.freeSpace(SlottedPage::Operation::UPDATE),
            pageSize - header.size());
  ASSERT_EQ(_page.freeSpace(SlottedPage::Operation::INSERT),
            pageSize - header.size() - sizeof(SlottedPage::Header::HeaderSlot));
}

TEST_F(SlottedPageTestFixture, TestGetPageSlot) {
  Transaction txn(logManager.get(), 0);
  SlottedPageSlot _pageSlot = page->getPageSlot(slotId_1, txn);

  ASSERT_EQ(_pageSlot.data, pageSlotData_1);
  ASSERT_TRUE(_pageSlot.getNextLocation().isNull());
  ASSERT_TRUE(_pageSlot.getPrevLocation().isNull());
}

TEST_F(SlottedPageTestFixture, TestGetPageSlotError) {
  try {
    Transaction txn(logManager.get(), 0);
    SlottedPageSlot _pageSlot = page->getPageSlot(10, txn);
    FAIL() << "Expected PageSlotNotFoundError Exception.";
  } catch (PageSlotNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageSlotNotFoundError Exception.";
  }
}

TEST_F(SlottedPageTestFixture, TestAddPageSlot) {
  SlottedPageSlot pageSlot;
  pageSlot.data = "testing_3"_bb;

  // Current free space in block
  page->registerObserver(&observer);
  EXPECT_CALL(observer, handleModifiedPage(page->getId())).Times(AtLeast(1));
  uint64_t oldFreeSpace = page->freeSpace(SlottedPage::Operation::INSERT);
  Transaction txn(logManager.get(), 0);
  PageSlotId slotId = page->insertPageSlot(pageSlot, txn).first;

  auto logRecord = logManager->get(txn.logLocation);
  ASSERT_EQ(logRecord->header.transactionId, txn.getId());
  ASSERT_EQ(logRecord->header.seqNumber, txn.logLocation.seqNumber);
  ASSERT_EQ(logRecord->header.prevLogRecordLocation.seqNumber, 0);
  ASSERT_EQ(logRecord->type, LogRecord::Type::INSERT);
  ASSERT_EQ(logRecord->location, SlottedPageSlot::Location(page->getId(), slotId));
  ASSERT_EQ(logRecord->pageSlotA, pageSlot);
  ASSERT_EQ(logRecord->pageSlotB, SlottedPageSlot());

  uint64_t newFreeSize = page->header.tail() - page->header.size();
  ASSERT_EQ(oldFreeSpace - newFreeSize, pageSlot.size());
  ASSERT_EQ(page->getPageSlot(slotId, txn), pageSlot);
}

TEST_F(SlottedPageTestFixture, TestUpdatePageSlot) {
  SlottedPageSlot pageSlot;
  pageSlot.data = "testing_1-update"_bb;
  SlottedPageSlot pageSlotCopy = pageSlot;

  // Current free space in block
  page->registerObserver(&observer);
  EXPECT_CALL(observer, handleModifiedPage(page->getId())).Times(AtLeast(1));
  uint64_t oldFreeSpace = page->freeSpace(SlottedPage::Operation::UPDATE);
  Transaction txn(logManager.get(), 0);
  page->updatePageSlot(slotId_1, pageSlot, txn);

  auto logRecord = logManager->get(txn.logLocation);
  ASSERT_EQ(logRecord->header.transactionId, txn.getId());
  ASSERT_EQ(logRecord->header.seqNumber, txn.logLocation.seqNumber);
  ASSERT_EQ(logRecord->header.prevLogRecordLocation.seqNumber, 0);
  ASSERT_EQ(logRecord->type, LogRecord::Type::UPDATE);
  ASSERT_EQ(logRecord->location, SlottedPageSlot::Location(page->getId(), slotId_1));
  ASSERT_EQ(logRecord->pageSlotA, *pageSlot_1);
  ASSERT_EQ(logRecord->pageSlotB, pageSlotCopy);

  uint64_t newFreeSize = page->header.tail() - page->header.size();
  SlottedPageSlot pageSlot_;
  pageSlot_.data = "testing_1-update"_bb;
  ASSERT_EQ(oldFreeSpace - newFreeSize, pageSlot_.size() - pageSlot_1->size());
  ASSERT_EQ(page->getPageSlot(slotId_1, txn), pageSlotCopy);
}

TEST_F(SlottedPageTestFixture, TestRemovePageSlot) {
  page->registerObserver(&observer);
  EXPECT_CALL(observer, handleModifiedPage(page->getId())).Times(AtLeast(1));
  uint64_t oldFreeSpace = page->freeSpace(SlottedPage::Operation::UPDATE);
  Transaction txn(logManager.get(), 0);
  page->removePageSlot(slotId_2, txn);

  auto logRecord = logManager->get(txn.logLocation);
  ASSERT_EQ(logRecord->header.transactionId, txn.getId());
  ASSERT_EQ(logRecord->header.seqNumber, txn.logLocation.seqNumber);
  ASSERT_EQ(logRecord->header.prevLogRecordLocation.seqNumber, 0);
  ASSERT_EQ(logRecord->type, LogRecord::Type::DELETE);
  ASSERT_EQ(logRecord->location, SlottedPageSlot::Location(page->getId(), slotId_2));
  ASSERT_EQ(logRecord->pageSlotA, *pageSlot_2);
  ASSERT_EQ(logRecord->pageSlotB, SlottedPageSlot());

  uint64_t newFreeSize = page->header.tail() - page->header.size();
  ASSERT_THROW(page->getPageSlot(slotId_2, txn), PageSlotNotFoundError);
  ASSERT_EQ(newFreeSize - oldFreeSpace,
            pageSlot_2->size() + sizeof(SlottedPage::Header::HeaderSlot));
}

TEST_F(SlottedPageTestFixture, TestRemovePageSlotError) {
  try {
    Transaction txn(logManager.get(), 0);
    page->removePageSlot(20, txn);
    FAIL() << "Expected PageSlotNotFoundError Exception.";
  } catch (PageSlotNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageSlotNotFoundError Exception.";
  }
}

TEST_F(SlottedPageTestFixture, TestLoad) {
  SlottedPage _page;
  _page.load(Span(input));

  ASSERT_EQ(_page.getId(), page->getId());

  Transaction txn(logManager.get(), 0);

  SlottedPageSlot _pageSlot_1 = _page.getPageSlot(slotId_1, txn);
  ASSERT_EQ(_pageSlot_1.data, pageSlotData_1);
  ASSERT_TRUE(_pageSlot_1.getNextLocation().isNull());
  ASSERT_TRUE(_pageSlot_1.getPrevLocation().isNull());

  SlottedPageSlot _pageSlot_2 = _page.getPageSlot(slotId_2, txn);
  ASSERT_EQ(_pageSlot_2.data, pageSlotData_2);
  ASSERT_TRUE(_pageSlot_2.getNextLocation().isNull());
  ASSERT_TRUE(_pageSlot_2.getPrevLocation().isNull());
}

TEST_F(SlottedPageTestFixture, TestLoadError) {
  try {
    ByteBuffer _input(pageSize);
    SlottedPage _page;
    _page.load(Span(_input));
    FAIL() << "Expected PageCorruptError Exception.";
  } catch (PageCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageCorruptError Exception.";
  }
}

TEST_F(SlottedPageTestFixture, TestDump) {
  ByteBuffer output(pageSize);
  page->dump(Span(output));

  ASSERT_EQ(input, output);
}
