/**
 * test_slotted_page.cpp - Persist
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
 * SlottedPage Unit Tests
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

/**
 * Enabled intrusive testing
 */
#define PERSIST_INTRUSIVE_TESTING

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>
#include <persist/core/page/slotted_page.hpp>

using namespace persist;
using ::testing::AtLeast;
using ::testing::Return;

/***********************************************
 * Slotted Page Header Unit Tests
 **********************************************/

class PageHeaderTestFixture : public ::testing::Test {
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
        SlottedPage::Header::Slot({1, DEFAULT_PAGE_SIZE - 10, 10});
    header->slots[2] =
        SlottedPage::Header::Slot({2, DEFAULT_PAGE_SIZE - 15, 5});
    header->slots[3] =
        SlottedPage::Header::Slot({3, DEFAULT_PAGE_SIZE - 18, 3});

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

TEST_F(PageHeaderTestFixture, TestLoad) {
  SlottedPage::Header _header;
  ByteBuffer _input;
  _input.insert(_input.end(), input.begin(), input.end());
  _input.insert(_input.end(), extra.begin(), extra.end());
  _header.load(Span(_input));

  ASSERT_EQ(_header.pageId, header->pageId);
  ASSERT_EQ(_header.slots.size(), header->slots.size());
  SlottedPage::Header::Slots::iterator _it = _header.slots.begin();
  SlottedPage::Header::Slots::iterator it = header->slots.begin();
  while (_it != _header.slots.end() && it != header->slots.end()) {
    ASSERT_EQ(_it->second.id, it->second.id);
    ASSERT_EQ(_it->second.offset, it->second.offset);
    ASSERT_EQ(_it->second.size, it->second.size);
    ++_it;
    ++it;
  }
}

TEST_F(PageHeaderTestFixture, TestLoadError) {
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

TEST_F(PageHeaderTestFixture, TestLoadCorruptErrorInvalidChecksum) {
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

TEST_F(PageHeaderTestFixture, TestLoadCorruptErrorInvalidSlotsCount) {
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

TEST_F(PageHeaderTestFixture, TestDump) {
  ByteBuffer output(header->size());
  header->dump(Span(output));

  ASSERT_EQ(input, output);
}

TEST_F(PageHeaderTestFixture, TestSize) {
  ASSERT_EQ(header->size(), input.size());
}

TEST_F(PageHeaderTestFixture, TestCreateSlot) {
  uint64_t size = 100;
  uint64_t tail = header->tail();
  PageSlotId slotId = header->createSlot(size);
  ASSERT_EQ(header->tail(), tail - size);
  ASSERT_EQ(slotId, 4);
  ASSERT_EQ(header->slots.rbegin()->second.id, slotId);
  ASSERT_EQ(header->slots.rbegin()->second.offset, DEFAULT_PAGE_SIZE - 118);
  ASSERT_EQ(header->slots.rbegin()->second.size, size);
}

TEST_F(PageHeaderTestFixture, TestUpdateSlot) {
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

TEST_F(PageHeaderTestFixture, TestFreeSlot) {
  uint64_t tail = header->tail();
  SlottedPage::Header::Slots::iterator it = header->slots.begin();
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

class MockSlottedPageObserver : public PageObserver {
public:
  MOCK_METHOD(void, handleModifiedPage, (PageId pageId), (override));
};

class SlottedPageTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  const PageId pageId = 12;
  const PageId nextPageId = 15;
  const PageId prevPageId = 1;
  const uint64_t pageSize = DEFAULT_PAGE_SIZE;
  std::unique_ptr<SlottedPage> page;
  PageSlotId slotId_1, slotId_2;
  std::unique_ptr<RecordBlock> recordBlock_1, recordBlock_2;
  const ByteBuffer recordBlockData_1 = "testing_1"_bb,
                   recordBlockData_2 = "testing_2"_bb;
  std::unique_ptr<LogManager> logManager;
  MockSlottedPageObserver observer;

  void SetUp() override {
    // Setup log manager
    logManager = std::make_unique<LogManager>();
    // Setup valid page
    page = std::make_unique<SlottedPage>(pageId, pageSize);
    page->setNextPageId(nextPageId);
    page->setPrevPageId(prevPageId);
    // Add record blocks
    Transaction txn(*logManager, 0);
    recordBlock_1 = std::make_unique<RecordBlock>();
    recordBlock_1->data = recordBlockData_1;
    slotId_1 = page->addRecordBlock(txn, *recordBlock_1).first;
    recordBlock_2 = std::make_unique<RecordBlock>();
    recordBlock_2->data = recordBlockData_2;
    slotId_2 = page->addRecordBlock(txn, *recordBlock_2).first;

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
  SlottedPage _block(pageId, pageSize);
  _block.setNextPageId(nextPageId);
  _block.setPrevPageId(prevPageId);

  ASSERT_EQ(_block.freeSpace(SlottedPage::Operation::UPDATE),
            pageSize - header.size());
  ASSERT_EQ(_block.freeSpace(SlottedPage::Operation::INSERT),
            pageSize - header.size() - sizeof(SlottedPage::Header::Slot));
}

TEST_F(SlottedPageTestFixture, TestGetRecordBlock) {
  Transaction txn(*logManager, 0);
  RecordBlock _recordBlock = page->getRecordBlock(txn, slotId_1);

  ASSERT_EQ(_recordBlock.data, recordBlockData_1);
  ASSERT_TRUE(_recordBlock.nextLocation().isNull());
  ASSERT_TRUE(_recordBlock.prevLocation().isNull());
}

TEST_F(SlottedPageTestFixture, TestGetRecordBlockError) {
  try {
    Transaction txn(*logManager, 0);
    RecordBlock _recordBlock = page->getRecordBlock(txn, 10);
    FAIL() << "Expected RecordBlockNotFoundError Exception.";
  } catch (RecordBlockNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordBlockNotFoundError Exception.";
  }
}

TEST_F(SlottedPageTestFixture, TestAddRecordBlock) {
  RecordBlock recordBlock;
  recordBlock.data = "testing_3"_bb;

  // Current free space in block
  page->registerObserver(&observer);
  EXPECT_CALL(observer, handleModifiedPage(page->getId())).Times(AtLeast(1));
  uint64_t oldFreeSpace = page->freeSpace(SlottedPage::Operation::INSERT);
  Transaction txn(*logManager, 0);
  PageSlotId slotId = page->addRecordBlock(txn, recordBlock).first;

  LogRecord logRecord = logManager->get(txn.prevSeqNumber);
  ASSERT_EQ(logRecord.header.transactionId, txn.getId());
  ASSERT_EQ(logRecord.header.seqNumber, txn.prevSeqNumber);
  ASSERT_EQ(logRecord.header.prevSeqNumber, 0);
  ASSERT_EQ(logRecord.type, LogRecord::Type::INSERT);
  ASSERT_EQ(logRecord.location, RecordBlock::Location(page->getId(), slotId));
  ASSERT_EQ(logRecord.recordBlockA, recordBlock);
  ASSERT_EQ(logRecord.recordBlockB, RecordBlock());

  uint64_t newFreeSize = page->header.tail() - page->header.size();
  ASSERT_EQ(oldFreeSpace - newFreeSize, recordBlock.size());
  ASSERT_EQ(page->getRecordBlock(txn, slotId), recordBlock);
}

TEST_F(SlottedPageTestFixture, TestUpdateRecordBlock) {
  RecordBlock recordBlock;
  recordBlock.data = "testing_1-update"_bb;
  RecordBlock recordBlockCopy = recordBlock;

  // Current free space in block
  page->registerObserver(&observer);
  EXPECT_CALL(observer, handleModifiedPage(page->getId())).Times(AtLeast(1));
  uint64_t oldFreeSpace = page->freeSpace(SlottedPage::Operation::UPDATE);
  Transaction txn(*logManager, 0);
  page->updateRecordBlock(txn, slotId_1, recordBlock);

  LogRecord logRecord = logManager->get(txn.prevSeqNumber);
  ASSERT_EQ(logRecord.header.transactionId, txn.getId());
  ASSERT_EQ(logRecord.header.seqNumber, txn.prevSeqNumber);
  ASSERT_EQ(logRecord.header.prevSeqNumber, 0);
  ASSERT_EQ(logRecord.type, LogRecord::Type::UPDATE);
  ASSERT_EQ(logRecord.location, RecordBlock::Location(page->getId(), slotId_1));
  ASSERT_EQ(logRecord.recordBlockA, *recordBlock_1);
  ASSERT_EQ(logRecord.recordBlockB, recordBlockCopy);

  uint64_t newFreeSize = page->header.tail() - page->header.size();
  RecordBlock recordBlock_;
  recordBlock_.data = "testing_1-update"_bb;
  ASSERT_EQ(oldFreeSpace - newFreeSize,
            recordBlock_.size() - recordBlock_1->size());
  ASSERT_EQ(page->getRecordBlock(txn, slotId_1), recordBlockCopy);
}

TEST_F(SlottedPageTestFixture, TestRemoveRecordBlock) {
  page->registerObserver(&observer);
  EXPECT_CALL(observer, handleModifiedPage(page->getId())).Times(AtLeast(1));
  uint64_t oldFreeSpace = page->freeSpace(SlottedPage::Operation::UPDATE);
  Transaction txn(*logManager, 0);
  page->removeRecordBlock(txn, slotId_2);

  LogRecord logRecord = logManager->get(txn.prevSeqNumber);
  ASSERT_EQ(logRecord.header.transactionId, txn.getId());
  ASSERT_EQ(logRecord.header.seqNumber, txn.prevSeqNumber);
  ASSERT_EQ(logRecord.header.prevSeqNumber, 0);
  ASSERT_EQ(logRecord.type, LogRecord::Type::DELETE);
  ASSERT_EQ(logRecord.location, RecordBlock::Location(page->getId(), slotId_2));
  ASSERT_EQ(logRecord.recordBlockA, *recordBlock_2);
  ASSERT_EQ(logRecord.recordBlockB, RecordBlock());

  uint64_t newFreeSize = page->header.tail() - page->header.size();
  ASSERT_THROW(page->getRecordBlock(txn, slotId_2), RecordBlockNotFoundError);
  ASSERT_EQ(newFreeSize - oldFreeSpace,
            recordBlock_2->size() + sizeof(SlottedPage::Header::Slot));
}

TEST_F(SlottedPageTestFixture, TestRemoveRecordBlockError) {
  try {
    Transaction txn(*logManager, 0);
    page->removeRecordBlock(txn, 20);
    FAIL() << "Expected RecordBlockNotFoundError Exception.";
  } catch (RecordBlockNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordBlockNotFoundError Exception.";
  }
}

TEST_F(SlottedPageTestFixture, TestLoad) {
  SlottedPage _block;
  _block.load(Span(input));

  ASSERT_EQ(_block.getId(), page->getId());

  Transaction txn(*logManager, 0);

  RecordBlock &_recordBlock_1 = _block.getRecordBlock(txn, slotId_1);
  ASSERT_EQ(_recordBlock_1.data, recordBlockData_1);
  ASSERT_TRUE(_recordBlock_1.nextLocation().isNull());
  ASSERT_TRUE(_recordBlock_1.prevLocation().isNull());

  RecordBlock &_recordBlock_2 = _block.getRecordBlock(txn, slotId_2);
  ASSERT_EQ(_recordBlock_2.data, recordBlockData_2);
  ASSERT_TRUE(_recordBlock_2.nextLocation().isNull());
  ASSERT_TRUE(_recordBlock_2.prevLocation().isNull());
}

TEST_F(SlottedPageTestFixture, TestLoadError) {
  try {
    ByteBuffer _input;
    RecordBlock _block;
    _block.load(Span(_input));
    FAIL() << "Expected RecordBlockParseError Exception.";
  } catch (RecordBlockParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordBlockParseError Exception.";
  }
}

TEST_F(SlottedPageTestFixture, TestDump) {
  ByteBuffer output(pageSize);
  page->dump(Span(output));

  ASSERT_EQ(input, output);
}
