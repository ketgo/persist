/**
 * test_block.cpp - Persist
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
 * Data Block Unit Tests
 */

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include <persist/core/exceptions.hpp>
#include <persist/core/page.hpp>

using namespace persist;

TEST(DataBlockTest, PageSizeError) {
  try {
    Page page(1, 64);
    FAIL() << "Expected PageSizeError Exception.";
  } catch (PageSizeError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageSizeError Exception.";
  }
}

class PageTestFixture : public ::testing::Test {
protected:
  std::vector<uint8_t> input;
  const PageId pageId = 12;
  const PageId nextPageId = 15;
  const PageId prevPageId = 1;
  const uint64_t pageSize = DEFAULT_PAGE_SIZE;
  std::unique_ptr<Page> block;
  std::unique_ptr<Page::Header> pageHeader;
  PageSlotId slotId_1, slotId_2;
  std::unique_ptr<RecordBlock> recordBlock_1, recordBlock_2;
  const std::string recordBlockData_1 = "testing_1",
                    recordBlockData_2 = "testing_2";

  void SetUp() override {
    // Setup valid test header
    pageHeader = std::make_unique<Page::Header>(pageId, pageSize);
    pageHeader->nextPageId = nextPageId;
    pageHeader->prevPageId = prevPageId;

    // Setup valid bock
    block = std::make_unique<Page>(pageId, pageSize);
    block->setNextPageId(nextPageId);
    block->setPrevPageId(prevPageId);
    // Add record blocks
    recordBlock_1 = std::make_unique<RecordBlock>();
    recordBlock_1->data = recordBlockData_1;
    slotId_1 = block->addRecordBlock(*recordBlock_1);
    pageHeader->createSlot(recordBlock_1->size());
    recordBlock_2 = std::make_unique<RecordBlock>();
    recordBlock_2->data = recordBlockData_2;
    slotId_2 = block->addRecordBlock(*recordBlock_2);
    pageHeader->createSlot(recordBlock_2->size());

    input = {
        123, 105, 10,  110, 101, 120, 116, 80,  97,  103, 101, 73,  100, 105,
        15,  105, 6,   112, 97,  103, 101, 73,  100, 105, 12,  105, 8,   112,
        97,  103, 101, 83,  105, 122, 101, 73,  4,   0,   105, 10,  112, 114,
        101, 118, 80,  97,  103, 101, 73,  100, 105, 1,   105, 5,   115, 108,
        111, 116, 115, 91,  123, 105, 2,   105, 100, 105, 1,   105, 6,   111,
        102, 102, 115, 101, 116, 73,  3,   189, 105, 4,   115, 105, 122, 101,
        105, 67,  125, 123, 105, 2,   105, 100, 105, 2,   105, 6,   111, 102,
        102, 115, 101, 116, 73,  3,   122, 105, 4,   115, 105, 122, 101, 105,
        67,  125, 93,  125, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
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
        0,   0,   0,   0,   0,   0,   0,   0,   123, 105, 4,   110, 101, 120,
        116, 123, 105, 6,   112, 97,  103, 101, 73,  100, 105, 0,   105, 6,
        115, 108, 111, 116, 73,  100, 105, 0,   125, 105, 4,   112, 114, 101,
        118, 123, 105, 6,   112, 97,  103, 101, 73,  100, 105, 0,   105, 6,
        115, 108, 111, 116, 73,  100, 105, 0,   125, 125, 116, 101, 115, 116,
        105, 110, 103, 95,  50,  123, 105, 4,   110, 101, 120, 116, 123, 105,
        6,   112, 97,  103, 101, 73,  100, 105, 0,   105, 6,   115, 108, 111,
        116, 73,  100, 105, 0,   125, 105, 4,   112, 114, 101, 118, 123, 105,
        6,   112, 97,  103, 101, 73,  100, 105, 0,   105, 6,   115, 108, 111,
        116, 73,  100, 105, 0,   125, 125, 116, 101, 115, 116, 105, 110, 103,
        95,  49};
  }
};

TEST_F(PageTestFixture, TestGetId) { ASSERT_EQ(block->getId(), pageId); }

TEST_F(PageTestFixture, TestGetNextBlockId) {
  ASSERT_EQ(block->getNextPageId(), nextPageId);
}

TEST_F(PageTestFixture, TestSetNextBlockId) {
  PageId blockId = 99;
  block->setNextPageId(blockId);
  ASSERT_EQ(block->getNextPageId(), blockId);
}

TEST_F(PageTestFixture, TestGetPrevBlockId) {
  ASSERT_EQ(block->getPrevPageId(), prevPageId);
}

TEST_F(PageTestFixture, TestSetPrevBlockId) {
  PageId blockId = 99;
  block->setPrevPageId(blockId);
  ASSERT_EQ(block->getPrevPageId(), blockId);
}

TEST_F(PageTestFixture, TestFreeSpace) {
  Page::Header header(pageId, pageSize);
  header.nextPageId = nextPageId;
  header.prevPageId = prevPageId;
  Page _block(pageId, pageSize);
  _block.setNextPageId(nextPageId);
  _block.setPrevPageId(prevPageId);

  ASSERT_EQ(_block.freeSpace(), pageSize - header.size());
}

TEST_F(PageTestFixture, TestGetRecordBlock) {
  RecordBlock _recordBlock = block->getRecordBlock(slotId_1);

  ASSERT_EQ(_recordBlock.data, recordBlockData_1);
  ASSERT_TRUE(_recordBlock.getNextLocation().is_null());
  ASSERT_TRUE(_recordBlock.getPrevLocation().is_null());
}

TEST_F(PageTestFixture, TestGetRecordBlockError) {
  try {
    RecordBlock _recordBlock = block->getRecordBlock(10);
    FAIL() << "Expected RecordBlockNotFoundError Exception.";
  } catch (RecordBlockNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordBlockNotFoundError Exception.";
  }
}

TEST_F(PageTestFixture, TestAddRecordBlock) {
  RecordBlock recordBlock_1;
  recordBlock_1.data = "testing_1";
  RecordBlock recordBlock_2;
  recordBlock_2.data = "testing_2";

  // Current free space in block
  uint64_t oldDataSize = pageSize - block->freeSpace() - pageHeader->size();
  block->addRecordBlock(recordBlock_1);
  pageHeader->createSlot(recordBlock_1.size());
  uint64_t newDataSize = pageSize - block->freeSpace() - pageHeader->size();
  ASSERT_TRUE(newDataSize - oldDataSize == recordBlock_1.size());

  oldDataSize = pageSize - block->freeSpace() - pageHeader->size();
  block->addRecordBlock(recordBlock_2);
  pageHeader->createSlot(recordBlock_2.size());
  newDataSize = pageSize - block->freeSpace() - pageHeader->size();
  ASSERT_TRUE(newDataSize - oldDataSize == recordBlock_2.size());

  ASSERT_EQ(block->isModified(), true);
}

TEST_F(PageTestFixture, TestRemoveRecordBlock) {
  uint64_t oldDataSize = pageSize - block->freeSpace() - pageHeader->size();
  block->removeRecordBlock(slotId_2);
  pageHeader->slots.pop_back();
  uint64_t newDataSize = pageSize - block->freeSpace() - pageHeader->size();
  ASSERT_THROW(block->getRecordBlock(slotId_2), RecordBlockNotFoundError);
  ASSERT_TRUE(oldDataSize - newDataSize == recordBlock_2->size());

  oldDataSize = pageSize - block->freeSpace() - pageHeader->size();
  block->removeRecordBlock(slotId_1);
  pageHeader->slots.pop_back();
  newDataSize = pageSize - block->freeSpace() - pageHeader->size();
  ASSERT_THROW(block->getRecordBlock(slotId_1), RecordBlockNotFoundError);
  ASSERT_TRUE(oldDataSize - newDataSize == recordBlock_1->size());

  ASSERT_EQ(block->isModified(), true);
}

TEST_F(PageTestFixture, TestRemoveRecordBlockError) {
  try {
    block->removeRecordBlock(20);
    FAIL() << "Expected RecordBlockNotFoundError Exception.";
  } catch (RecordBlockNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordBlockNotFoundError Exception.";
  }
}

TEST_F(PageTestFixture, TestLoad) {
  Page _block;
  _block.load(input);

  ASSERT_EQ(_block.getId(), block->getId());

  RecordBlock &_recordBlock_1 = _block.getRecordBlock(slotId_1);
  ASSERT_EQ(_recordBlock_1.data, recordBlockData_1);
  ASSERT_TRUE(_recordBlock_1.getNextLocation().is_null());
  ASSERT_TRUE(_recordBlock_1.getPrevLocation().is_null());

  RecordBlock &_recordBlock_2 = _block.getRecordBlock(slotId_2);
  ASSERT_EQ(_recordBlock_2.data, recordBlockData_2);
  ASSERT_TRUE(_recordBlock_2.getNextLocation().is_null());
  ASSERT_TRUE(_recordBlock_2.getPrevLocation().is_null());

  ASSERT_EQ(_block.isModified(), false);
}

TEST_F(PageTestFixture, TestLoadError) {
  try {
    std::vector<uint8_t> _input;
    RecordBlock _block;
    _block.load(_input);
    FAIL() << "Expected RecordBlockParseError Exception.";
  } catch (RecordBlockParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordBlockParseError Exception.";
  }
}

TEST_F(PageTestFixture, TestDump) {
  ByteBuffer &output = block->dump();

  ASSERT_EQ(input, output);
  ASSERT_EQ(output.size(), pageSize);
}
