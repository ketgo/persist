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

#include <persist/core/defs.hpp>
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
  ByteBuffer input;
  const PageId pageId = 12;
  const PageId nextPageId = 15;
  const PageId prevPageId = 1;
  const uint64_t pageSize = DEFAULT_PAGE_SIZE;
  std::unique_ptr<Page> page;
  std::unique_ptr<Page::Header> pageHeader;
  PageSlotId slotId_1, slotId_2;
  std::unique_ptr<RecordBlock> recordBlock_1, recordBlock_2;
  const ByteBuffer recordBlockData_1 = {'t', 'e', 's', 't', 'i',
                                        'n', 'g', '_', '1'},
                   recordBlockData_2 = {'t', 'e', 's', 't', 'i',
                                        'n', 'g', '_', '2'};

  void SetUp() override {
    // Setup valid test header
    pageHeader = std::make_unique<Page::Header>(pageId, pageSize);
    pageHeader->nextPageId = nextPageId;
    pageHeader->prevPageId = prevPageId;

    // Setup valid page
    page = std::make_unique<Page>(pageId, pageSize);
    page->setNextPageId(nextPageId);
    page->setPrevPageId(prevPageId);
    // Add record blocks
    recordBlock_1 = std::make_unique<RecordBlock>();
    recordBlock_1->data = recordBlockData_1;
    slotId_1 = page->addRecordBlock(*recordBlock_1);
    pageHeader->createSlot(recordBlock_1->size());
    recordBlock_2 = std::make_unique<RecordBlock>();
    recordBlock_2->data = recordBlockData_2;
    slotId_2 = page->addRecordBlock(*recordBlock_2);
    pageHeader->createSlot(recordBlock_2->size());

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

TEST_F(PageTestFixture, TestGetId) { ASSERT_EQ(page->getId(), pageId); }

TEST_F(PageTestFixture, TestGetNextBlockId) {
  ASSERT_EQ(page->getNextPageId(), nextPageId);
}

TEST_F(PageTestFixture, TestSetNextBlockId) {
  PageId blockId = 99;
  page->setNextPageId(blockId);
  ASSERT_EQ(page->getNextPageId(), blockId);
}

TEST_F(PageTestFixture, TestGetPrevBlockId) {
  ASSERT_EQ(page->getPrevPageId(), prevPageId);
}

TEST_F(PageTestFixture, TestSetPrevBlockId) {
  PageId blockId = 99;
  page->setPrevPageId(blockId);
  ASSERT_EQ(page->getPrevPageId(), blockId);
}

TEST_F(PageTestFixture, TestFreeSpace) {
  Page::Header header(pageId, pageSize);
  header.nextPageId = nextPageId;
  header.prevPageId = prevPageId;
  Page _block(pageId, pageSize);
  _block.setNextPageId(nextPageId);
  _block.setPrevPageId(prevPageId);

  ASSERT_EQ(_block.freeSpace(), pageSize - header.size());
  ASSERT_EQ(_block.freeSpace(true),
            pageSize - header.size() - sizeof(Page::Header::Slot));
}

TEST_F(PageTestFixture, TestGetRecordBlock) {
  RecordBlock _recordBlock = page->getRecordBlock(slotId_1);

  ASSERT_EQ(_recordBlock.data, recordBlockData_1);
  ASSERT_TRUE(_recordBlock.nextLocation().isNull());
  ASSERT_TRUE(_recordBlock.prevLocation().isNull());
}

TEST_F(PageTestFixture, TestGetRecordBlockError) {
  try {
    RecordBlock _recordBlock = page->getRecordBlock(10);
    FAIL() << "Expected RecordBlockNotFoundError Exception.";
  } catch (RecordBlockNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordBlockNotFoundError Exception.";
  }
}

TEST_F(PageTestFixture, TestAddRecordBlock) {
  RecordBlock recordBlock_1;
  recordBlock_1.data = {'t', 'e', 's', 't', 'i', 'n', 'g', '_', '1'};
  RecordBlock recordBlock_2;
  recordBlock_2.data = {'t', 'e', 's', 't', 'i', 'n', 'g', '_', '2'};

  // Current free space in block
  uint64_t oldDataSize = pageSize - page->freeSpace() - pageHeader->size();
  page->addRecordBlock(recordBlock_1);
  pageHeader->createSlot(recordBlock_1.size());
  uint64_t newDataSize = pageSize - page->freeSpace() - pageHeader->size();
  ASSERT_TRUE(newDataSize - oldDataSize == recordBlock_1.size());

  oldDataSize = pageSize - page->freeSpace() - pageHeader->size();
  page->addRecordBlock(recordBlock_2);
  pageHeader->createSlot(recordBlock_2.size());
  newDataSize = pageSize - page->freeSpace() - pageHeader->size();
  ASSERT_TRUE(newDataSize - oldDataSize == recordBlock_2.size());
}

TEST_F(PageTestFixture, TestRemoveRecordBlock) {
  uint64_t oldDataSize = pageSize - page->freeSpace() - pageHeader->size();
  page->removeRecordBlock(slotId_2);
  pageHeader->slots.erase(std::prev(pageHeader->slots.end()));
  uint64_t newDataSize = pageSize - page->freeSpace() - pageHeader->size();
  ASSERT_THROW(page->getRecordBlock(slotId_2), RecordBlockNotFoundError);
  ASSERT_TRUE(oldDataSize - newDataSize == recordBlock_2->size());

  oldDataSize = pageSize - page->freeSpace() - pageHeader->size();
  page->removeRecordBlock(slotId_1);
  pageHeader->slots.erase(std::prev(pageHeader->slots.end()));
  newDataSize = pageSize - page->freeSpace() - pageHeader->size();
  ASSERT_THROW(page->getRecordBlock(slotId_1), RecordBlockNotFoundError);
  ASSERT_TRUE(oldDataSize - newDataSize == recordBlock_1->size());
}

TEST_F(PageTestFixture, TestRemoveRecordBlockError) {
  try {
    page->removeRecordBlock(20);
    FAIL() << "Expected RecordBlockNotFoundError Exception.";
  } catch (RecordBlockNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordBlockNotFoundError Exception.";
  }
}

TEST_F(PageTestFixture, TestLoad) {
  Page _block;
  _block.load(Span(input));

  ASSERT_EQ(_block.getId(), page->getId());

  RecordBlock &_recordBlock_1 = _block.getRecordBlock(slotId_1);
  ASSERT_EQ(_recordBlock_1.data, recordBlockData_1);
  ASSERT_TRUE(_recordBlock_1.nextLocation().isNull());
  ASSERT_TRUE(_recordBlock_1.prevLocation().isNull());

  RecordBlock &_recordBlock_2 = _block.getRecordBlock(slotId_2);
  ASSERT_EQ(_recordBlock_2.data, recordBlockData_2);
  ASSERT_TRUE(_recordBlock_2.nextLocation().isNull());
  ASSERT_TRUE(_recordBlock_2.prevLocation().isNull());
}

TEST_F(PageTestFixture, TestLoadError) {
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

TEST_F(PageTestFixture, TestDump) {
  ByteBuffer output(pageSize);
  page->dump(Span(output));

  ASSERT_EQ(input, output);
}
