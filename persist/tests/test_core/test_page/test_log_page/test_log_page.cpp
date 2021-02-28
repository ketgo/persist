/**
 * test_log_page.cpp - Persist
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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

/**
 * Enabled intrusive testing
 */
#define PERSIST_INTRUSIVE_TESTING

#include <persist/core/page/log_page/log_page.hpp>

#include "persist/test/mocks.hpp"

using namespace persist;
using namespace persist::test;

using ::testing::AtLeast;
using ::testing::Return;

/***********************************************
 * VLS Slotted Page Header Unit Tests
 **********************************************/

class LogPageHeaderTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  ByteBuffer extra;
  const PageId pageId = 12;
  const SeqNumber lastSeqNumber = 1;
  const uint64_t slotCount = 10;
  std::unique_ptr<LogPage::Header> header;

  void SetUp() override {
    header = std::make_unique<LogPage::Header>(pageId);
    header->lastSeqNumber = lastSeqNumber;
    header->slotCount = slotCount;

    input = {12, 0, 0, 0, 0, 0, 0, 0, 1,   0,  0,   0,   0,  0,  0, 0,
             10, 0, 0, 0, 0, 0, 0, 0, 113, 96, 163, 251, 22, 10, 0, 0};
    extra = {42, 0, 0, 0, 21, 48, 4};
  }
};

TEST_F(LogPageHeaderTestFixture, TestLoad) {
  LogPage::Header _header;
  ByteBuffer _input;
  _input.insert(_input.end(), input.begin(), input.end());
  _input.insert(_input.end(), extra.begin(), extra.end());
  _header.load(Span(_input));

  ASSERT_EQ(_header.pageId, header->pageId);
  ASSERT_EQ(_header.lastSeqNumber, header->lastSeqNumber);
  ASSERT_EQ(_header.slotCount, header->slotCount);
}

TEST_F(LogPageHeaderTestFixture, TestLoadError) {
  try {
    ByteBuffer _input;
    LogPage::Header _header;
    _header.load(Span(_input));
    FAIL() << "Expected PageParseError Exception.";
  } catch (PageParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageParseError Exception.";
  }
}

TEST_F(LogPageHeaderTestFixture, TestLoadCorruptErrorInvalidChecksum) {
  try {
    ByteBuffer _input = input;
    _input[0] = 0;
    LogPage::Header _header;
    _header.load(Span(_input));
    FAIL() << "Expected PageCorruptError Exception.";
  } catch (PageCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageCorruptError Exception.";
  }
}

TEST_F(LogPageHeaderTestFixture, TestDump) {
  ByteBuffer output(header->size());
  header->dump(Span(output));

  ASSERT_EQ(input, output);
}

TEST_F(LogPageHeaderTestFixture, TestSize) {
  ASSERT_EQ(header->size(), input.size());
}

/***********************************************
 * LogPage Unit Tests
 ***********************************************/

TEST(LogPageTest, PageSizeError) {
  try {
    LogPage page(1, 64);
    FAIL() << "Expected PageSizeError Exception.";
  } catch (PageSizeError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageSizeError Exception.";
  }
}

class LogPageTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  const PageId pageId = 12;
  const uint64_t pageSize = DEFAULT_PAGE_SIZE;
  std::unique_ptr<LogPage> page;
  SeqNumber seqNumber_1 = 1, seqNumber_2 = 2;
  LogPageSlot::Location nextLocation_1 = {10, 4}, nextLocation_2 = {0, 0};
  std::unique_ptr<LogPageSlot> pageSlot_1, pageSlot_2;
  const ByteBuffer pageSlotData_1 = "testing_1"_bb,
                   pageSlotData_2 = "testing_2"_bb;
  MockPageObserver observer;

  void SetUp() override {
    // Setup valid page
    page = std::make_unique<LogPage>(pageId, pageSize);
    // Add record blocks
    pageSlot_1 = std::make_unique<LogPageSlot>(seqNumber_1, nextLocation_1);
    pageSlot_1->data = pageSlotData_1;
    page->insertPageSlot(*pageSlot_1);
    pageSlot_2 = std::make_unique<LogPageSlot>(seqNumber_2, nextLocation_2);
    pageSlot_2->data = pageSlotData_2;
    page->insertPageSlot(*pageSlot_2);

    input = {
        12, 0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 2,
        0,  0, 0,   0,   0,   0,   0,   184, 97,  163, 251, 22, 10, 0, 0, 2, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   125, 223, 192, 198, 83,  130, 2,  0,  9, 0, 0, 0,
        0,  0, 0,   0,   116, 101, 115, 116, 105, 110, 103, 95, 50, 1, 0, 0, 0,
        0,  0, 0,   0,   10,  0,   0,   0,   0,   0,   0,   0,  4,  0, 0, 0, 0,
        0,  0, 0,   97,  186, 172, 198, 83,  130, 2,   0,   9,  0,  0, 0, 0, 0,
        0,  0, 116, 101, 115, 116, 105, 110, 103, 95,  49,  0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,  0, 0, 0, 0,
        0,  0, 0,   0};
  }
};

TEST_F(LogPageTestFixture, TestGetId) { ASSERT_EQ(page->getId(), pageId); }

TEST_F(LogPageTestFixture, TestFreeSpace) {
  LogPage::Header header(pageId, pageSize);
  LogPage _page(pageId, pageSize);

  ASSERT_EQ(_page.freeSpace(LogPage::Operation::UPDATE),
            pageSize - header.size());
  ASSERT_EQ(_page.freeSpace(LogPage::Operation::INSERT),
            pageSize - header.size());
}

TEST_F(LogPageTestFixture, TestGetPageSlot) {
  LogPageSlot _pageSlot = page->getPageSlot(seqNumber_1);

  ASSERT_EQ(_pageSlot.data, pageSlotData_1);
  ASSERT_EQ(_pageSlot.getNextLocation(), nextLocation_1);
}

TEST_F(LogPageTestFixture, TestGetPageSlotError) {
  try {
    LogPageSlot _pageSlot = page->getPageSlot(20);
    FAIL() << "Expected PageSlotNotFoundError Exception.";
  } catch (PageSlotNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageSlotNotFoundError Exception.";
  }
}

TEST_F(LogPageTestFixture, TestInsertPageSlot) {
  SeqNumber seqNumber = 100;
  LogPageSlot pageSlot(seqNumber);
  pageSlot.data = "testing_3"_bb;

  // Current free space in block
  page->registerObserver(&observer);
  EXPECT_CALL(observer, handleModifiedPage(page->getId())).Times(AtLeast(1));
  uint64_t oldFreeSpace = page->freeSpace(LogPage::Operation::INSERT);
  page->insertPageSlot(pageSlot);
  uint64_t newFreeSize = page->freeSpace(LogPage::Operation::INSERT);
  ASSERT_EQ(oldFreeSpace - newFreeSize, pageSlot.size());
  ASSERT_EQ(page->getPageSlot(seqNumber), pageSlot);
}

TEST_F(LogPageTestFixture, TestLoad) {
  LogPage _page;
  _page.load(Span(input));

  ASSERT_EQ(_page.getId(), page->getId());

  LogPageSlot _pageSlot_1 = _page.getPageSlot(seqNumber_1);
  ASSERT_EQ(_pageSlot_1.data, pageSlotData_1);
  ASSERT_EQ(_pageSlot_1.getNextLocation(), nextLocation_1);

  LogPageSlot _pageSlot_2 = _page.getPageSlot(seqNumber_2);
  ASSERT_EQ(_pageSlot_2.data, pageSlotData_2);
  ASSERT_EQ(_pageSlot_2.getNextLocation(), nextLocation_2);
}

TEST_F(LogPageTestFixture, TestLoadError) {
  try {
    ByteBuffer _input(pageSize);
    LogPage _page;
    _page.load(Span(_input));
    FAIL() << "Expected PageCorruptError Exception.";
  } catch (PageCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageCorruptError Exception.";
  }
}

TEST_F(LogPageTestFixture, TestDump) {
  ByteBuffer output(pageSize);
  page->dump(Span(output));

  ASSERT_EQ(input, output);
}
