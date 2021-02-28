/**
 * page/test_serializer.cpp - Persist
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
 * @brief Page Serializer Unit Test
 *
 */

#include <gtest/gtest.h>

#include <memory>

/**
 * Enabled intrusive testing
 */
#define PERSIST_INTRUSIVE_TESTING

#include <persist/core/page/serializer.hpp>

#include "persist/test/mocks.hpp"

using namespace persist;
using namespace persist::test;

class PageSerializerTestFixture : public ::testing::Test {
protected:
  ByteBuffer inputLogPage;
  std::unique_ptr<LogPage> logPage;
  const PageId logPageId = 12;
  const uint64_t logPageSize = DEFAULT_PAGE_SIZE;
  SeqNumber seqNumber = 1;
  LogPageSlot::Location nextLocation = {10, 4};
  std::unique_ptr<LogPageSlot> logPageSlot;
  const ByteBuffer logPageSlotData = "testing"_bb;

  ByteBuffer inputSlottedPage;
  std::unique_ptr<SlottedPage> slottedPage;
  const PageId slottedPageId = 12;
  const PageId nextSlottedPageId = 15;
  const PageId prevSlottedPageId = 1;
  const uint64_t slottedPageSize = DEFAULT_PAGE_SIZE;

  MockPageObserver observer;

  void SetUp() override {
    // Setup log page
    logPage = createPage<LogPage>(logPageId, logPageSize);
    // Add record blocks
    logPageSlot = std::make_unique<LogPageSlot>(seqNumber, nextLocation);
    logPageSlot->data = logPageSlotData;
    logPage->insertPageSlot(*logPageSlot);
    // Input buffer
    inputLogPage = {
        1, 0, 0, 0,   0,   0,   0,   0,   174, 125, 55, 158, 0, 0, 0, 0, 12,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 1, 0,
        0, 0, 0, 0,   0,   0,   185, 97,  163, 251, 22, 10,  0, 0, 1, 0, 0,
        0, 0, 0, 0,   0,   10,  0,   0,   0,   0,   0,  0,   0, 4, 0, 0, 0,
        0, 0, 0, 0,   99,  186, 172, 198, 83,  130, 2,  0,   7, 0, 0, 0, 0,
        0, 0, 0, 116, 101, 115, 116, 105, 110, 103, 0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0,  0,   0, 0, 0, 0, 0,
        0, 0, 0, 0};

    // Setup slotted page
    slottedPage = createPage<SlottedPage>(slottedPageId, slottedPageSize);
    slottedPage->setNextPageId(nextSlottedPageId);
    slottedPage->setPrevPageId(prevSlottedPageId);
    // Input buffer
    inputSlottedPage = {
        2, 0, 0, 0, 0,  0, 0, 0, 175, 125, 55, 158, 0,  0,   0, 0, 12, 0, 0, 0,
        0, 0, 0, 0, 15, 0, 0, 0, 0,   0,   0,  0,   1,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 206, 147, 91, 191, 83, 130, 2, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0, 0,   0,   0,  0,   0,  0,   0, 0, 0,  0, 0, 0,
        0, 0, 0, 0};
  }
};

TEST_F(PageSerializerTestFixture, TestLoadLogPage) {
  auto page = loadPage(inputLogPage);

  ASSERT_EQ(page->getTypeId(), logPage->getTypeId());
  ASSERT_EQ(page->getId(), logPage->getId());

  LogPageSlot pageSlot =
      static_cast<LogPage *>(page.get())->getPageSlot(seqNumber);
  ASSERT_EQ(pageSlot.data, logPageSlotData);
  ASSERT_EQ(pageSlot.getNextLocation(), nextLocation);
}

TEST_F(PageSerializerTestFixture, TestLoadSlottedPage) {
  auto page = loadPage(inputSlottedPage);

  ASSERT_EQ(page->getTypeId(), slottedPage->getTypeId());
  ASSERT_EQ(page->getId(), slottedPage->getId());
  ASSERT_EQ(static_cast<SlottedPage *>(page.get())->getNextPageId(),
            slottedPage->getNextPageId());
  ASSERT_EQ(static_cast<SlottedPage *>(page.get())->getPrevPageId(),
            slottedPage->getPrevPageId());
}

TEST_F(PageSerializerTestFixture, TestLoadError) {
  try {
    ByteBuffer _input;
    loadPage(_input);
    FAIL() << "Expected PageParseError Exception.";
  } catch (PageParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageParseError Exception.";
  }
}

TEST_F(PageSerializerTestFixture, TestDumpLogPage) {
  ByteBuffer output(logPageSize);
  dumpPage(*logPage, output);

  ASSERT_EQ(inputLogPage, output);
}

TEST_F(PageSerializerTestFixture, TestDumpSlottedPage) {
  ByteBuffer output(slottedPageSize);
  dumpPage(*slottedPage, output);

  ASSERT_EQ(inputSlottedPage, output);
}
