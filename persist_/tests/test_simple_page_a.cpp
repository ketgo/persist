/**
 * test_simple_page.cpp - Persist
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
 * @brief Simple Page Unit Test
 *
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

/**
 * Enabled intrusive testing
 */
#define PERSIST_INTRUSIVE_TESTING

#include <persist/core/page/simple_page_a.hpp>

using namespace persist;

/***********************************************
 * Simple Page Header Unit Tests
 **********************************************/

class SimplePageAHeaderTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  ByteBuffer extra;
  const PageId pageId = 12;
  std::unique_ptr<SimplePageA::Header> header;

  void SetUp() override {
    header = std::make_unique<SimplePageA::Header>(pageId);
    input = {12, 0, 0, 0, 0, 0, 0, 0, 201, 125, 55, 158, 0, 0, 0, 0};
    extra = {42, 0, 0, 0, 21, 48, 4};
  }
};

TEST_F(SimplePageAHeaderTestFixture, TestLoad) {
  SimplePageA::Header _header;
  ByteBuffer _input;
  _input.insert(_input.end(), input.begin(), input.end());
  _input.insert(_input.end(), extra.begin(), extra.end());
  _header.load(Span(_input));

  ASSERT_EQ(_header.pageId, header->pageId);
}

TEST_F(SimplePageAHeaderTestFixture, TestLoadError) {
  try {
    ByteBuffer _input;
    SimplePageA::Header _header;
    _header.load(Span(_input));
    FAIL() << "Expected PageParseError Exception.";
  } catch (PageParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageParseError Exception.";
  }
}

TEST_F(SimplePageAHeaderTestFixture, TestLoadCorruptErrorInvalidChecksum) {
  try {
    ByteBuffer _input = input;
    _input.back() = 10;
    SimplePageA::Header _header;
    _header.load(Span(_input));
    FAIL() << "Expected PageCorruptError Exception.";
  } catch (PageCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageCorruptError Exception.";
  }
}

TEST_F(SimplePageAHeaderTestFixture, TestDump) {
  ByteBuffer output(header->Size());
  header->dump(Span(output));

  ASSERT_EQ(input, output);
}

TEST_F(SimplePageAHeaderTestFixture, TestSize) {
  ASSERT_EQ(header->Size(), input.size());
}

/***********************************************
 * Simple Page Unit Tests
 ***********************************************/

TEST(SimplePageATest, PageSizeError) {
  try {
    SimplePageA page(1, 64);
    FAIL() << "Expected PageSizeError Exception.";
  } catch (PageSizeError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageSizeError Exception.";
  }
}

class SimplePageATestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  const PageId pageId = 12;
  const uint64_t pageSize = DEFAULT_PAGE_SIZE;
  std::unique_ptr<SimplePageA> page;
  const ByteBuffer record = "testing"_bb;

  void SetUp() override {
    // Setup valid page
    page = std::make_unique<SimplePageA>(pageId, pageSize);

    // Add record to page
    page->record = record;

    input = {
        12, 0, 0, 0, 0, 0,   0,   0,   201, 125, 55,  158, 0, 0, 0, 0, 7, 0, 0,
        0,  0, 0, 0, 0, 116, 101, 115, 116, 105, 110, 103, 0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0, 0, 0,
        0,  0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0};
  }
};

TEST_F(SimplePageATestFixture, TestGetId) { ASSERT_EQ(page->GetId(), pageId); }

TEST_F(SimplePageATestFixture, TestFreeSpace) {
  ASSERT_EQ(page->GetFreeSpaceSize(Operation::UPDATE),
            pageSize - page->header.Size() - page->record.size() -
                sizeof(size_t));
  ASSERT_EQ(page->GetFreeSpaceSize(Operation::INSERT),
            pageSize - page->header.Size() - page->record.size() -
                sizeof(size_t));
}

TEST_F(SimplePageATestFixture, TestGetRecord) {
  ASSERT_EQ(page->GetRecord(), record);
}

TEST_F(SimplePageATestFixture, TestSetRecord) {
  ByteBuffer record_ = "testing_set"_bb;

  // Current free space in block
  page->SetRecord(record_);

  ASSERT_EQ(page->GetRecord(), record_);
}

TEST_F(SimplePageATestFixture, TestLoad) {
  SimplePageA _page;
  _page.Load(Span(input));

  ASSERT_EQ(_page.GetId(), page->GetId());
  ASSERT_EQ(_page.GetRecord(), page->GetRecord());
}

TEST_F(SimplePageATestFixture, TestLoadError) {
  try {
    ByteBuffer _input(pageSize);
    SimplePageA _page;
    _page.Load(Span(_input));
    FAIL() << "Expected PageCorruptError Exception.";
  } catch (PageCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageCorruptError Exception.";
  }
}

TEST_F(SimplePageATestFixture, TestDump) {
  ByteBuffer output(pageSize);
  page->Dump(Span(output));

  ASSERT_EQ(input, output);
}
