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

#include "persist/test/simple_page.hpp"

using namespace persist;
using namespace persist::test;

/***********************************************
 * Simple Page Header Unit Tests
 **********************************************/

class SimplePageHeaderTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  ByteBuffer extra;
  const PageId page_id = 12;
  std::unique_ptr<SimplePage::Header> header;

  void SetUp() override {
    header = std::make_unique<SimplePage::Header>(page_id);
    input = {12, 0, 0, 0, 0, 0, 0, 0, 201, 125, 55, 158, 0, 0, 0, 0};
    extra = {42, 0, 0, 0, 21, 48, 4};
  }
};

TEST_F(SimplePageHeaderTestFixture, TestLoad) {
  SimplePage::Header _header;
  ByteBuffer _input;
  _input.insert(_input.end(), input.begin(), input.end());
  _input.insert(_input.end(), extra.begin(), extra.end());
  _header.Load(_input);

  ASSERT_EQ(_header.page_id, header->page_id);
}

TEST_F(SimplePageHeaderTestFixture, TestLoadError) {
  try {
    ByteBuffer _input;
    SimplePage::Header _header;
    _header.Load(_input);
    FAIL() << "Expected PageParseError Exception.";
  } catch (PageParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageParseError Exception.";
  }
}

TEST_F(SimplePageHeaderTestFixture, TestLoadCorruptErrorInvalidChecksum) {
  try {
    ByteBuffer _input = input;
    _input.back() = 10;
    SimplePage::Header _header;
    _header.Load(_input);
    FAIL() << "Expected PageCorruptError Exception.";
  } catch (PageCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageCorruptError Exception.";
  }
}

TEST_F(SimplePageHeaderTestFixture, TestDump) {
  ByteBuffer output(header->GetSize());
  header->Dump(output);

  ASSERT_EQ(input, output);
}

TEST_F(SimplePageHeaderTestFixture, TestSize) {
  ASSERT_EQ(header->GetSize(), input.size());
}

/***********************************************
 * Simple Page Unit Tests
 ***********************************************/

class SimplePageTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  const PageId page_id = 12;
  const size_t page_size = DEFAULT_PAGE_SIZE;
  std::unique_ptr<SimplePage> page;
  const ByteBuffer record = "testing"_bb;

  void SetUp() override {
    // Setup valid page
    page = std::make_unique<SimplePage>(page_id, page_size);

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

TEST_F(SimplePageTestFixture, TestGetId) { ASSERT_EQ(page->GetId(), page_id); }

TEST_F(SimplePageTestFixture, TestFreeSpace) {
  ASSERT_EQ(page->GetFreeSpaceSize(Operation::UPDATE),
            page_size - page->header.GetSize() - page->record.size() -
                sizeof(size_t));
  ASSERT_EQ(page->GetFreeSpaceSize(Operation::INSERT),
            page_size - page->header.GetSize() - page->record.size() -
                sizeof(size_t));
}

TEST_F(SimplePageTestFixture, TestGetRecord) {
  ASSERT_EQ(page->GetRecord(), record);
}

TEST_F(SimplePageTestFixture, TestSetRecord) {
  ByteBuffer record_ = "testing_set"_bb;

  // Current free space in block
  page->SetRecord(record_);

  ASSERT_EQ(page->GetRecord(), record_);
}

TEST_F(SimplePageTestFixture, TestLoad) {
  SimplePage _page;
  _page.Load(input);

  ASSERT_EQ(_page.GetId(), page->GetId());
  ASSERT_EQ(_page.GetRecord(), page->GetRecord());
}

TEST_F(SimplePageTestFixture, TestLoadError) {
  try {
    ByteBuffer _input(page_size);
    SimplePage _page;
    _page.Load(_input);
    FAIL() << "Expected PageCorruptError Exception.";
  } catch (PageCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageCorruptError Exception.";
  }
}

TEST_F(SimplePageTestFixture, TestDump) {
  ByteBuffer output(page_size);
  page->Dump(output);

  ASSERT_EQ(input, output);
}
