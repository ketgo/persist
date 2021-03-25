/**
 * test_serializer.cpp - Persist
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
 * @brief Test page serializer.
 *
 */

#include <gtest/gtest.h>

#include <memory>

#include <persist/core/page/serializer.hpp>

#include "persist/test/simple_page.hpp"

using namespace persist;
using namespace persist::test;

class PageSerializerTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  const PageId page_id = 12;
  const size_t page_size = DEFAULT_PAGE_SIZE;
  std::unique_ptr<SimplePage> page;
  ByteBuffer record = "testing"_bb;

  void SetUp() override {
    // Setup valid page
    page = persist::CreatePage<SimplePage>(page_id, page_size);

    // Add record to page
    page->SetRecord(record);

    input = {
        18, 3, 34, 247, 0, 0, 0,   0,   12,  0,   0,   0,   0,   0, 0, 0, 7, 0,
        0,  0, 0,  0,   0, 0, 116, 101, 115, 116, 105, 110, 103, 0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0, 0, 0,
        0,  0, 0,  0,   0, 0, 0,   0,   0,   0,   0,   0,   0,   0, 0, 0};
  }
};

TEST_F(PageSerializerTestFixture, TestLoad) {
  auto _page = persist::LoadPage<SimplePage>(input);

  ASSERT_EQ(_page->GetId(), page->GetId());
  ASSERT_EQ(_page->GetRecord(), page->GetRecord());
}

TEST_F(PageSerializerTestFixture, TestLoadError) {
  ByteBuffer _input;

  ASSERT_THROW(persist::LoadPage<SimplePage>(_input), PageParseError);
}

TEST_F(PageSerializerTestFixture, TestPageCorruptError) {
  ByteBuffer _input;

  ASSERT_THROW(persist::LoadPage<SimplePage>(_input), PageParseError);
}

TEST_F(PageSerializerTestFixture, TestDump) {
  ByteBuffer output(page_size);
  persist::DumpPage(*page, output);

  ASSERT_EQ(input, output);
}
