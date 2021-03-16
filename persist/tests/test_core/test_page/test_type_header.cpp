/**
 * test_type_header.cpp - Persist
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

#include <persist/core/page/type_header.hpp>

using namespace persist;

class PageTypeHeaderTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  ByteBuffer extra;
  const PageTypeId type_id = 3;
  std::unique_ptr<PageTypeHeader> header;
  void SetUp() override {
    header = std::make_unique<PageTypeHeader>(type_id);

    input = {3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    extra = {41, 0, 6, 0, 21, 48, 4};
  }
};

TEST_F(PageTypeHeaderTestFixture, TestLoad) {
  PageTypeHeader _header;
  ByteBuffer _input;
  _input.insert(_input.end(), input.begin(), input.end());
  _input.insert(_input.end(), extra.begin(), extra.end());
  _header.Load(Span(_input));

  ASSERT_EQ(_header.GetTypeId(), header->GetTypeId());
}

TEST_F(PageTypeHeaderTestFixture, TestLoadError) {
  try {
    ByteBuffer _input;
    PageTypeHeader _header;
    _header.Load(_input);
    FAIL() << "Expected PageParseError Exception.";
  } catch (PageParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageParseError Exception.";
  }
}

TEST_F(PageTypeHeaderTestFixture, TestDump) {
  ByteBuffer output(header->GetSize());
  header->Dump(output);

  ASSERT_EQ(input, output);
}

TEST_F(PageTypeHeaderTestFixture, TestGetSize) {
  ASSERT_EQ(header->GetSize(), sizeof(PageTypeHeader));
}
