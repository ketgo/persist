/**
 * page/test_type_header.cpp - Persist
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

#include <persist/core/page/type_header.hpp>

using namespace persist;

class PageTypeHeaderTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  ByteBuffer extra;
  const PageTypeId typeId = 3;
  std::unique_ptr<PageTypeHeader> header;
  void SetUp() override {
    header = std::make_unique<PageTypeHeader>(3);

    input = {3, 0, 0, 0, 0, 0, 0, 0, 208, 125, 55, 158, 0, 0, 0, 0};
    extra = {41, 0, 6, 0, 21, 48, 4};
  }
};

TEST_F(PageTypeHeaderTestFixture, TestLoad) {
  PageTypeHeader _header;
  ByteBuffer _input;
  _input.insert(_input.end(), input.begin(), input.end());
  _input.insert(_input.end(), extra.begin(), extra.end());
  _header.load(Span(_input));

  ASSERT_EQ(_header.getTypeId(), header->getTypeId());
}

TEST_F(PageTypeHeaderTestFixture, TestLoadError) {
  try {
    ByteBuffer _input;
    PageTypeHeader _header;
    _header.load(_input);
    FAIL() << "Expected PageParseError Exception.";
  } catch (PageParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageParseError Exception.";
  }
}

TEST_F(PageTypeHeaderTestFixture, TestDump) {
  ByteBuffer output(header->size());
  header->dump(output);

  for (int c : output) {
    std::cout << c << ", ";
  }
  std::cout << "\n";

  ASSERT_EQ(input, output);
}

TEST_F(PageTypeHeaderTestFixture, TestSize) {
  ASSERT_EQ(header->size(), sizeof(PageTypeHeader));
}
