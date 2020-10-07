/**
 * test_metadata.cpp - Persist
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
 * Storage MetaData unit tests.
 */

#include <gtest/gtest.h>

#include <list>
#include <memory>
#include <vector>

#include <persist/core/exceptions.hpp>
#include <persist/core/storage/base.hpp>

using namespace persist;

class MetaDataTestFixture : public ::testing::Test {
protected:
  std::vector<uint8_t> input;
  std::unique_ptr<Storage::MetaData> metadata;
  const uint64_t blockSize = 1024;
  const PageId firstPageId = 1;
  const PageId lastPageId = 10;
  const std::list<PageId> freeBlocks = {0, 1, 2, 3};

  void SetUp() override {
    metadata = std::make_unique<Storage::MetaData>();
    metadata->pageSize = blockSize;
    metadata->firstPageId = firstPageId;
    metadata->lastPageId = lastPageId;
    metadata->freePages = freeBlocks;
    input = {123, 105, 11,  102, 105, 114, 115, 116, 80,  97,  103, 101, 73,
             100, 105, 1,   105, 9,   102, 114, 101, 101, 80,  97,  103, 101,
             115, 91,  105, 0,   105, 1,   105, 2,   105, 3,   93,  105, 10,
             108, 97,  115, 116, 80,  97,  103, 101, 73,  100, 105, 10,  105,
             8,   112, 97,  103, 101, 83,  105, 122, 101, 73,  4,   0,   125};
  }
};

TEST_F(MetaDataTestFixture, TestLoad) {
  Storage::MetaData _metadata;
  _metadata.load(input);

  ASSERT_EQ(_metadata.pageSize, metadata->pageSize);
  ASSERT_EQ(_metadata.firstPageId, metadata->firstPageId);
  ASSERT_EQ(_metadata.lastPageId, metadata->lastPageId);
  ASSERT_EQ(_metadata.freePages, metadata->freePages);
}

TEST_F(MetaDataTestFixture, TestLoadError) {
  try {
    std::vector<uint8_t> _input;
    Storage::MetaData _metadata;
    _metadata.load(_input);
    FAIL() << "Expected MetaDataParseError Exception.";
  } catch (MetaDataParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected MetaDataParseError Exception.";
  }
}

TEST_F(MetaDataTestFixture, TestDump) {
  ByteBuffer &output = metadata->dump();

  ASSERT_EQ(input, output);
}
