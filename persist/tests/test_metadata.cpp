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

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>
#include <persist/core/metadata.hpp>

using namespace persist;

class MetaDataTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  std::unique_ptr<MetaData> metadata;
  const uint64_t pageSize = 1024;
  const uint64_t numPages = 10;
  const std::set<PageId> freeBlocks = {0, 1, 2, 3};

  void SetUp() override {
    metadata = std::make_unique<MetaData>();
    metadata->pageSize = pageSize;
    metadata->numPages = numPages;
    metadata->freePages = freeBlocks;
    input = {0, 4, 0, 0, 0, 0, 0, 0, 10,  0,  0,   0,   0,  0,  0,   0,
             4, 0, 0, 0, 0, 0, 0, 0, 0,   0,  0,   0,   0,  0,  0,   0,
             1, 0, 0, 0, 0, 0, 0, 0, 2,   0,  0,   0,   0,  0,  0,   0,
             3, 0, 0, 0, 0, 0, 0, 0, 156, 15, 190, 216, 63, 88, 125, 94};
  }
};

TEST_F(MetaDataTestFixture, TestLoad) {
  MetaData _metadata;
  _metadata.load(Span({input.data(), input.size()}));

  ASSERT_EQ(_metadata.pageSize, metadata->pageSize);
  ASSERT_EQ(_metadata.numPages, metadata->numPages);
  ASSERT_EQ(_metadata.freePages, metadata->freePages);
}

TEST_F(MetaDataTestFixture, TestLoadError) {
  try {
    ByteBuffer _input;
    MetaData _metadata;
    _metadata.load(Span({_input.data(), _input.size()}));
    FAIL() << "Expected MetaDataParseError Exception.";
  } catch (MetaDataParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected MetaDataParseError Exception.";
  }
}

TEST_F(MetaDataTestFixture, TestLoadCorruptErrorInvalidChecksum) {
  try {
    ByteBuffer _input = input;
    _input.back() = 0;
    MetaData _metadata;
    _metadata.load(Span({_input.data(), _input.size()}));
    FAIL() << "Expected MetaDataCorruptError Exception.";
  } catch (MetaDataCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected MetaDataCorruptError Exception.";
  }
}

TEST_F(MetaDataTestFixture, TestLoadCorruptErrorInvalidFreePagesCount) {
  try {
    ByteBuffer _input = input;
    _input[16] = 9; //<- sets the free pages count located at 16th byte to 9
    MetaData _metadata;
    _metadata.load(Span({_input.data(), _input.size()}));
    FAIL() << "Expected MetaDataCorruptError Exception.";
  } catch (MetaDataCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected MetaDataCorruptError Exception.";
  }
}

TEST_F(MetaDataTestFixture, TestDump) {
  ByteBuffer output(metadata->size());
  metadata->dump(Span({output.data(), output.size()}));

  ASSERT_EQ(input, output);
}

class MetaDataDeltaTestFixture : public ::testing::Test {
protected:
  std::vector<uint8_t> input;
  std::unique_ptr<MetaData> metadata;
  std::unique_ptr<MetaDataDelta> delta;
  const uint64_t pageSize = 1024;
  const PageId firstPageId = 1;
  const PageId numPages = 10;
  const std::set<PageId> freeBlocks = {0, 1, 2, 3};

  void SetUp() override {
    // Create metadata
    metadata = std::make_unique<MetaData>();
    metadata->pageSize = pageSize;
    metadata->numPages = numPages;
    metadata->freePages = freeBlocks;

    // Create metadata delta
    delta = std::make_unique<MetaDataDelta>();

    input = {123, 105, 9,   102, 114, 101, 101, 80,  97,  103, 101,
             115, 91,  91,  105, 3,   105, 255, 93,  93,  105, 8,
             110, 117, 109, 80,  97,  103, 101, 115, 105, 1,   125};
  }
};

TEST_F(MetaDataDeltaTestFixture, TestApplyNoChange) {
  delta->apply(*metadata);

  ASSERT_EQ(metadata->pageSize, pageSize);
  ASSERT_EQ(metadata->numPages, numPages);
  ASSERT_EQ(metadata->freePages, freeBlocks);
}

TEST_F(MetaDataDeltaTestFixture, TestApply) {
  delta->numPagesUp();
  delta->removeFreePage(3);
  delta->addFreePage(4);
  delta->apply(*metadata);

  ASSERT_EQ(metadata->pageSize, pageSize);
  ASSERT_EQ(metadata->numPages, numPages + 1);
  ASSERT_EQ(metadata->freePages, std::set<PageId>({0, 1, 2, 4}));

  delta->removeFreePage(3); //<- removing page no present in list
  delta->apply(*metadata);

  ASSERT_EQ(metadata->pageSize, pageSize);
  ASSERT_EQ(metadata->numPages, numPages + 2); //<- double increase
  ASSERT_EQ(metadata->freePages, std::set<PageId>({0, 1, 2, 4})); //<- no change

  delta->addFreePage(4); //<- adding page already present in list
  delta->apply(*metadata);

  ASSERT_EQ(metadata->pageSize, pageSize);
  ASSERT_EQ(metadata->numPages, numPages + 3); //<- tripple increase
  ASSERT_EQ(metadata->freePages, std::set<PageId>({0, 1, 2, 4})); //<- no change
}

TEST_F(MetaDataDeltaTestFixture, TestUndo) {
  delta->numPagesUp();
  delta->removeFreePage(3);
  delta->addFreePage(4);
  delta->undo(*metadata);

  ASSERT_EQ(metadata->pageSize, pageSize);
  ASSERT_EQ(metadata->numPages, numPages - 1);
  ASSERT_EQ(metadata->freePages, freeBlocks); //<- no change

  delta->addFreePage(2);
  delta->undo(*metadata);

  ASSERT_EQ(metadata->pageSize, pageSize);
  ASSERT_EQ(metadata->numPages, numPages - 2); //<- double decrease
  ASSERT_EQ(metadata->freePages, std::set<PageId>({0, 1, 3}));
}

TEST_F(MetaDataDeltaTestFixture, TestLoad) {
  MetaDataDelta _delta;
  _delta.load(input);
  _delta.apply(*metadata);

  ASSERT_EQ(metadata->pageSize, pageSize);
  ASSERT_EQ(metadata->numPages, numPages + 1);
  ASSERT_EQ(metadata->freePages, std::set<PageId>({0, 1, 2}));
}

TEST_F(MetaDataDeltaTestFixture, TestLoadError) {
  try {
    ByteBuffer _input;
    MetaDataDelta _delta;
    _delta.load(_input);
    FAIL() << "Expected MetaDataDeltaParseError Exception.";
  } catch (MetaDataDeltaParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected MetaDataDeltaParseError Exception.";
  }
}

TEST_F(MetaDataDeltaTestFixture, TestDump) {
  delta->numPagesUp();
  delta->removeFreePage(3);
  ByteBuffer output;
  delta->dump(output);

  ASSERT_EQ(input, output);
}