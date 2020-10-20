/**
 * test_record_block_header.cpp - Persist
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
 * Record Block Header Unit Tests
 */

#include <gtest/gtest.h>

#include <memory>
#include <type_traits>

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>
#include <persist/core/record_block.hpp>

using namespace persist;

TEST(RecordBlockLocationTest, RecordBlockLocationNullTest) {
  RecordBlock::Location location;
  ASSERT_TRUE(location.isNull());
}

class RecordBlockHeaderTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  ByteBuffer extra;
  const PageId nextPageId = 10, prevPageId = 1;
  const PageSlotId nextSlotId = 100, prevSlotId = 10;
  std::unique_ptr<RecordBlock::Header> header;

  void SetUp() override {
    header = std::make_unique<RecordBlock::Header>();
    header->nextLocation.pageId = nextPageId;
    header->nextLocation.slotId = nextSlotId;
    header->prevLocation.pageId = prevPageId;
    header->prevLocation.slotId = prevSlotId;

    input = {10, 0, 0, 0, 0,  0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
             0,  0, 0, 0, 10, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    extra = {41, 0, 6, 0, 21, 48, 4};
  }
};

TEST_F(RecordBlockHeaderTestFixture, TestLoad) {
  RecordBlock::Header _header;
  ByteBuffer _input;
  _input.insert(_input.end(), input.begin(), input.end());
  _input.insert(_input.end(), extra.begin(), extra.end());
  _header.load(Span(_input));

  ASSERT_EQ(_header.nextLocation.pageId, header->nextLocation.pageId);
  ASSERT_EQ(_header.nextLocation.slotId, header->nextLocation.slotId);
  ASSERT_EQ(_header.prevLocation.pageId, header->prevLocation.pageId);
  ASSERT_EQ(_header.prevLocation.slotId, header->prevLocation.slotId);
}

TEST_F(RecordBlockHeaderTestFixture, TestLoadError) {
  try {
    ByteBuffer _input;
    RecordBlock::Header _header;
    _header.load(Span(_input));
    FAIL() << "Expected RecordBlockParseError Exception.";
  } catch (RecordBlockParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordBlockParseError Exception.";
  }
}

TEST_F(RecordBlockHeaderTestFixture, TestDump) {
  ByteBuffer output(sizeof(RecordBlock::Header));
  header->dump(Span(output));

  ASSERT_EQ(input, output);
}

TEST_F(RecordBlockHeaderTestFixture, TestSize) {
  ASSERT_EQ(header->size(), sizeof(RecordBlock::Header));
}
