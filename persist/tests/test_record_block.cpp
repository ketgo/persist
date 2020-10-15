/**
 * test_record_block.cpp - Persist
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
 * Record Block Unit Tests
 */

#include <gtest/gtest.h>

#include <memory>

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>
#include <persist/core/record_block.hpp>

using namespace persist;

class RecordBlockTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  const PageId nextPageId = 10, prevPageId = 1;
  const PageSlotId nextSlotId = 100, prevSlotId = 10;
  const ByteBuffer data = {'t', 'e', 's', 't', 'i', 'n', 'g'};
  std::unique_ptr<RecordBlock> block;

  void SetUp() override {
    RecordBlock::Header header;
    header.nextLocation.pageId = nextPageId;
    header.nextLocation.slotId = nextSlotId;
    header.prevLocation.pageId = prevPageId;
    header.prevLocation.slotId = prevSlotId;
    block = std::make_unique<RecordBlock>(header);
    block->data = data;

    input = {10,  0,  0,   0,   0,   0,   0,   0,   100, 0,   0,   0,
             0,   0,  0,   0,   1,   0,   0,   0,   0,   0,   0,   0,
             10,  0,  0,   0,   0,   0,   0,   0,   152, 195, 193, 182,
             152, 23, 192, 140, 116, 101, 115, 116, 105, 110, 103};
  }
};

TEST_F(RecordBlockTestFixture, TestLoad) {
  RecordBlock _block;
  _block.load(Span({input.data(), input.size()}));

  ASSERT_EQ(_block.data, block->data);
}

TEST_F(RecordBlockTestFixture, TestLoadParseError) {
  try {
    ByteBuffer _input;
    RecordBlock _block;
    _block.load(Span({_input.data(), _input.size()}));
    FAIL() << "Expected RecordBlockParseError Exception.";
  } catch (RecordBlockParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordBlockParseError Exception.";
  }
}

TEST_F(RecordBlockTestFixture, TestLoadCorruptError) {
  try {
    ByteBuffer _input = input;
    _input.back() = 0;
    RecordBlock _block;
    _block.load(Span({_input.data(), _input.size()}));
    FAIL() << "Expected RecordBlockCorruptError Exception.";
  } catch (RecordBlockCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordBlockCorruptError Exception.";
  }
}

TEST_F(RecordBlockTestFixture, TestDump) {
  ByteBuffer output(data.size() + sizeof(RecordBlock::Header));

  block->dump(Span({output.data(), output.size()}));

  ASSERT_EQ(input, output);
}

TEST_F(RecordBlockTestFixture, TestSize) {
  ASSERT_EQ(block->size(), data.size() + sizeof(RecordBlock::Header));
}

TEST_F(RecordBlockTestFixture, TestGetNextLocation) {
  ASSERT_EQ(block->nextLocation().pageId, nextPageId);
  ASSERT_EQ(block->nextLocation().slotId, nextSlotId);
}

TEST_F(RecordBlockTestFixture, TestSetNextLocation) {
  RecordBlock::Location location(15, 5);
  block->nextLocation().pageId = location.pageId;
  block->nextLocation().slotId = location.slotId;

  ASSERT_EQ(block->nextLocation().pageId, location.pageId);
  ASSERT_EQ(block->nextLocation().slotId, location.slotId);
}

TEST_F(RecordBlockTestFixture, TestGetPrevLocation) {
  ASSERT_EQ(block->prevLocation().pageId, prevPageId);
  ASSERT_EQ(block->prevLocation().slotId, prevSlotId);
}

TEST_F(RecordBlockTestFixture, TestSetPrevLocation) {
  RecordBlock::Location location(15, 5);
  block->prevLocation().pageId = location.pageId;
  block->prevLocation().slotId = location.slotId;

  ASSERT_EQ(block->prevLocation().pageId, location.pageId);
  ASSERT_EQ(block->prevLocation().slotId, location.slotId);
}