/**
 * test_log_page/test_slot.cpp - Persist
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
 * @brief LogLogPageSlot Unit Tests
 *
 */

#include <gtest/gtest.h>

#include <memory>

#include <persist/core/page/log_page/page_slot.hpp>

using namespace persist;

TEST(LogPageSlotLocationTest, LogPageSlotLocationNullTest) {
  LogPageSlot::Location location;
  ASSERT_TRUE(location.isNull());
}

/***********************************************
 * LogPageSlot Header Unit Tests
 **********************************************/

class LogPageSlotHeaderTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  ByteBuffer extra;
  const PageId nextPageId = 10;
  const SeqNumber seqNumber = 0, nextSeqNumber = 100;
  std::unique_ptr<LogPageSlot::Header> header;

  void SetUp() override {
    header = std::make_unique<LogPageSlot::Header>(seqNumber);
    header->nextLocation.pageId = nextPageId;
    header->nextLocation.seqNumber = nextSeqNumber;

    input = {0,   0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0,
             100, 0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0};
    extra = {41, 0, 6, 0, 21, 48, 4};
  }
};

TEST_F(LogPageSlotHeaderTestFixture, TestLoad) {
  LogPageSlot::Header _header;
  ByteBuffer _input;
  _input.insert(_input.end(), input.begin(), input.end());
  _input.insert(_input.end(), extra.begin(), extra.end());
  _header.load(Span(_input));

  ASSERT_EQ(_header.seqNumber, header->seqNumber);
  ASSERT_EQ(_header.nextLocation.pageId, header->nextLocation.pageId);
  ASSERT_EQ(_header.nextLocation.seqNumber, header->nextLocation.seqNumber);
}

TEST_F(LogPageSlotHeaderTestFixture, TestLoadError) {
  try {
    ByteBuffer _input;
    LogPageSlot::Header _header;
    _header.load(Span(_input));
    FAIL() << "Expected PageSlotParseError Exception.";
  } catch (PageSlotParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageSlotParseError Exception.";
  }
}

TEST_F(LogPageSlotHeaderTestFixture, TestDump) {
  ByteBuffer output(sizeof(LogPageSlot::Header));
  header->dump(Span(output));

  ASSERT_EQ(input, output);
}

/***********************************************
 * LogPageSlot Unit Tests
 **********************************************/

class LogPageSlotTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  const PageId nextPageId = 10;
  const SeqNumber seqNumber = 0, nextSeqNumber = 100;
  const ByteBuffer data = "testing"_bb;
  std::unique_ptr<LogPageSlot> slot;

  void SetUp() override {
    LogPageSlot::Location location(nextPageId, nextSeqNumber);
    slot = std::make_unique<LogPageSlot>(seqNumber, location);
    slot->data = data;

    input = {0,   0, 0, 0, 0, 0, 0, 0, 10,  0,   0,   0,   0,   0,   0,  0,
             100, 0, 0, 0, 0, 0, 0, 0, 97,  60,  168, 198, 83,  130, 2,  0,
             7,   0, 0, 0, 0, 0, 0, 0, 116, 101, 115, 116, 105, 110, 103};
  }
};

TEST_F(LogPageSlotTestFixture, TestLoad) {
  LogPageSlot _block;
  _block.load(Span(input));

  ASSERT_EQ(_block.data, slot->data);
}

TEST_F(LogPageSlotTestFixture, TestLoadParseError) {
  try {
    ByteBuffer _input;
    LogPageSlot _block;
    _block.load(Span(_input));
    FAIL() << "Expected PageSlotParseError Exception.";
  } catch (PageSlotParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageSlotParseError Exception.";
  }
}

TEST_F(LogPageSlotTestFixture, TestLoadCorruptError) {
  try {
    ByteBuffer _input = input;
    _input[8] = 0;
    LogPageSlot _block;
    _block.load(Span(_input));
    FAIL() << "Expected PageSlotCorruptError Exception.";
  } catch (PageSlotCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageSlotCorruptError Exception.";
  }
}

TEST_F(LogPageSlotTestFixture, TestDump) {
  ByteBuffer output(data.size() + sizeof(size_t) + sizeof(LogPageSlot::Header));
  slot->dump(Span(output));

  for (int c : output) {
    std::cout << c << ", ";
  }
  std::cout << "\n";

  ASSERT_EQ(input, output);
}

TEST_F(LogPageSlotTestFixture, TestMoveLogPageSlot) {
  LogPageSlot _block;
  _block = std::move(*slot);

  ASSERT_EQ(slot->data, ""_bb);
  ASSERT_EQ(_block.data, data);
}

TEST_F(LogPageSlotTestFixture, TestSize) {
  ASSERT_EQ(slot->size(),
            data.size() + sizeof(size_t) + sizeof(LogPageSlot::Header));
}

TEST_F(LogPageSlotTestFixture, TestGetNextLocation) {
  ASSERT_EQ(slot->getNextLocation().pageId, nextPageId);
  ASSERT_EQ(slot->getNextLocation().seqNumber, nextSeqNumber);
}

TEST_F(LogPageSlotTestFixture, TestSetNextLocation) {
  LogPageSlot::Location location(15, 5);
  slot->setNextLocation(location);

  ASSERT_EQ(slot->getNextLocation(), location);
}
