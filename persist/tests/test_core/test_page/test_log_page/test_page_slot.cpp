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
  ASSERT_TRUE(location.IsNull());
}

/***********************************************
 * LogPageSlot Header Unit Tests
 **********************************************/

class LogPageSlotHeaderTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  ByteBuffer extra;
  const PageId next_page_id = 10;
  const SeqNumber seq_number = 0, next_seq_number = 100;
  std::unique_ptr<LogPageSlot::Header> header;

  void SetUp() override {
    header = std::make_unique<LogPageSlot::Header>(seq_number);
    header->next_location.page_id = next_page_id;
    header->next_location.seq_number = next_seq_number;

    input = {0, 0, 0, 0, 0,   0, 0, 0, 10, 0, 0, 0,
             0, 0, 0, 0, 100, 0, 0, 0, 0,  0, 0, 0};
    extra = {41, 0, 6, 0, 21, 48, 4};
  }
};

TEST_F(LogPageSlotHeaderTestFixture, TestLoad) {
  LogPageSlot::Header _header;
  ByteBuffer _input;
  _input.insert(_input.end(), input.begin(), input.end());
  _input.insert(_input.end(), extra.begin(), extra.end());
  _header.Load(_input);

  ASSERT_EQ(_header.seq_number, header->seq_number);
  ASSERT_EQ(_header.next_location.page_id, header->next_location.page_id);
  ASSERT_EQ(_header.next_location.seq_number, header->next_location.seq_number);
}

TEST_F(LogPageSlotHeaderTestFixture, TestLoadError) {
  try {
    ByteBuffer _input;
    LogPageSlot::Header _header;
    _header.Load(_input);
    FAIL() << "Expected PageSlotParseError Exception.";
  } catch (PageSlotParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageSlotParseError Exception.";
  }
}

TEST_F(LogPageSlotHeaderTestFixture, TestDump) {
  ByteBuffer output(sizeof(LogPageSlot::Header));
  header->Dump(output);

  ASSERT_EQ(input, output);
}

/***********************************************
 * LogPageSlot Unit Tests
 **********************************************/

class LogPageSlotTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  const PageId next_page_id = 10;
  const SeqNumber seq_number = 0, next_seq_number = 100;
  const ByteBuffer data = "testing"_bb;
  std::unique_ptr<LogPageSlot> slot;

  void SetUp() override {
    LogPageSlot::Location location(next_page_id, next_seq_number);
    slot = std::make_unique<LogPageSlot>(seq_number, location);
    slot->data = data;

    input = {7,   0,   0, 0,   0, 0, 0, 0, 116, 101, 115, 116, 105,
             110, 103, 0, 100, 0, 0, 0, 0, 0,   0,   0,   0,   0,
             0,   0,   0, 0,   0, 0, 0, 0, 0,   0,   0,   0,   0};
  }
};

TEST_F(LogPageSlotTestFixture, TestLoad) {
  LogPageSlot _block;
  _block.Load(input);

  ASSERT_EQ(_block.data, slot->data);
}

TEST_F(LogPageSlotTestFixture, TestLoadParseError) {
  try {
    ByteBuffer _input;
    LogPageSlot _block;
    _block.Load(_input);
    FAIL() << "Expected PageSlotParseError Exception.";
  } catch (PageSlotParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageSlotParseError Exception.";
  }
}

TEST_F(LogPageSlotTestFixture, TestDump) {
  ByteBuffer output(data.size() + sizeof(size_t) + sizeof(LogPageSlot::Header));
  slot->Dump(output);

  ASSERT_EQ(input, output);
}

TEST_F(LogPageSlotTestFixture, TestMoveLogPageSlot) {
  LogPageSlot _block;
  _block = std::move(*slot);

  ASSERT_EQ(slot->data, ""_bb);
  ASSERT_EQ(_block.data, data);
}

TEST_F(LogPageSlotTestFixture, TestGetSize) {
  ASSERT_EQ(slot->GetSize(),
            data.size() + sizeof(size_t) + sizeof(LogPageSlot::Header));
}

TEST_F(LogPageSlotTestFixture, TestGetNextLocation) {
  ASSERT_EQ(slot->GetNextLocation().page_id, next_page_id);
  ASSERT_EQ(slot->GetNextLocation().seq_number, next_seq_number);
}

TEST_F(LogPageSlotTestFixture, TestSetNextLocation) {
  LogPageSlot::Location location(15, 5);
  slot->SetNextLocation(location);

  ASSERT_EQ(slot->GetNextLocation(), location);
}
