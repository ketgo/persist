/**
 * test_slot.cpp - Persist
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
 * @brief RecordPageSlot unit tests
 *
 */

#include <gtest/gtest.h>

#include <memory>

#include <persist/core/page/record_page/slot.hpp>

using namespace persist;

/***********************************************
 * RecordPageSlot Header Unit Tests
 **********************************************/

TEST(RecordPageSlotLocationTest, RecordPageSlotLocationNullTest) {
  RecordPageSlot::Location location;
  ASSERT_TRUE(location.IsNull());
}

class RecordPageSlotHeaderTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  ByteBuffer extra;
  const PageId next_page_id = 10, prev_page_id = 1;
  const PageSlotId next_slot_id = 100, prev_slot_id = 10;
  std::unique_ptr<RecordPageSlot::Header> header;

  void SetUp() override {
    header = std::make_unique<RecordPageSlot::Header>();
    header->next_location.page_id = next_page_id;
    header->next_location.slot_id = next_slot_id;
    header->prev_location.page_id = prev_page_id;
    header->prev_location.slot_id = prev_slot_id;

    input = {10, 0, 0, 0, 0,  0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
             0,  0, 0, 0, 10, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    extra = {41, 0, 6, 0, 21, 48, 4};
  }
};

TEST_F(RecordPageSlotHeaderTestFixture, TestLoad) {
  RecordPageSlot::Header _header;
  ByteBuffer _input;
  _input.insert(_input.end(), input.begin(), input.end());
  _input.insert(_input.end(), extra.begin(), extra.end());
  _header.Load(_input);

  ASSERT_EQ(_header.next_location.page_id, header->next_location.page_id);
  ASSERT_EQ(_header.next_location.slot_id, header->next_location.slot_id);
  ASSERT_EQ(_header.prev_location.page_id, header->prev_location.page_id);
  ASSERT_EQ(_header.prev_location.slot_id, header->prev_location.slot_id);
}

TEST_F(RecordPageSlotHeaderTestFixture, TestLoadError) {
  ByteBuffer _input;
  RecordPageSlot::Header _header;

  ASSERT_THROW(_header.Load(_input), PageParseError);
}

TEST_F(RecordPageSlotHeaderTestFixture, TestDump) {
  ByteBuffer output(header->GetStorageSize());
  header->Dump(output);

  ASSERT_EQ(input, output);
}

TEST_F(RecordPageSlotHeaderTestFixture, TestSize) {
  ASSERT_EQ(header->GetStorageSize(), sizeof(RecordPageSlot::Header));
}

/***********************************************
 * RecordPageSlot Unit Tests
 **********************************************/

class RecordPageSlotTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  const PageId next_page_id = 10, prev_page_id = 1;
  const PageSlotId next_slot_id = 100, prev_slot_id = 10;
  const ByteBuffer data = "testing"_bb;
  std::unique_ptr<RecordPageSlot> slot;

  void SetUp() override {
    RecordPageSlot::Header header;
    header.next_location.page_id = next_page_id;
    header.next_location.slot_id = next_slot_id;
    header.prev_location.page_id = prev_page_id;
    header.prev_location.slot_id = prev_slot_id;
    slot = std::make_unique<RecordPageSlot>(header);
    slot->data = data;

    input = {10, 0, 0, 0, 0, 0, 0,   0,   100, 0,   0,   0,   0,  0,
             0,  0, 1, 0, 0, 0, 0,   0,   0,   0,   10,  0,   0,  0,
             0,  0, 0, 0, 0, 0, 0,   0,   0,   0,   0,   0,   7,  0,
             0,  0, 0, 0, 0, 0, 116, 101, 115, 116, 105, 110, 103};
  }
};

TEST_F(RecordPageSlotTestFixture, TestLoad) {
  RecordPageSlot _slot;
  _slot.Load(input);

  ASSERT_EQ(_slot.data, slot->data);
}

TEST_F(RecordPageSlotTestFixture, TestLoadParseError) {
  ByteBuffer _input;
  RecordPageSlot _slot;

  ASSERT_THROW(_slot.Load(_input), PageParseError);
}

TEST_F(RecordPageSlotTestFixture, TestDump) {
  ByteBuffer output(slot->GetStorageSize());
  slot->Dump(output);

  ASSERT_EQ(input, output);
}

TEST_F(RecordPageSlotTestFixture, TestMoveRecordPageSlot) {
  RecordPageSlot _slot;
  _slot = std::move(*slot);

  ASSERT_EQ(slot->data, ""_bb);
  ASSERT_EQ(_slot.data, data);
}

TEST_F(RecordPageSlotTestFixture, TestSize) {
  ASSERT_EQ(slot->GetStorageSize(), sizeof(RecordPageSlot::Header) +
                                        sizeof(size_t) +
                                        sizeof(Byte) * data.size());
}

TEST_F(RecordPageSlotTestFixture, TestGetNextLocation) {
  ASSERT_EQ(slot->GetNextLocation().page_id, next_page_id);
  ASSERT_EQ(slot->GetNextLocation().slot_id, next_slot_id);
}

TEST_F(RecordPageSlotTestFixture, TestSetNextLocation) {
  RecordPageSlot::Location location(15, 5);
  slot->SetNextLocation(location);

  ASSERT_EQ(slot->GetNextLocation(), location);
}

TEST_F(RecordPageSlotTestFixture, TestGetPrevLocation) {
  ASSERT_EQ(slot->GetPrevLocation().page_id, prev_page_id);
  ASSERT_EQ(slot->GetPrevLocation().slot_id, prev_slot_id);
}

TEST_F(RecordPageSlotTestFixture, TestSetPrevLocation) {
  RecordPageSlot::Location location(15, 5);
  slot->SetPrevLocation(location);

  ASSERT_EQ(slot->GetPrevLocation(), location);
}
