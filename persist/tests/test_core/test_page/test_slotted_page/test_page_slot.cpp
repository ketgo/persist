/**
 * test_page_slot.cpp - Persist
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
 * @brief SlottedPageSlot unit tests
 *
 */

#include <gtest/gtest.h>

#include <memory>

#include <persist/core/page/slotted_page/page_slot.hpp>

using namespace persist;

/***********************************************
 * SlottedPageSlot Header Unit Tests
 **********************************************/

TEST(SlottedPageSlotLocationTest, SlottedPageSlotLocationNullTest) {
  SlottedPageSlot::Location location;
  ASSERT_TRUE(location.IsNull());
}

class SlottedPageSlotHeaderTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  ByteBuffer extra;
  const PageId next_page_id = 10, prev_page_id = 1;
  const PageSlotId next_slot_id = 100, prev_slot_id = 10;
  std::unique_ptr<SlottedPageSlot::Header> header;

  void SetUp() override {
    header = std::make_unique<SlottedPageSlot::Header>();
    header->next_location.page_id = next_page_id;
    header->next_location.slot_id = next_slot_id;
    header->prev_location.page_id = prev_page_id;
    header->prev_location.slot_id = prev_slot_id;

    input = {10, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0,
             1,  0, 0, 0, 0, 0, 0, 0, 10,  0, 0, 0, 0, 0, 0, 0};
    extra = {41, 0, 6, 0, 21, 48, 4};
  }
};

TEST_F(SlottedPageSlotHeaderTestFixture, TestLoad) {
  SlottedPageSlot::Header _header;
  ByteBuffer _input;
  _input.insert(_input.end(), input.begin(), input.end());
  _input.insert(_input.end(), extra.begin(), extra.end());
  _header.Load(_input);

  ASSERT_EQ(_header.next_location.page_id, header->next_location.page_id);
  ASSERT_EQ(_header.next_location.slot_id, header->next_location.slot_id);
  ASSERT_EQ(_header.prev_location.page_id, header->prev_location.page_id);
  ASSERT_EQ(_header.prev_location.slot_id, header->prev_location.slot_id);
}

TEST_F(SlottedPageSlotHeaderTestFixture, TestLoadError) {
  try {
    ByteBuffer _input;
    SlottedPageSlot::Header _header;
    _header.Load(_input);
    FAIL() << "Expected PageSlotParseError Exception.";
  } catch (PageSlotParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageSlotParseError Exception.";
  }
}

TEST_F(SlottedPageSlotHeaderTestFixture, TestDump) {
  ByteBuffer output(sizeof(SlottedPageSlot::Header));
  header->Dump(output);

  ASSERT_EQ(input, output);
}

TEST_F(SlottedPageSlotHeaderTestFixture, TestSize) {
  ASSERT_EQ(header->GetSize(), sizeof(SlottedPageSlot::Header));
}

/***********************************************
 * SlottedPageSlot Unit Tests
 **********************************************/

class SlottedPageSlotTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  const PageId next_page_id = 10, prev_page_id = 1;
  const PageSlotId next_slot_id = 100, prev_slot_id = 10;
  const ByteBuffer data = "testing"_bb;
  std::unique_ptr<SlottedPageSlot> slot;

  void SetUp() override {
    SlottedPageSlot::Header header;
    header.next_location.page_id = next_page_id;
    header.next_location.slot_id = next_slot_id;
    header.prev_location.page_id = prev_page_id;
    header.prev_location.slot_id = prev_slot_id;
    slot = std::make_unique<SlottedPageSlot>(header);
    slot->data = data;

    input = {10, 0, 0, 0, 0, 0, 0, 0, 100, 0,   0,   0,   0,   0,   0,  0,
             1,  0, 0, 0, 0, 0, 0, 0, 10,  0,   0,   0,   0,   0,   0,  0,
             7,  0, 0, 0, 0, 0, 0, 0, 116, 101, 115, 116, 105, 110, 103};
  }
};

TEST_F(SlottedPageSlotTestFixture, TestLoad) {
  SlottedPageSlot _slot;
  _slot.Load(input);

  ASSERT_EQ(_slot.data, slot->data);
}

TEST_F(SlottedPageSlotTestFixture, TestLoadParseError) {
  try {
    ByteBuffer _input;
    SlottedPageSlot _slot;
    _slot.Load(_input);
    FAIL() << "Expected PageSlotParseError Exception.";
  } catch (PageSlotParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageSlotParseError Exception.";
  }
}

TEST_F(SlottedPageSlotTestFixture, TestDump) {
  ByteBuffer output(slot->GetSize());
  slot->Dump(output);

  ASSERT_EQ(input, output);
}

TEST_F(SlottedPageSlotTestFixture, TestMoveSlottedPageSlot) {
  SlottedPageSlot _slot;
  _slot = std::move(*slot);

  ASSERT_EQ(slot->data, ""_bb);
  ASSERT_EQ(_slot.data, data);
}

TEST_F(SlottedPageSlotTestFixture, TestSize) {
  ASSERT_EQ(slot->GetSize(), sizeof(SlottedPageSlot::Header) + sizeof(size_t) +
                                 sizeof(Byte) * data.size());
}

TEST_F(SlottedPageSlotTestFixture, TestGetNextLocation) {
  ASSERT_EQ(slot->GetNextLocation().page_id, next_page_id);
  ASSERT_EQ(slot->GetNextLocation().slot_id, next_slot_id);
}

TEST_F(SlottedPageSlotTestFixture, TestSetNextLocation) {
  SlottedPageSlot::Location location(15, 5);
  slot->SetNextLocation(location);

  ASSERT_EQ(slot->GetNextLocation(), location);
}

TEST_F(SlottedPageSlotTestFixture, TestGetPrevLocation) {
  ASSERT_EQ(slot->GetPrevLocation().page_id, prev_page_id);
  ASSERT_EQ(slot->GetPrevLocation().slot_id, prev_slot_id);
}

TEST_F(SlottedPageSlotTestFixture, TestSetPrevLocation) {
  SlottedPageSlot::Location location(15, 5);
  slot->SetPrevLocation(location);

  ASSERT_EQ(slot->GetPrevLocation(), location);
}
