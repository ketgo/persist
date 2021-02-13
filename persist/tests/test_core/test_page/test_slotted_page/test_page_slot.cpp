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
 * @brief PageSlot unit tests
 *
 */

#include <gtest/gtest.h>

#include <memory>

#include <persist/core/page/slotted_page/page_slot.hpp>

using namespace persist;

/***********************************************
 * PageSlot Header Unit Tests
 **********************************************/

TEST(PageSlotLocationTest, PageSlotLocationNullTest) {
  PageSlot::Location location;
  ASSERT_TRUE(location.isNull());
}

class PageSlotHeaderTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  ByteBuffer extra;
  const PageId nextPageId = 10, prevPageId = 1;
  const PageSlotId nextSlotId = 100, prevSlotId = 10;
  std::unique_ptr<PageSlot::Header> header;

  void SetUp() override {
    header = std::make_unique<PageSlot::Header>();
    header->nextLocation.pageId = nextPageId;
    header->nextLocation.slotId = nextSlotId;
    header->prevLocation.pageId = prevPageId;
    header->prevLocation.slotId = prevSlotId;

    input = {10, 0, 0, 0, 0,  0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
             0,  0, 0, 0, 10, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    extra = {41, 0, 6, 0, 21, 48, 4};
  }
};

TEST_F(PageSlotHeaderTestFixture, TestLoad) {
  PageSlot::Header _header;
  ByteBuffer _input;
  _input.insert(_input.end(), input.begin(), input.end());
  _input.insert(_input.end(), extra.begin(), extra.end());
  _header.load(Span(_input));

  ASSERT_EQ(_header.nextLocation.pageId, header->nextLocation.pageId);
  ASSERT_EQ(_header.nextLocation.slotId, header->nextLocation.slotId);
  ASSERT_EQ(_header.prevLocation.pageId, header->prevLocation.pageId);
  ASSERT_EQ(_header.prevLocation.slotId, header->prevLocation.slotId);
}

TEST_F(PageSlotHeaderTestFixture, TestLoadError) {
  try {
    ByteBuffer _input;
    PageSlot::Header _header;
    _header.load(Span(_input));
    FAIL() << "Expected PageSlotParseError Exception.";
  } catch (PageSlotParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageSlotParseError Exception.";
  }
}

TEST_F(PageSlotHeaderTestFixture, TestDump) {
  ByteBuffer output(sizeof(PageSlot::Header));
  header->dump(Span(output));

  ASSERT_EQ(input, output);
}

TEST_F(PageSlotHeaderTestFixture, TestSize) {
  ASSERT_EQ(header->size(), sizeof(PageSlot::Header));
}

/***********************************************
 * PageSlot Unit Tests
 **********************************************/

class PageSlotTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  const PageId nextPageId = 10, prevPageId = 1;
  const PageSlotId nextSlotId = 100, prevSlotId = 10;
  const ByteBuffer data = "testing"_bb;
  std::unique_ptr<PageSlot> block;

  void SetUp() override {
    PageSlot::Header header;
    header.nextLocation.pageId = nextPageId;
    header.nextLocation.slotId = nextSlotId;
    header.prevLocation.pageId = prevPageId;
    header.prevLocation.slotId = prevSlotId;
    block = std::make_unique<PageSlot>(header);
    block->data = data;

    input = {10,  0,  0,   0,   0,   0,   0,   0,   100, 0,   0,  0,
             0,   0,  0,   0,   1,   0,   0,   0,   0,   0,   0,  0,
             10,  0,  0,   0,   0,   0,   0,   0,   136, 86,  95, 3,
             171, 70, 156, 140, 116, 101, 115, 116, 105, 110, 103};
  }
};

TEST_F(PageSlotTestFixture, TestLoad) {
  PageSlot _block;
  _block.load(Span(input));

  ASSERT_EQ(_block.data, block->data);
}

TEST_F(PageSlotTestFixture, TestLoadParseError) {
  try {
    ByteBuffer _input;
    PageSlot _block;
    _block.load(Span(_input));
    FAIL() << "Expected PageSlotParseError Exception.";
  } catch (PageSlotParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageSlotParseError Exception.";
  }
}

TEST_F(PageSlotTestFixture, TestLoadCorruptError) {
  try {
    ByteBuffer _input = input;
    _input.back() = 0;
    PageSlot _block;
    _block.load(Span(_input));
    FAIL() << "Expected PageSlotCorruptError Exception.";
  } catch (PageSlotCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageSlotCorruptError Exception.";
  }
}

TEST_F(PageSlotTestFixture, TestDump) {
  ByteBuffer output(data.size() + sizeof(PageSlot::Header));

  block->dump(Span(output));

  ASSERT_EQ(input, output);
}

TEST_F(PageSlotTestFixture, TestMovePageSlot) {
  PageSlot _block;
  _block = std::move(*block);

  ASSERT_EQ(block->data, ""_bb);
  ASSERT_EQ(_block.data, data);
}

TEST_F(PageSlotTestFixture, TestSize) {
  ASSERT_EQ(block->size(), data.size() + sizeof(PageSlot::Header));
}

TEST_F(PageSlotTestFixture, TestGetNextLocation) {
  ASSERT_EQ(block->getNextLocation().pageId, nextPageId);
  ASSERT_EQ(block->getNextLocation().slotId, nextSlotId);
}

TEST_F(PageSlotTestFixture, TestSetNextLocation) {
  PageSlot::Location location(15, 5);
  block->setNextLocation(location);

  ASSERT_EQ(block->getNextLocation(), location);
}

TEST_F(PageSlotTestFixture, TestGetPrevLocation) {
  ASSERT_EQ(block->getPrevLocation().pageId, prevPageId);
  ASSERT_EQ(block->getPrevLocation().slotId, prevSlotId);
}

TEST_F(PageSlotTestFixture, TestSetPrevLocation) {
  PageSlot::Location location(15, 5);
  block->setPrevLocation(location);

  ASSERT_EQ(block->getPrevLocation(), location);
}
