/**
 * test_block.cpp - Persist
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
 * Data Block Header Unit Tests
 */

#include <gtest/gtest.h>

#include <memory>

#include <persist/core/defs.hpp>
#include <persist/core/exceptions.hpp>
#include <persist/core/page.hpp>

using namespace persist;

class PageHeaderTestFixture : public ::testing::Test {
protected:
  ByteBuffer input;
  ByteBuffer extra;
  const PageId pageId = 12;
  const PageId nextPageId = 15;
  const PageId prevPageId = 1;
  std::unique_ptr<Page::Header> header;

  void SetUp() override {
    header = std::make_unique<Page::Header>(pageId);
    // setup valid header
    header->nextPageId = nextPageId;
    header->prevPageId = prevPageId;
    header->slots.push_back({1, DEFAULT_PAGE_SIZE - 10, 10});
    header->slots.push_back({2, DEFAULT_PAGE_SIZE - 15, 5});
    header->slots.push_back({3, DEFAULT_PAGE_SIZE - 18, 3});

    input = {12,  0, 0, 0, 0, 0, 0, 0, 15,  0,   0,   0,  0,  0,   0,  0,
             1,   0, 0, 0, 0, 0, 0, 0, 3,   0,   0,   0,  0,  0,   0,  0,
             1,   0, 0, 0, 0, 0, 0, 0, 246, 3,   0,   0,  0,  0,   0,  0,
             10,  0, 0, 0, 0, 0, 0, 0, 2,   0,   0,   0,  0,  0,   0,  0,
             241, 3, 0, 0, 0, 0, 0, 0, 5,   0,   0,   0,  0,  0,   0,  0,
             3,   0, 0, 0, 0, 0, 0, 0, 238, 3,   0,   0,  0,  0,   0,  0,
             3,   0, 0, 0, 0, 0, 0, 0, 142, 141, 188, 43, 11, 216, 12, 107};
    extra = {42, 0, 0, 0, 21, 48, 4};
  }
};

TEST_F(PageHeaderTestFixture, TestLoad) {
  Page::Header _header;
  ByteBuffer _input;
  _input.insert(_input.end(), input.begin(), input.end());
  _input.insert(_input.end(), extra.begin(), extra.end());
  _header.load(Span({_input.data(), _input.size()}));

  ASSERT_EQ(_header.pageId, header->pageId);
  ASSERT_EQ(_header.slots.size(), header->slots.size());
  Page::Header::Slots::iterator _it = _header.slots.begin();
  Page::Header::Slots::iterator it = header->slots.begin();
  while (_it != _header.slots.end() && it != header->slots.end()) {
    ASSERT_EQ(_it->id, it->id);
    ASSERT_EQ(_it->offset, it->offset);
    ASSERT_EQ(_it->size, it->size);
    ++_it;
    ++it;
  }
}

TEST_F(PageHeaderTestFixture, TestLoadError) {
  try {
    ByteBuffer _input;
    Page::Header _header;
    _header.load(Span({_input.data(), _input.size()}));
    FAIL() << "Expected PageParseError Exception.";
  } catch (PageParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageParseError Exception.";
  }
}

TEST_F(PageHeaderTestFixture, TestLoadCorruptErrorInvalidChecksum) {
  try {
    ByteBuffer _input = input;
    _input.back() = 0;
    Page::Header _header;
    _header.load(Span({_input.data(), _input.size()}));
    FAIL() << "Expected PageCorruptError Exception.";
  } catch (PageCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageCorruptError Exception.";
  }
}

TEST_F(PageHeaderTestFixture, TestLoadCorruptErrorInvalidSlotsCount) {
  try {
    ByteBuffer _input = input;
    _input[24] = 9; //<- sets the slot count located at 24th byte to 9
    Page::Header _header;
    _header.load(Span({_input.data(), _input.size()}));
    FAIL() << "Expected PageCorruptError Exception.";
  } catch (PageCorruptError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected PageCorruptError Exception.";
  }
}

TEST_F(PageHeaderTestFixture, TestDump) {
  ByteBuffer output(header->size());
  header->dump(Span({output.data(), output.size()}));

  ASSERT_EQ(input, output);
}

TEST_F(PageHeaderTestFixture, TestSize) {
  ASSERT_EQ(header->size(), input.size());
}

TEST_F(PageHeaderTestFixture, TestCreateSlot) {
  uint64_t size = 100;
  uint64_t tail = header->tail();
  Page::Header::Slot *slot = header->createSlot(size);
  ASSERT_EQ(header->tail(), tail - size);
  ASSERT_EQ(slot->id, 4);
  ASSERT_EQ(slot->offset, DEFAULT_PAGE_SIZE - 118);
  ASSERT_EQ(slot->size, size);
}

TEST_F(PageHeaderTestFixture, TestFreeSlot) {
  uint64_t tail = header->tail();
  Page::Header::Slots::iterator it = header->slots.begin();
  ++it;
  uint64_t entrySize = it->size;

  header->freeSlot(&(*it));
  ASSERT_EQ(header->tail(), tail + entrySize);
}