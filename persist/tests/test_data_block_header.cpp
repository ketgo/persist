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

#include <iostream>

#include <gtest/gtest.h>
#include <memory>
#include <vector>

#include <persist/block.hpp>
#include <persist/exceptions.hpp>

using namespace persist;

class DataBlockHeaderTestFixture : public ::testing::Test {
protected:
  std::vector<uint8_t> input;
  std::vector<uint8_t> extra;
  const DataBlockId blockId = 12;
  std::unique_ptr<DataBlock::Header> header;

  void SetUp() override {
    header = std::make_unique<DataBlock::Header>(blockId);
    // setup valid header
    header->entries.push_back({DEFAULT_DATA_BLOCK_SIZE - 10, 10});
    header->entries.push_back({DEFAULT_DATA_BLOCK_SIZE - 15, 5});
    header->entries.push_back({DEFAULT_DATA_BLOCK_SIZE - 18, 3});
    header->tail = DEFAULT_DATA_BLOCK_SIZE - 18;

    input = {123, 105, 7,   98,  108, 111, 99,  107, 73,  100, 105, 12,
             105, 7,   101, 110, 116, 114, 105, 101, 115, 91,  123, 105,
             6,   111, 102, 102, 115, 101, 116, 73,  3,   246, 105, 4,
             115, 105, 122, 101, 105, 10,  125, 123, 105, 6,   111, 102,
             102, 115, 101, 116, 73,  3,   241, 105, 4,   115, 105, 122,
             101, 105, 5,   125, 123, 105, 6,   111, 102, 102, 115, 101,
             116, 73,  3,   238, 105, 4,   115, 105, 122, 101, 105, 3,
             125, 93,  105, 4,   116, 97,  105, 108, 73,  3,   238, 125};
    extra = {42, 0, 0, 0, 21, 48, 4};
  }
};

TEST_F(DataBlockHeaderTestFixture, TestLoad) {
  DataBlock::Header _header;
  std::vector<uint8_t> _input;
  _input.insert(_input.end(), input.begin(), input.end());
  _input.insert(_input.end(), extra.begin(), extra.end());
  _header.load(_input);

  ASSERT_EQ(_header.blockId, header->blockId);
  ASSERT_EQ(_header.tail, header->tail);
  ASSERT_EQ(_header.entries.size(), header->entries.size());
  for (size_t i = 0; i < _header.entries.size(); i++) {
    ASSERT_EQ(_header.entries[i].offset, header->entries[i].offset);
    ASSERT_EQ(_header.entries[i].size, header->entries[i].size);
  }
}

TEST_F(DataBlockHeaderTestFixture, TestLoadError) {
  try {
    std::vector<uint8_t> _input;
    DataBlock::Header _header;
    _header.load(_input);
    FAIL() << "Expected DataBlockParseError Exception.";
  } catch (DataBlockParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected DataBlockParseError Exception.";
  }
}

TEST_F(DataBlockHeaderTestFixture, TestDump) {
  ByteBuffer &output = header->dump();

  ASSERT_EQ(input, output);
}

TEST_F(DataBlockHeaderTestFixture, TestSize) {
  ASSERT_EQ(header->size(), input.size());
}

TEST_F(DataBlockHeaderTestFixture, TestUseSpace) {
  uint64_t size = 100;
  uint64_t tail = header->tail;
  header->useSpace(size);
  ASSERT_EQ(header->tail, tail - size);
}

TEST_F(DataBlockHeaderTestFixture, TestFreeSpace) {
  uint64_t tail = header->tail;
  DataBlock::Header::Entries::iterator it = header->entries.begin() + 1;
  uint64_t entrySize = it->size;

  header->freeSpace(entrySize);
  ASSERT_EQ(header->tail, tail + entrySize);
}