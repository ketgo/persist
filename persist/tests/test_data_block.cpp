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
 * Data Block Unit Tests
 */

#include <iostream>

#include <gtest/gtest.h>
#include <memory>
#include <vector>

#include <persist/block.hpp>
#include <persist/exceptions.hpp>

using namespace persist;

class DataBlockTestFixture : public ::testing::Test {
protected:
  std::vector<uint8_t> input;
  const DataBlockId blockId = 12;
  const uint64_t blockSize = DEFAULT_DATA_BLOCK_SIZE;
  std::unique_ptr<DataBlock> block;

  void SetUp() override {
    block = std::make_unique<DataBlock>(blockId, blockSize);
    input = {123, 105, 11,  110, 101, 120, 116, 66,  108, 111, 99,  107,
             73,  100, 105, 12,  105, 11,  112, 114, 101, 118, 66,  108,
             111, 99,  107, 73,  100, 105, 1,   105, 13,  114, 101, 99,
             111, 114, 100, 66,  108, 111, 99,  107, 73,  100, 105, 10,
             125, 116, 101, 115, 116, 105, 110, 103};
  }
};

TEST_F(DataBlockTestFixture, TestFreeSize) {
  DataBlock::Header header(blockId, blockSize);

  ASSERT_EQ(block->freeSize(), blockSize - header.size());
}

/*
TEST_F(DataBlockTestFixture, TestLoad) {
  DataBlock _block;
  _block.load(input);

  ASSERT_EQ(_block.getId(), block->getId());
}

TEST_F(DataBlockTestFixture, TestLoadError) {
  try {
    std::vector<uint8_t> _input;
    RecordBlock _block;
    _block.load(_input);
    FAIL() << "Expected RecordBlockParseError Exception.";
  } catch (RecordBlockParseError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordBlockParseError Exception.";
  }
}

TEST_F(DataBlockTestFixture, TestDump) {
  std::vector<uint8_t> output;
  block->dump(output);

  for (int c : output) {
    std::cout << c << ",";
  }
  std::cout << "\n";

  ASSERT_EQ(input, output);
}
*/