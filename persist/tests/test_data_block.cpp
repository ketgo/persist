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
  const RecordBlockId recordBlockId_1 = 1, recordBlockId_2 = 2;
  std::unique_ptr<RecordBlock> recordBlock_1, recordBlock_2;
  const std::string recordBlockData_1 = "testing_1",
                    recordBlockData_2 = "testing_2";

  void SetUp() override {
    block = std::make_unique<DataBlock>(blockId, blockSize);
    recordBlock_1 = std::make_unique<RecordBlock>(recordBlockId_1);
    recordBlock_1->data = recordBlockData_1;
    block->addRecordBlock(*recordBlock_1);
    recordBlock_2 = std::make_unique<RecordBlock>(recordBlockId_2);
    recordBlock_2->data = recordBlockData_2;
    block->addRecordBlock(*recordBlock_2);

    input = {
        123, 105, 7,   98,  108, 111, 99,  107, 73,  100, 105, 12,  105, 7,
        101, 110, 116, 114, 105, 101, 115, 91,  123, 105, 6,   111, 102, 102,
        115, 101, 116, 73,  3,   206, 105, 4,   115, 105, 122, 101, 105, 50,
        125, 93,  105, 4,   116, 97,  105, 108, 73,  3,   206, 125, 0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,   123, 105, 7,   98,  108, 111,
        99,  107, 73,  100, 105, 1,   105, 11,  110, 101, 120, 116, 66,  108,
        111, 99,  107, 73,  100, 105, 0,   105, 11,  112, 114, 101, 118, 66,
        108, 111, 99,  107, 73,  100, 105, 0,   125, 116, 101, 115, 116, 105,
        110, 103};
  }
};

TEST_F(DataBlockTestFixture, TestGetId) { ASSERT_EQ(block->getId(), blockId); }

TEST_F(DataBlockTestFixture, TestFreeSpace) {
  DataBlock::Header header(blockId, blockSize);
  DataBlock _block(blockId, blockSize);

  ASSERT_EQ(_block.freeSpace(), blockSize - header.size());
}

TEST_F(DataBlockTestFixture, TestGetRecordBlock) {
  RecordBlock _recordBlock = block->getRecordBlock(recordBlockId_1);

  ASSERT_EQ(_recordBlock.getId(), recordBlockId_1);
  ASSERT_EQ(_recordBlock.data, recordBlockData_1);
}

TEST_F(DataBlockTestFixture, TestGetRecordBlockError) {
  try {
    RecordBlock _recordBlock = block->getRecordBlock(10);
    FAIL() << "Expected RecordBlockNotFoundError Exception.";
  } catch (RecordBlockNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordBlockNotFoundError Exception.";
  }
}

TEST_F(DataBlockTestFixture, TestAddRecordBlock) {
  RecordBlock recordBlock_1(10);
  recordBlock_1.data = "testing_1";
  RecordBlock recordBlock_2(11);
  recordBlock_2.data = "testing_2";

  // Current free space in block
  uint64_t freeSize = block->freeSpace();
  block->addRecordBlock(recordBlock_1);
  // Change in free space is greater than equals to amount record block size
  // NOTE: Its not exactly equal since the data block header size is increased
  ASSERT_TRUE(freeSize - block->freeSpace() >= recordBlock_1.size());

  freeSize = block->freeSpace();
  block->addRecordBlock(recordBlock_2);
  // Change in free space is greater than equals to amount record block size
  // NOTE: Its not exactly equal since the data block header size is increased
  ASSERT_TRUE(freeSize - block->freeSpace() >= recordBlock_2.size());
}

TEST_F(DataBlockTestFixture, TestAddRecordBlockError) {
  // A single data block is prevented from having multiple
  // record blocks with same ID. This implies record blocks
  // with same ID have to be stored in seprate data blocks.
  try {
    RecordBlock _recordBlock(1);
    _recordBlock.data = "_testing";
    block->addRecordBlock(_recordBlock);
    FAIL() << "Expected RecordBlockExistsError Exception.";
  } catch (RecordBlockExistsError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordBlockExistsError Exception.";
  }
}

/*
TEST_F(DataBlockTestFixture, TestLoad) {
  DataBlock _block;
  _block.load(input);

  ASSERT_EQ(_block.getId(), block->getId());

  RecordBlock _recordBlock = _block.getRecordBlock(recordBlockId_1);
  ASSERT_EQ(_recordBlock.getId(), recordBlockId_1);
  ASSERT_EQ(_recordBlock.data, recordBlockData_1);
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
  ByteBuffer &output = block->dump();

  for (int c : output) {
    std::cout << c << ",";
  }
  std::cout << "\n";

  ASSERT_EQ(input, output);
  ASSERT_EQ(output.size(), blockSize);
}
*/