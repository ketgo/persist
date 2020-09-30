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

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include <persist/core/block.hpp>
#include <persist/core/exceptions.hpp>

using namespace persist;

TEST(DataBlockTest, DataBlockSizeError) {
  try {
    DataBlock block(1, 64);
    FAIL() << "Expected DataBlockSizeError Exception.";
  } catch (DataBlockSizeError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected DataBlockSizeError Exception.";
  }
}

class DataBlockTestFixture : public ::testing::Test {
protected:
  std::vector<uint8_t> input;
  const DataBlockId blockId = 12;
  const uint64_t blockSize = DEFAULT_DATA_BLOCK_SIZE;
  std::unique_ptr<DataBlock> block;
  std::unique_ptr<DataBlock::Header> blockHeader;
  const RecordBlockId recordBlockId_1 = 1, recordBlockId_2 = 2;
  std::unique_ptr<RecordBlock> recordBlock_1, recordBlock_2;
  const std::string recordBlockData_1 = "testing_1",
                    recordBlockData_2 = "testing_2";

  void SetUp() override {
    block = std::make_unique<DataBlock>(blockId, blockSize);
    blockHeader = std::make_unique<DataBlock::Header>(blockId, blockSize);

    recordBlock_1 = std::make_unique<RecordBlock>(recordBlockId_1);
    recordBlock_1->data = recordBlockData_1;
    block->addRecordBlock(*recordBlock_1);
    blockHeader->useSpace(recordBlock_1->size());
    recordBlock_2 = std::make_unique<RecordBlock>(recordBlockId_2);
    recordBlock_2->data = recordBlockData_2;
    block->addRecordBlock(*recordBlock_2);
    blockHeader->useSpace(recordBlock_2->size());

    input = {
        123, 105, 7,   98,  108, 111, 99,  107, 73,  100, 105, 12,  105, 9,
        98,  108, 111, 99,  107, 83,  105, 122, 101, 73,  4,   0,   105, 7,
        101, 110, 116, 114, 105, 101, 115, 91,  123, 105, 6,   111, 102, 102,
        115, 101, 116, 73,  3,   204, 105, 4,   115, 105, 122, 101, 105, 52,
        125, 123, 105, 6,   111, 102, 102, 115, 101, 116, 73,  3,   152, 105,
        4,   115, 105, 122, 101, 105, 52,  125, 93,  125, 0,   0,   0,   0,
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
        0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   123, 105, 7,   98,
        108, 111, 99,  107, 73,  100, 105, 2,   105, 11,  110, 101, 120, 116,
        66,  108, 111, 99,  107, 73,  100, 105, 0,   105, 11,  112, 114, 101,
        118, 66,  108, 111, 99,  107, 73,  100, 105, 0,   125, 116, 101, 115,
        116, 105, 110, 103, 95,  50,  123, 105, 7,   98,  108, 111, 99,  107,
        73,  100, 105, 1,   105, 11,  110, 101, 120, 116, 66,  108, 111, 99,
        107, 73,  100, 105, 0,   105, 11,  112, 114, 101, 118, 66,  108, 111,
        99,  107, 73,  100, 105, 0,   125, 116, 101, 115, 116, 105, 110, 103,
        95,  49};
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
  uint64_t oldDataSize = blockSize - block->freeSpace() - blockHeader->size();
  block->addRecordBlock(recordBlock_1);
  blockHeader->useSpace(recordBlock_1.size());
  uint64_t newDataSize = blockSize - block->freeSpace() - blockHeader->size();
  ASSERT_TRUE(newDataSize - oldDataSize == recordBlock_1.size());

  oldDataSize = blockSize - block->freeSpace() - blockHeader->size();
  block->addRecordBlock(recordBlock_2);
  blockHeader->useSpace(recordBlock_2.size());
  newDataSize = blockSize - block->freeSpace() - blockHeader->size();
  ASSERT_TRUE(newDataSize - oldDataSize == recordBlock_2.size());
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

TEST_F(DataBlockTestFixture, TestRemoveRecordBlock) {
  uint64_t oldDataSize = blockSize - block->freeSpace() - blockHeader->size();
  block->removeRecordBlock(recordBlockId_2);
  blockHeader->entries.pop_back();
  uint64_t newDataSize = blockSize - block->freeSpace() - blockHeader->size();
  ASSERT_THROW(block->getRecordBlock(recordBlockId_2),
               RecordBlockNotFoundError);
  ASSERT_TRUE(oldDataSize - newDataSize == recordBlock_2->size());

  oldDataSize = blockSize - block->freeSpace() - blockHeader->size();
  block->removeRecordBlock(recordBlockId_1);
  blockHeader->entries.pop_back();
  newDataSize = blockSize - block->freeSpace() - blockHeader->size();
  ASSERT_THROW(block->getRecordBlock(recordBlockId_1),
               RecordBlockNotFoundError);
  ASSERT_TRUE(oldDataSize - newDataSize == recordBlock_1->size());
}

TEST_F(DataBlockTestFixture, TestRemoveRecordBlockError) {
  try {
    block->removeRecordBlock(20);
    FAIL() << "Expected RecordBlockNotFoundError Exception.";
  } catch (RecordBlockNotFoundError &err) {
    SUCCEED();
  } catch (...) {
    FAIL() << "Expected RecordBlockNotFoundError Exception.";
  }
}

TEST_F(DataBlockTestFixture, TestLoad) {
  DataBlock _block;
  _block.load(input);

  ASSERT_EQ(_block.getId(), block->getId());

  RecordBlock &_recordBlock_1 = _block.getRecordBlock(recordBlockId_1);
  ASSERT_EQ(_recordBlock_1.getId(), recordBlockId_1);
  ASSERT_EQ(_recordBlock_1.data, recordBlockData_1);

  RecordBlock &_recordBlock_2 = _block.getRecordBlock(recordBlockId_2);
  ASSERT_EQ(_recordBlock_2.getId(), recordBlockId_2);
  ASSERT_EQ(_recordBlock_2.data, recordBlockData_2);
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

  ASSERT_EQ(input, output);
  ASSERT_EQ(output.size(), blockSize);
}
